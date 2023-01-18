/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_server.h"
#include "mock_cookie.h"

#include <gsl/gsl-lite.hpp>
#include <json/syntax_validator.h>
#include <logger/logger.h>
#include <memcached/config_parser.h>
#include <memcached/document_expired.h>
#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_bucket_iface.h>
#include <memcached/server_core_iface.h>
#include <platform/platform_time.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <xattr/blob.h>
#include <xattr/utils.h>
#include <array>
#include <atomic>
#include <cstring>
#include <ctime>
#include <list>
#include <mutex>
#include <queue>

#define REALTIME_MAXDELTA 60*60*24*3

std::atomic<time_t> process_started;     /* when the mock server was started */

/* Offset from 'real' time used to test time handling */
std::atomic<rel_time_t> time_travel_offset;

spdlog::level::level_enum log_level = spdlog::level::level_enum::info;

/// Queue of notification status for each cookie
using CookieNotifyQueue = std::queue<cb::engine_errc>;

/// Map of Cookie to its queued notifications.
using CookieToNotificationsMap =
        std::unordered_map<CookieIface*, CookieNotifyQueue>;

/// CookieToNotificationsMap guarded by a mutex to allow concurrent waiting
/// and signalling on a new status being added for a given cookie.
folly::Synchronized<CookieToNotificationsMap, std::mutex> cookieNotifications;

/// Condition variable used with cookieNotifications to allow waiting on
/// notify_io_complete being called for a given cookie.
std::condition_variable cookieNotificationSignal;

/* Forward declarations */

/* time-sensitive callers can call it by hand with this, outside the
   normal ever-1-second timer */
static rel_time_t mock_get_current_time() {
#ifdef WIN32
    rel_time_t result = (rel_time_t)(time(NULL) - process_started + time_travel_offset);
#else
    struct timeval timer {};
    gettimeofday(&timer, nullptr);
    auto result =
            (rel_time_t)(timer.tv_sec - process_started + time_travel_offset);
#endif
    return result;
}

void mock_notify_io_complete(CookieIface& cookie, cb::engine_errc status) {
    // Using at() here as the cookie should have been registered in
    // cookieNotifications when it was created.
    cookieNotifications.lock()->at(&cookie).push(status);
    cookieNotificationSignal.notify_all();
}

static rel_time_t mock_realtime(rel_time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */

    rel_time_t rv = 0;
    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started) {
            rv = (rel_time_t)1;
        } else {
            rv = (rel_time_t)(exptime - process_started);
        }
    } else {
        rv = (rel_time_t)(exptime + mock_get_current_time());
    }

    return rv;
}

static time_t mock_abstime(const rel_time_t exptime)
{
    return process_started + exptime;
}

static time_t mock_limit_abstime(time_t t, std::chrono::seconds limit) {
    auto upperbound = mock_abstime(mock_get_current_time()) + limit.count();

    if (t == 0 || t > upperbound) {
        t = upperbound;
    }

    return t;
}

void mock_time_travel(int by) {
    time_travel_offset += by;
}

struct MockServerCoreApi : public ServerCoreIface {
    rel_time_t get_current_time() override {
        return mock_get_current_time();
    }
    rel_time_t realtime(rel_time_t exptime) override {
        return mock_realtime(exptime);
    }
    time_t abstime(rel_time_t exptime) override {
        return mock_abstime(exptime);
    }
    time_t limit_abstime(time_t t, std::chrono::seconds limit) override {
        return mock_limit_abstime(t, limit);
    }
    ThreadPoolConfig getThreadPoolSizes() override {
        return {};
    }
    size_t getMaxEngineFileDescriptors() override {
        // 1024 is kind of an abitrary limit (it just needs to be greater than
        // the number of reserved file descriptors in the environment) but we
        // don't link to the engine in mock_server so we can't get the
        // environment to calculate the value.
        return 1024;
    }
    size_t getQuotaSharingPagerConcurrency() override {
        return 2;
    }
    bool isCollectionsEnabled() const override {
        return true;
    }
    bool isServerlessDeployment() const override {
        return false;
    }
};

void cb::server::document_expired(const EngineIface&, size_t) {
    // empty
}

struct MockServerBucketApi : public ServerBucketIface {
    std::optional<AssociatedBucketHandle> tryAssociateBucket(
            EngineIface* engine) const override {
        return AssociatedBucketHandle(engine, [](auto*) {});
    }
};

void mock_register_cookie(CookieIface& cookie) {
    auto [it, inserted] = cookieNotifications.lock()->try_emplace(&cookie);
    if (!inserted) {
        throw std::logic_error(fmt::format(
                "mock_register_cookie(): Cookie '{}' already exists in "
                "cookieNotifications map.",
                reinterpret_cast<void*>(&cookie)));
    }
}

void mock_unregister_cookie(CookieIface& cookie) {
    auto locked = cookieNotifications.lock();
    auto it = locked->find(&cookie);
    if (it == locked->end()) {
        throw std::logic_error(fmt::format(
                "mock_unregister_cookie(): Cookie '{}' does not exist "
                "in cookieNotifications map.",
                reinterpret_cast<const void*>(&cookie)));
    }
    locked->erase(it);
}

cb::engine_errc mock_waitfor_cookie(CookieIface* cookie) {
    Expects(cookie);
    // Wait for at least one element to be present in this cookie's
    // notification queue.
    // Using at() here as the cookie should have been registered in
    // cookieNotifications when it was created.
    auto locked = cookieNotifications.lock();
    cookieNotificationSignal.wait(locked.as_lock(), [&locked, cookie] {
        return !locked->at(cookie).empty();
    });
    auto& notificationQueue = (*locked)[cookie];
    auto status = notificationQueue.front();
    notificationQueue.pop();
    return status;
}

bool mock_cookie_notified(CookieIface* cookie) {
    Expects(cookie);
    return !cookieNotifications.lock()->at(cookie).empty();
}

ServerApi* get_mock_server_api() {
    static MockServerCoreApi core_api;
    static MockServerBucketApi server_bucket_api;
    static ServerApi rv;
    static int init;
    if (!init) {
        init = 1;
        rv.core = &core_api;
        rv.bucket = &server_bucket_api;
    }

   return &rv;
}

void init_mock_server() {
    process_started = time(nullptr);
    time_travel_offset = 0;
    log_level = spdlog::level::level_enum::critical;
}
