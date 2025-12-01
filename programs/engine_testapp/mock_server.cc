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
#include <platform/atomic_duration.h>
#include <platform/platform_time.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <xattr/blob.h>
#include <xattr/utils.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <ctime>
#include <list>
#include <mutex>
#include <queue>

#define REALTIME_MAXDELTA 60*60*24*3

std::atomic<time_t> process_started;     /* when the mock server was started */

// When the mock server was started, std::chrono variant
cb::AtomicTimePoint<> process_started_steady;

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

static std::chrono::steady_clock::time_point mock_get_uptime_now() {
    // Want to return a time_point in terms of time since our server startup
    // (not since steady_clock's epoch), so need to first calculate the
    // duration since startup, then conver to time_point.
    auto duration_since_start = std::chrono::steady_clock::now() -
                                process_started_steady.load() +
                                std::chrono::seconds(time_travel_offset);
    return std::chrono::steady_clock::time_point(duration_since_start);
}

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
        rv = exptime + mock_get_current_time();
    }

    return rv;
}

static time_t mock_abstime(const rel_time_t exptime)
{
    return process_started + exptime;
}

static uint32_t mock_limit_expiry_time(uint32_t t, std::chrono::seconds limit) {
    auto upperbound = mock_abstime(mock_get_current_time()) + limit.count();

    if (t == 0 || t > upperbound) {
        t = gsl::narrow<uint32_t>(upperbound);
    }

    return t;
}

void mock_time_travel(int by) {
    time_travel_offset += by;
}

static std::chrono::seconds dcp_disconnect_when_stuck_timeout;
static std::string dcp_disconnect_when_stuck_name_regex;
static bool not_locked_returns_tmpfail;
static double dcp_consumer_enable_max_marker_version;
static bool dcp_snapshot_marker_hps_enabled;
static bool dcp_snapshot_marker_purge_seqno_enabled;
static std::atomic_bool magma_blind_write_optimisation_enabled;

void mock_set_dcp_disconnect_when_stuck_timeout(std::chrono::seconds timeout) {
    dcp_disconnect_when_stuck_timeout = timeout;
}

void mock_set_dcp_disconnect_when_stuck_name_regex(std::string regex) {
    dcp_disconnect_when_stuck_name_regex = regex;
}

void mock_set_not_locked_returns_tmpfail(bool value) {
    not_locked_returns_tmpfail = value;
}

void mock_set_dcp_consumer_max_marker_version(double value) {
    dcp_consumer_enable_max_marker_version = value;
}

void mock_set_dcp_snapshot_marker_hps_enabled(bool value) {
    dcp_snapshot_marker_hps_enabled = value;
}

void mock_set_dcp_snapshot_marker_purge_seqno_enabled(bool value) {
    dcp_snapshot_marker_purge_seqno_enabled = value;
}

void mock_set_magma_blind_write_optimisation_enabled(bool enabled) {
    magma_blind_write_optimisation_enabled = enabled;
}

struct MockServerCoreApi : public ServerCoreIface {
    std::chrono::steady_clock::time_point get_uptime_now() override {
        return mock_get_uptime_now();
    }

    rel_time_t get_current_time() override {
        return mock_get_current_time();
    }
    rel_time_t realtime(rel_time_t exptime) override {
        return mock_realtime(exptime);
    }
    time_t abstime(rel_time_t exptime) override {
        return mock_abstime(exptime);
    }
    uint32_t limit_expiry_time(uint32_t t,
                               std::chrono::seconds limit) override {
        return mock_limit_expiry_time(t, limit);
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

    std::chrono::milliseconds getQuotaSharingPagerSleepTime() override {
        using namespace std::chrono_literals;
        return 5000ms;
    }

    std::chrono::seconds getDcpDisconnectWhenStuckTimeout() override {
        return dcp_disconnect_when_stuck_timeout;
    }

    std::string getDcpDisconnectWhenStuckNameRegex() override {
        return dcp_disconnect_when_stuck_name_regex;
    }

    bool getNotLockedReturnsTmpfail() override {
        return not_locked_returns_tmpfail;
    }

    double getDcpConsumerMaxMarkerVersion() override {
        return dcp_consumer_enable_max_marker_version;
    }

    bool isDcpSnapshotMarkerHPSEnabled() override {
        return dcp_snapshot_marker_hps_enabled;
    }

    bool isDcpSnapshotMarkerPurgeSeqnoEnabled() override {
        return dcp_snapshot_marker_purge_seqno_enabled;
    }

    bool isMagmaBlindWriteOptimisationEnabled() override {
        return magma_blind_write_optimisation_enabled.load();
    }

    bool isFileFragmentChecksumEnabled() const override {
        return true;
    }

    size_t getFileFragmentChecksumLength() const override {
        // Use a small chunk size in testing which may help improve test
        // coverage
        return 128;
    }

    bool shouldPrepareSnapshotAlwaysChecksum() const override {
        return true;
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
    process_started_steady = std::chrono::steady_clock::now();

    time_travel_offset = 0;
    log_level = spdlog::level::level_enum::critical;

    dcp_disconnect_when_stuck_timeout = std::chrono::seconds(720);
    dcp_disconnect_when_stuck_name_regex = "";
    not_locked_returns_tmpfail = false;
    dcp_consumer_enable_max_marker_version = 2.2;
    dcp_snapshot_marker_hps_enabled = true;
    dcp_snapshot_marker_purge_seqno_enabled = true;
    magma_blind_write_optimisation_enabled = true;
}
