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
#include "daemon/doc_pre_expiry.h"
#include "mock_cookie.h"
#include <logger/logger.h>
#include <memcached/config_parser.h>
#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_core_iface.h>
#include <memcached/server_document_iface.h>
#include <platform/platform_time.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <gsl/gsl-lite.hpp>
#include <array>
#include <atomic>
#include <cstring>
#include <ctime>
#include <list>
#include <mutex>

#define REALTIME_MAXDELTA 60*60*24*3

std::atomic<time_t> process_started;     /* when the mock server was started */

/* Offset from 'real' time used to test time handling */
std::atomic<rel_time_t> time_travel_offset;

spdlog::level::level_enum log_level = spdlog::level::level_enum::info;

/* Forward declarations */

static PreLinkFunction pre_link_function;

void mock_set_pre_link_function(PreLinkFunction function) {
    pre_link_function = std::move(function);
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

static int mock_parse_config(const char *str, struct config_item items[], FILE *error) {
    return parse_config(str, items, error);
}

void mock_init_alloc_hooks() {
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
    int parse_config(const char* str,
                     config_item* items,
                     FILE* error) override {
        return mock_parse_config(str, items, error);
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
    bool isCollectionsEnabled() const override {
        return true;
    }
};

struct MockServerDocumentApi : public ServerDocumentIface {
    cb::engine_errc pre_link(CookieIface& cookie, item_info& info) override {
        if (pre_link_function) {
            pre_link_function(info);
        }

        return cb::engine_errc::success;
    }

    std::string pre_expiry(const item_info& itm_info) override {
        return document_pre_expiry(itm_info);
    }

    void audit_document_access(
            CookieIface& cookie,
            cb::audit::document::Operation operation) override {
        // empty
    }
};

static uint32_t privilege_context_revision = 0;
void mock_set_privilege_context_revision(uint32_t rev) {
    privilege_context_revision = rev;
}

uint32_t mock_get_privilege_context_revision() {
    return privilege_context_revision;
}

struct MockServerCookieApi : public ServerCookieIface {
    void setDcpConnHandler(const CookieIface& cookie,
                           DcpConnHandlerIface* handler) override {
        cookie_to_mock_cookie(cookie).setConHandler(handler);
    }
    DcpConnHandlerIface* getDcpConnHandler(const CookieIface& cookie) override {
        return cookie_to_mock_cookie(cookie).getConHandler();
    }
    void setDcpFlowControlBufferSize(const CookieIface& cookie,
                                     std::size_t size) override {
    }

    void reserve(const CookieIface& cookie) override {
        const_cast<CookieIface&>(cookie).incrementRefcount();
    }

    void release(const CookieIface& cookie) override {
        auto* c = cookie_to_mock_cookie(&cookie);
        if (c->decrementRefcount() == 0) {
            delete c;
        }
    }

    void set_priority(const CookieIface& cookie, ConnectionPriority) override {
        // Just verify the cookie type
        cookie_to_mock_cookie(cookie);
    }

    ConnectionPriority get_priority(const CookieIface& cookie) override {
        // Just verify the cookie type
        cookie_to_mock_cookie(cookie);
        return ConnectionPriority::Medium;
    }

    uint64_t get_connection_id(const CookieIface& cookie) override {
        return cookie_to_mock_cookie(cookie).getConnectionId();
    }

    cb::rbac::PrivilegeAccess check_privilege(
            const CookieIface& cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override {
        return cookie.testPrivilege(privilege, sid, cid);
    }

    uint32_t get_privilege_context_revision(
            const CookieIface& cookie) override {
        return privilege_context_revision;
    }

    cb::mcbp::Status engine_error2mcbp(const CookieIface& cookie,
                                       cb::engine_errc code) override {
        if (code == cb::engine_errc::disconnect) {
            return cb::mcbp::Status(cb::engine_errc(-1));
        }

        return cb::mcbp::to_status(cb::engine_errc(code));
    }

    std::pair<uint32_t, std::string> get_log_info(
            const CookieIface& cookie) override {
        // The DCP test suite don't use a real cookie, and until we've
        // fixed that we can't try to use the provided cookie
        return std::make_pair(uint32_t(0xdead), std::string{"[you - me]"});
    }

    std::string get_authenticated_user(const CookieIface& cookie) override {
        return cookie_to_mock_cookie(cookie).getAuthedUser();
    }

    in_port_t get_connected_port(const CookieIface& cookie) override {
        return cookie_to_mock_cookie(cookie).getParentPort();
    }

    void set_error_context(CookieIface& cookie,
                           std::string_view message) override {
    }

    void set_error_json_extras(CookieIface& cookie,
                               const nlohmann::json& json) override {
    }

    void set_unknown_collection_error_context(CookieIface& cookie,
                                              uint64_t manifestUid) override {
    }

    void notify_io_complete(const CookieIface& cookie,
                            cb::engine_errc status) override {
        auto& c = cookie_to_mock_cookie(cookie);
        c.handleIoComplete(status);
    }

    void scheduleDcpStep(const CookieIface& cookie) override {
        notify_io_complete(cookie, cb::engine_errc::success);
    }
};

ServerApi* get_mock_server_api() {
    static MockServerCoreApi core_api;
    static MockServerCookieApi server_cookie_api;
    static ServerApi rv;
    static MockServerDocumentApi document_api;
    static int init;
    if (!init) {
        init = 1;
        rv.core = &core_api;
        rv.cookie = &server_cookie_api;
        rv.document = &document_api;
    }

   return &rv;
}

void init_mock_server() {
    process_started = time(nullptr);
    time_travel_offset = 0;
    log_level = spdlog::level::level_enum::critical;
}
