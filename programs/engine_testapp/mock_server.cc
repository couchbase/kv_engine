/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "daemon/alloc_hooks.h"
#include "daemon/doc_pre_expiry.h"
#include "daemon/protocol/mcbp/engine_errc_2_mcbp.h"
#include "mock_server.h"
#include <logger/logger.h>
#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_allocator_iface.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_core_iface.h>
#include <memcached/server_document_iface.h>
#include <memcached/server_log_iface.h>
#include <platform/cbassert.h>
#include <platform/platform_time.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <gsl/gsl>
#include <iostream>
#include <list>
#include <mutex>

#define REALTIME_MAXDELTA 60*60*24*3

std::array<std::list<mock_callbacks>, MAX_ENGINE_EVENT_TYPE + 1> mock_event_handlers;

std::atomic<time_t> process_started;     /* when the mock server was started */

/* Offset from 'real' time used to test time handling */
std::atomic<rel_time_t> time_travel_offset;

/* ref_mutex to guard references, and object deletion in case references becomes zero */
static std::mutex ref_mutex;
spdlog::level::level_enum log_level = spdlog::level::level_enum::info;

/**
 * Session cas elements
 */
uint64_t session_cas;
uint8_t session_ctr;
static std::mutex session_mutex;

void MockCookie::validate() const {
    if (magic != MAGIC) {
        throw std::runtime_error("MockCookie::validate(): Invalid magic");
    }
}

/* Forward declarations */

void disconnect_mock_connection(struct MockCookie* c);

MockCookie* cookie_to_mock_object(const void* cookie) {
    auto* ret = reinterpret_cast<const MockCookie*>(cookie);
    ret->validate();
    return const_cast<MockCookie*>(ret);
}

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

static void mock_perform_callbacks(ENGINE_EVENT_TYPE type,
                                   const void *data,
                                   const void *c) {
    for (auto& h : mock_event_handlers[type]) {
        h.cb(c, type, data, h.cb_data);
    }
}

void mock_init_alloc_hooks() {
    AllocHooks::initialize();
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
    void shutdown() override {
        throw std::runtime_error(
                "MockServerCoreApi::shutdown() not implemented");
    }
    size_t get_max_item_iovec_size() override {
        return 1;
    }

    void trigger_tick() override {
        throw std::runtime_error(
                "MockServerCoreApi::trigger_tick() not implemented");
    }

    ThreadPoolConfig getThreadPoolSizes() override {
        return {};
    }
    bool isCollectionsEnabled() const override {
        return true;
    }
};

struct MockServerLogApi : public ServerLogIface {
    spdlog::logger* get_spdlogger() override {
        return cb::logger::get();
    }

    void register_spdlogger(std::shared_ptr<spdlog::logger> l) override {
        cb::logger::registerSpdLogger(l);
    }

    void unregister_spdlogger(const std::string& n) override {
        cb::logger::unregisterSpdLogger(n);
    }

    void set_level(spdlog::level::level_enum severity) override {
        log_level = severity;
    }
};

struct MockServerDocumentApi : public ServerDocumentIface {
    ENGINE_ERROR_CODE pre_link(gsl::not_null<const void*> cookie,
                               item_info& info) override {
        if (pre_link_function) {
            pre_link_function(info);
        }

        return ENGINE_SUCCESS;
    }

    std::string pre_expiry(const item_info& itm_info) override {
        return document_pre_expiry(itm_info);
    }

    void audit_document_access(
            gsl::not_null<const void*> cookie,
            cb::audit::document::Operation operation) override {
        // empty
    }
};

struct MockServerCallbackApi : public ServerCallbackIface {
    void register_callback(EngineIface* engine,
                           ENGINE_EVENT_TYPE type,
                           EVENT_CALLBACK cb,
                           const void* cb_data) override {
        mock_event_handlers[type].emplace_back(mock_callbacks{cb, cb_data});
    }

    void perform_callbacks(ENGINE_EVENT_TYPE type,
                           const void* data,
                           const void* cookie) override {
        mock_perform_callbacks(type, data, cookie);
    }
};

struct MockServerCookieApi : public ServerCookieIface {
    void store_engine_specific(gsl::not_null<const void*> cookie,
                               void* engine_data) override {
        auto* c = cookie_to_mock_object(cookie.get());
        c->engine_data = engine_data;
    }

    void* get_engine_specific(gsl::not_null<const void*> cookie) override {
        const auto* c = cookie_to_mock_object(cookie.get());
        return c->engine_data;
    }

    bool is_datatype_supported(gsl::not_null<const void*> cookie,
                               protocol_binary_datatype_t datatype) override {
        const auto* c = cookie_to_mock_object(cookie.get());
        std::bitset<8> in(datatype);
        return (c->enabled_datatypes & in) == in;
    }

    bool is_mutation_extras_supported(
            gsl::not_null<const void*> cookie) override {
        const auto* c = cookie_to_mock_object(cookie.get());
        return c->handle_mutation_extras;
    }

    bool is_collections_supported(gsl::not_null<const void*> cookie) override {
        const auto* c = cookie_to_mock_object(cookie.get());
        return c->handle_collections_support;
    }

    cb::mcbp::ClientOpcode get_opcode_if_ewouldblock_set(
            gsl::not_null<const void*> cookie) override {
        (void)cookie_to_mock_object(cookie.get()); // validate cookie
        return cb::mcbp::ClientOpcode::Invalid;
    }

    bool validate_session_cas(uint64_t cas) override {
        bool ret = true;
        std::lock_guard<std::mutex> guard(session_mutex);
        if (cas != 0) {
            if (session_cas != cas) {
                ret = false;
            } else {
                session_ctr++;
            }
        } else {
            session_ctr++;
        }
        return ret;
    }

    void decrement_session_ctr() override {
        std::lock_guard<std::mutex> guard(session_mutex);
        cb_assert(session_ctr != 0);
        session_ctr--;
    }

    ENGINE_ERROR_CODE reserve(gsl::not_null<const void*> cookie) override {
        std::lock_guard<std::mutex> guard(ref_mutex);
        auto* c = cookie_to_mock_object(cookie.get());
        c->references++;
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE release(gsl::not_null<const void*> cookie) override {
        std::lock_guard<std::mutex> guard(ref_mutex);
        auto* c = cookie_to_mock_object(cookie.get());

        const int new_rc = --c->references;
        if (new_rc == 0) {
            delete c;
        }
        return ENGINE_SUCCESS;
    }
    void set_priority(gsl::not_null<const void*> cookie,
                      CONN_PRIORITY) override {
        (void)cookie_to_mock_object(cookie.get()); // validate cookie
    }

    CONN_PRIORITY get_priority(gsl::not_null<const void*> cookie) override {
        (void)cookie_to_mock_object(cookie.get()); // validate cookie
        return CONN_PRIORITY_MED;
    }

    bucket_id_t get_bucket_id(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error(
                "MockServerCookieApi::get_bucket_id() not implemented");
    }

    uint64_t get_connection_id(gsl::not_null<const void*> cookie) override {
        auto* c = cookie_to_mock_object(cookie.get());
        return c->sfd;
    }

    cb::rbac::PrivilegeAccess check_privilege(
            gsl::not_null<const void*> cookie,
            cb::rbac::Privilege privilege) override {
        // @todo allow for mocking privilege access
        return cb::rbac::PrivilegeAccess::Ok;
    }

    cb::mcbp::Status engine_error2mcbp(gsl::not_null<const void*> cookie,
                                       ENGINE_ERROR_CODE code) override {
        if (code == ENGINE_DISCONNECT) {
            return cb::mcbp::Status(cb::engine_errc(-1));
        }

        return cb::mcbp::to_status(cb::engine_errc(code));
    }

    std::pair<uint32_t, std::string> get_log_info(
            gsl::not_null<const void*> cookie) override {
        // The DCP test suite don't use a real cookie, and until we've
        // fixed that we can't try to use the provided cookie
        return std::make_pair(uint32_t(0xdead), std::string{"[you - me]"});
    }

    std::string get_authenticated_user(
            gsl::not_null<const void*> cookie) override {
        auto* c = cookie_to_mock_object(cookie.get());
        return c->authenticatedUser;
    }

    in_port_t get_connected_port(gsl::not_null<const void*> cookie) override {
        auto* c = cookie_to_mock_object(cookie.get());
        return c->parent_port;
    }

    void set_error_context(gsl::not_null<void*> cookie,
                           cb::const_char_buffer message) override {
    }

    void set_error_json_extras(gsl::not_null<void*> cookie,
                               const nlohmann::json& json) override {
    }

    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status) override {
        auto* c = cookie_to_mock_object(cookie.get());
        std::lock_guard<std::mutex> guard(c->mutex);
        c->status = status;
        c->num_io_notifications++;
        c->cond.notify_all();
    }
};

SERVER_HANDLE_V1* get_mock_server_api() {
    static MockServerCoreApi core_api;
    static MockServerCookieApi server_cookie_api;
    static MockServerCallbackApi callback_api;
    static MockServerLogApi log_api;
    static ServerAllocatorIface hooks_api;
    static SERVER_HANDLE_V1 rv;
    static MockServerDocumentApi document_api;
    static int init;
    if (!init) {
        init = 1;

        hooks_api.add_new_hook = AllocHooks::add_new_hook;
        hooks_api.remove_new_hook = AllocHooks::remove_new_hook;
        hooks_api.add_delete_hook = AllocHooks::add_delete_hook;
        hooks_api.remove_delete_hook = AllocHooks::remove_delete_hook;
        hooks_api.get_extra_stats_size = AllocHooks::get_extra_stats_size;
        hooks_api.get_allocator_stats = AllocHooks::get_allocator_stats;
        hooks_api.get_allocation_size = AllocHooks::get_allocation_size;
        hooks_api.get_detailed_stats = AllocHooks::get_detailed_stats;
        hooks_api.release_free_memory = AllocHooks::release_free_memory;
        hooks_api.enable_thread_cache = AllocHooks::enable_thread_cache;
        hooks_api.get_allocator_property = AllocHooks::get_allocator_property;

        rv.core = &core_api;
        rv.callback = &callback_api;
        rv.log = &log_api;
        rv.cookie = &server_cookie_api;
        rv.alloc_hooks = &hooks_api;
        rv.document = &document_api;
    }

   return &rv;
}

void init_mock_server() {
    process_started = time(nullptr);
    time_travel_offset = 0;
    log_level = spdlog::level::level_enum::critical;

    session_cas = 0x0102030405060708;
    session_ctr = 0;
}

const void* create_mock_cookie() {
    return new MockCookie();
}

void destroy_mock_cookie(const void *cookie) {
    if (cookie == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> guard(ref_mutex);
    auto* c = cookie_to_mock_object(cookie);
    disconnect_mock_connection(c);
    if (c->references == 0) {
        delete c;
    }
}

void mock_set_ewouldblock_handling(const void *cookie, bool enable) {
    auto* c = cookie_to_mock_object(cookie);
    c->handle_ewouldblock = enable;
}

void mock_set_mutation_extras_handling(const void *cookie, bool enable) {
    auto* c = cookie_to_mock_object(cookie);
    c->handle_mutation_extras = enable;
}

void mock_set_datatype_support(const void* cookie,
                               protocol_binary_datatype_t datatypes) {
    auto* c = cookie_to_mock_object(cookie);
    c->enabled_datatypes = std::bitset<8>(datatypes);
}

void mock_set_collections_support(const void *cookie, bool enable) {
    auto* c = cookie_to_mock_object(cookie);
    c->handle_collections_support = enable;
}

void lock_mock_cookie(const void *cookie) {
    auto* c = cookie_to_mock_object(cookie);
    c->mutex.lock();
}

void unlock_mock_cookie(const void *cookie) {
    auto* c = cookie_to_mock_object(cookie);
    c->mutex.unlock();
}

void waitfor_mock_cookie(const void *cookie) {
    auto* c = cookie_to_mock_object(cookie);

    std::unique_lock<std::mutex> lock(c->mutex, std::adopt_lock);
    if (!lock.owns_lock()) {
        throw std::logic_error("waitfor_mock_cookie: cookie should be locked!");
    }

    c->cond.wait(lock, [&c] {
        return c->num_processed_notifications != c->num_io_notifications;
    });
    c->num_processed_notifications = c->num_io_notifications;

    lock.release();
}

void disconnect_mock_connection(struct MockCookie* c) {
    // ref_mutex already held in calling function
    c->connected = false;
    c->references--;
    mock_perform_callbacks(ON_DISCONNECT, nullptr, c);
}

void disconnect_all_mock_connections() {
    // Currently does nothing; we don't track mock_connstructs
}

void destroy_mock_event_callbacks() {
    for (int type = 0; type < MAX_ENGINE_EVENT_TYPE; type++) {
        mock_event_handlers[type].clear();
    }
}

int get_number_of_mock_cookie_references(const void *cookie) {
    if (cookie == nullptr) {
        return -1;
    }
    std::lock_guard<std::mutex> guard(ref_mutex);
    auto* c = cookie_to_mock_object(cookie);
    return c->references;
}

size_t get_number_of_mock_cookie_io_notifications(const void* cookie) {
    auto* c = cookie_to_mock_object(cookie);
    return c->num_io_notifications;
}
