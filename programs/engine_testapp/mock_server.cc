/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_allocator_iface.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_core_iface.h>
#include <memcached/server_document_iface.h>
#include <memcached/server_log_iface.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <xattr/blob.h>
#include <xattr/utils.h>
#include <atomic>
#include <iostream>

#include "daemon/alloc_hooks.h"
#include "daemon/doc_pre_expiry.h"
#include "daemon/protocol/mcbp/engine_errc_2_mcbp.h"
#include "mock_server.h"

#include <logger/logger.h>
#include <array>
#include <gsl/gsl>
#include <list>

#define REALTIME_MAXDELTA 60*60*24*3
#define CONN_MAGIC 0xbeefcafe

std::array<std::list<mock_callbacks>, MAX_ENGINE_EVENT_TYPE + 1> mock_event_handlers;

std::atomic<time_t> process_started;     /* when the mock server was started */

/* Offset from 'real' time used to test time handling */
std::atomic<rel_time_t> time_travel_offset;

/* ref_mutex to guard references, and object deletion in case references becomes zero */
static cb_mutex_t ref_mutex;
spdlog::level::level_enum log_level = spdlog::level::level_enum::info;

/**
 * Session cas elements
 */
uint64_t session_cas;
uint8_t session_ctr;
cb_mutex_t session_mutex;

mock_connstruct::mock_connstruct()
    : magic(CONN_MAGIC),
      engine_data(nullptr),
      connected(true),
      sfd(0),
      status(ENGINE_SUCCESS),
      nblocks(0),
      handle_ewouldblock(true),
      handle_mutation_extras(true),
      handle_collections_support(false),
      references(1),
      num_io_notifications(0),
      num_processed_notifications(0) {
    cb_mutex_initialize(&mutex);
    cb_cond_initialize(&cond);
}

mock_connstruct::~mock_connstruct() {
}

/* Forward declarations */

void disconnect_mock_connection(struct mock_connstruct *c);

static mock_connstruct* cookie_to_mock_object(const void* cookie) {
  return const_cast<mock_connstruct*>(reinterpret_cast<const mock_connstruct*>(cookie));
}

static PreLinkFunction pre_link_function;

void mock_set_pre_link_function(PreLinkFunction function) {
    pre_link_function = function;
}

/* time-sensitive callers can call it by hand with this, outside the
   normal ever-1-second timer */
static rel_time_t mock_get_current_time(void) {
#ifdef WIN32
    rel_time_t result = (rel_time_t)(time(NULL) - process_started + time_travel_offset);
#else
    struct timeval timer;
    gettimeofday(&timer, NULL);
    rel_time_t result = (rel_time_t) (timer.tv_sec - process_started + time_travel_offset);
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
};

struct MockServerLogApi : public ServerLogIface {
    spdlog::logger* get_spdlogger() override {
        return cb::logger::get();
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

    bool pre_expiry(item_info& itm_info) override {
        return document_pre_expiry(itm_info);
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
        cb_assert(c->magic == CONN_MAGIC);
        c->engine_data = engine_data;
    }

    void* get_engine_specific(gsl::not_null<const void*> cookie) override {
        const auto* c = reinterpret_cast<const mock_connstruct*>(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
        return c->engine_data;
    }

    bool is_datatype_supported(gsl::not_null<const void*> cookie,
                               protocol_binary_datatype_t datatype) override {
        const auto* c = reinterpret_cast<const mock_connstruct*>(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
        std::bitset<8> in(datatype);
        return (c->enabled_datatypes & in) == in;
    }

    bool is_mutation_extras_supported(
            gsl::not_null<const void*> cookie) override {
        const auto* c = reinterpret_cast<const mock_connstruct*>(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
        return c->handle_mutation_extras;
    }

    bool is_collections_supported(gsl::not_null<const void*> cookie) override {
        const auto* c = reinterpret_cast<const mock_connstruct*>(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
        return c->handle_collections_support;
    }

    cb::mcbp::ClientOpcode get_opcode_if_ewouldblock_set(
            gsl::not_null<const void*> cookie) override {
        const auto* c = reinterpret_cast<const mock_connstruct*>(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
        return cb::mcbp::ClientOpcode::Invalid;
    }

    bool validate_session_cas(uint64_t cas) override {
        bool ret = true;
        cb_mutex_enter(&(session_mutex));
        if (cas != 0) {
            if (session_cas != cas) {
                ret = false;
            } else {
                session_ctr++;
            }
        } else {
            session_ctr++;
        }
        cb_mutex_exit(&(session_mutex));
        return ret;
    }

    void decrement_session_ctr() override {
        cb_mutex_enter(&(session_mutex));
        cb_assert(session_ctr != 0);
        session_ctr--;
        cb_mutex_exit(&(session_mutex));
    }

    ENGINE_ERROR_CODE reserve(gsl::not_null<const void*> cookie) override {
        cb_mutex_enter(&(ref_mutex));
        auto* c = cookie_to_mock_object(cookie.get());
        c->references++;
        cb_mutex_exit(&(ref_mutex));
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE release(gsl::not_null<const void*> cookie) override {
        cb_mutex_enter(&(ref_mutex));
        auto* c = cookie_to_mock_object(cookie.get());

        const int new_rc = --c->references;
        if (new_rc == 0) {
            delete c;
        }
        cb_mutex_exit(&(ref_mutex));
        return ENGINE_SUCCESS;
    }
    void set_priority(gsl::not_null<const void*> cookie,
                      CONN_PRIORITY) override {
        auto* c = cookie_to_mock_object(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
    }

    CONN_PRIORITY get_priority(gsl::not_null<const void*> cookie) override {
        auto* c = cookie_to_mock_object(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
        return CONN_PRIORITY_MED;
    }

    bucket_id_t get_bucket_id(gsl::not_null<const void*> cookie) override {
        throw std::runtime_error(
                "MockServerCookieApi::get_bucket_id() not implemented");
    }

    uint64_t get_connection_id(gsl::not_null<const void*> cookie) override {
        auto* c = cookie_to_mock_object(cookie.get());
        cb_assert(c->magic == CONN_MAGIC);
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

    void set_error_context(gsl::not_null<void*> cookie,
                           cb::const_char_buffer message) override {
    }

    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status) override {
        auto* c = cookie_to_mock_object(cookie.get());
        cb_mutex_enter(&c->mutex);
        c->status = status;
        c->num_io_notifications++;
        cb_cond_signal(&c->cond);
        cb_mutex_exit(&c->mutex);
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
    process_started = time(0);
    time_travel_offset = 0;
    log_level = spdlog::level::level_enum::critical;

    session_cas = 0x0102030405060708;
    session_ctr = 0;
    cb_mutex_initialize(&session_mutex);
    cb_mutex_initialize(&ref_mutex);
}

const void *create_mock_cookie(void) {
    struct mock_connstruct *rv = new mock_connstruct();
    return rv;
}

void destroy_mock_cookie(const void *cookie) {
    if (cookie == nullptr) {
        return;
    }
    cb_mutex_enter(&(ref_mutex));
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    disconnect_mock_connection(c);
    if (c->references == 0) {
        delete c;
    }
    cb_mutex_exit(&(ref_mutex));
}

void mock_set_ewouldblock_handling(const void *cookie, bool enable) {
    mock_connstruct *c = cookie_to_mock_object(cookie);
    c->handle_ewouldblock = enable;
}

void mock_set_mutation_extras_handling(const void *cookie, bool enable) {
    mock_connstruct *c = cookie_to_mock_object(cookie);
    c->handle_mutation_extras = enable;
}

void mock_set_datatype_support(const void* cookie,
                               protocol_binary_datatype_t datatypes) {
    mock_connstruct *c = cookie_to_mock_object(cookie);
    c->enabled_datatypes = std::bitset<8>(datatypes);
}

void mock_set_collections_support(const void *cookie, bool enable) {
    mock_connstruct *c = cookie_to_mock_object(cookie);
    c->handle_collections_support = enable;
}

void lock_mock_cookie(const void *cookie) {
    mock_connstruct *c = cookie_to_mock_object(cookie);
    cb_mutex_enter(&c->mutex);
}

void unlock_mock_cookie(const void *cookie) {
    mock_connstruct *c = cookie_to_mock_object(cookie);
    cb_mutex_exit(&c->mutex);
}

void waitfor_mock_cookie(const void *cookie) {
    mock_connstruct *c = cookie_to_mock_object(cookie);
    while (c->num_processed_notifications == c->num_io_notifications) {
        cb_cond_wait(&c->cond, &c->mutex);
    }
    c->num_processed_notifications = c->num_io_notifications;
}

void disconnect_mock_connection(struct mock_connstruct *c) {
    // ref_mutex already held in calling function
    c->connected = false;
    c->references--;
    mock_perform_callbacks(ON_DISCONNECT, NULL, c);
}

void disconnect_all_mock_connections(void) {
    // Currently does nothing; we don't track mock_connstructs
}

void destroy_mock_event_callbacks(void) {
    for (int type = 0; type < MAX_ENGINE_EVENT_TYPE; type++) {
        mock_event_handlers[type].clear();
    }
}

int get_number_of_mock_cookie_references(const void *cookie) {
    if (cookie == nullptr) {
        return -1;
    }
    cb_mutex_enter(&(ref_mutex));
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    int numberOfReferences = c->references;
    cb_mutex_exit(&(ref_mutex));
    return numberOfReferences;
}

size_t get_number_of_mock_cookie_io_notifications(const void* cookie) {
    mock_connstruct* c = cookie_to_mock_object(cookie);
    return c->num_io_notifications;
}

MEMCACHED_PUBLIC_API cb::tracing::Traceable& mock_get_traceable(
        const void* cookie) {
    mock_connstruct* c = cookie_to_mock_object(cookie);
    return *c;
}
