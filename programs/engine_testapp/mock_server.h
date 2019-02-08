#pragma once

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_callback_iface.h>
#include <platform/platform.h>

#include <atomic>
#include <bitset>
#include <string>

#include "tracing/tracer.h"

struct MockCookie : cb::tracing::Traceable {
    MockCookie();

    const uint64_t magic{MAGIC};
    void* engine_data{};
    bool connected{};
    int sfd{};
    ENGINE_ERROR_CODE status{ENGINE_SUCCESS};
    int nblocks{0}; /* number of ewouldblocks */
    bool handle_ewouldblock{true};
    bool handle_mutation_extras{true};
    std::bitset<8> enabled_datatypes;
    bool handle_collections_support{false};
    cb_mutex_t mutex;
    cb_cond_t cond;
    int references{1};
    uint64_t num_io_notifications{};
    uint64_t num_processed_notifications{};

    void validate() const;

protected:
    static const uint64_t MAGIC = 0xbeefcafecafebeefULL;
};

struct mock_callbacks {
    EVENT_CALLBACK cb;
    const void *cb_data;
};

MEMCACHED_PUBLIC_API void mock_init_alloc_hooks();

MEMCACHED_PUBLIC_API SERVER_HANDLE_V1* get_mock_server_api();

MEMCACHED_PUBLIC_API void init_mock_server();

MEMCACHED_PUBLIC_API const void* create_mock_cookie();

MEMCACHED_PUBLIC_API void destroy_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void mock_set_ewouldblock_handling(const void *cookie, bool enable);

MEMCACHED_PUBLIC_API void mock_set_mutation_extras_handling(const void *cookie,
                                                            bool enable);

MEMCACHED_PUBLIC_API void mock_set_collections_support(const void *cookie,
                                                       bool enable);

MEMCACHED_PUBLIC_API void mock_set_datatype_support(
        const void* cookie, protocol_binary_datatype_t datatypes);

MEMCACHED_PUBLIC_API void lock_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void unlock_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void waitfor_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void mock_time_travel(int by);

MEMCACHED_PUBLIC_API void disconnect_all_mock_connections();

MEMCACHED_PUBLIC_API void destroy_mock_event_callbacks();

MEMCACHED_PUBLIC_API int get_number_of_mock_cookie_references(const void *cookie);

MEMCACHED_PUBLIC_API size_t
get_number_of_mock_cookie_io_notifications(const void* cookie);

void mock_set_pre_link_function(PreLinkFunction function);

MEMCACHED_PUBLIC_API cb::tracing::Traceable& mock_get_traceable(
        const void* cookie);
