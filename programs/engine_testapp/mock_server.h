#pragma once

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_callback_iface.h>

#include <atomic>
#include <bitset>
#include <condition_variable>
#include <mutex>
#include <string>

#include "tracing/tracer.h"

struct MockCookie : cb::tracing::Traceable {
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
    std::mutex mutex;
    std::condition_variable cond;
    int references{1};
    uint64_t num_io_notifications{};
    uint64_t num_processed_notifications{};
    std::string authenticatedUser{"nobody"};
    in_port_t parent_port{666};

    void validate() const;

protected:
    static const uint64_t MAGIC = 0xbeefcafecafebeefULL;
};

struct mock_callbacks {
    EVENT_CALLBACK cb;
    const void *cb_data;
};

void mock_init_alloc_hooks();

SERVER_HANDLE_V1* get_mock_server_api();

void init_mock_server();

const void* create_mock_cookie();

void destroy_mock_cookie(const void* cookie);

void mock_set_ewouldblock_handling(const void* cookie, bool enable);

void mock_set_mutation_extras_handling(const void* cookie, bool enable);

void mock_set_collections_support(const void* cookie, bool enable);

void mock_set_datatype_support(const void* cookie,
                               protocol_binary_datatype_t datatypes);

void lock_mock_cookie(const void* cookie);

void unlock_mock_cookie(const void* cookie);

void waitfor_mock_cookie(const void* cookie);

void mock_time_travel(int by);

void disconnect_all_mock_connections();

void destroy_mock_event_callbacks();

int get_number_of_mock_cookie_references(const void* cookie);

size_t get_number_of_mock_cookie_io_notifications(const void* cookie);

void mock_set_pre_link_function(PreLinkFunction function);

cb::tracing::Traceable& mock_get_traceable(const void* cookie);

MockCookie* cookie_to_mock_object(const void* cookie);
