#pragma once

#include <memcached/engine_testapp.h>
#include <memcached/server_callback_iface.h>

struct mock_callbacks {
    EVENT_CALLBACK cb;
    const void *cb_data;
};

void mock_init_alloc_hooks();

SERVER_HANDLE_V1* get_mock_server_api();

void init_mock_server();

void mock_time_travel(int by);

void destroy_mock_event_callbacks();

void mock_set_pre_link_function(PreLinkFunction function);

void mock_perform_callbacks(ENGINE_EVENT_TYPE type,
                            const void* data,
                            const void* c);
