/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
    Memcached protocol extensions for testapp.

    Provides extended protocol commands to enable interesting testcases.

    1. Currently supports shifting the Memcached's timeofday.
*/

#include "config.h"
#include "extensions/protocol_extension.h"

static EXTENSION_BINARY_PROTOCOL_DESCRIPTOR descriptor;
static SERVER_HANDLE_V1* server;

static const char *get_name(void) {
    return "testapp protocol extension";
}

static ENGINE_ERROR_CODE handle_adjust_time(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*,
                                            ENGINE_HANDLE*,
                                            const void* cookie,
                                            protocol_binary_request_header *request,
                                            ADD_RESPONSE response) {
    auto *req = reinterpret_cast<protocol_binary_adjust_time*>(request);
    int64_t offset = ntohll(req->message.body.offset);
    TimeType timeType = req->message.body.timeType;

    if (request->request.opcode == PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY) {
        if (timeType == TimeType::TimeOfDay) {
            cb_set_timeofday_offset(offset);
        } else if (timeType == TimeType::Uptime) {
            cb_set_uptime_offset((uint64_t)offset);
        }
        server->core->trigger_tick();
    }

    if (!response(nullptr, 0, nullptr, 0, nullptr, 0,
                  PROTOCOL_BINARY_RAW_BYTES,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  0, cookie)) {
        return ENGINE_DISCONNECT;
    }

    return ENGINE_SUCCESS;
}

static void setup(void (*add)(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor,
                              uint8_t cmd,
                              BINARY_COMMAND_CALLBACK new_handler)) {
    add(&descriptor, PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY, handle_adjust_time);
}

MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE memcached_extensions_initialize(const char *config,
                                                     GET_SERVER_API get_server_api) {
    server = get_server_api();
    descriptor.get_name = get_name;
    descriptor.setup = setup;

    if (server == nullptr) {
        return EXTENSION_FATAL;
    }

    if (config != nullptr && strlen(config) > 0) {
        return EXTENSION_FATAL;
    }

    if (!server->extension->register_extension(EXTENSION_BINARY_PROTOCOL,
                                               &descriptor)) {
        return EXTENSION_FATAL;
    }

    return EXTENSION_SUCCESS;
}
