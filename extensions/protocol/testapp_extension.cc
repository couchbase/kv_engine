/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
    Memcached protocol extensions for testapp.

    Provides extended protocol commands to enable interesting testcases.

    1. Currently supports shifting the Memcached's timeofday.
      * protocol command can be overridden in config.
*/

#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <inttypes.h>

#include "extensions/protocol_extension.h"
#include <memcached/util.h>
#include <platform/platform.h>

static uint8_t adjust_timeofday_command = PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY;

GET_SERVER_API server_api;

static const char *get_name(void);
static void setup(void (*add)(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor,
                              uint8_t cmd,
                              BINARY_COMMAND_CALLBACK new_handler));

static ENGINE_ERROR_CODE handle_adjust_time(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor,
                                            ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            protocol_binary_request_header *request,
                                            ADD_RESPONSE response);

static EXTENSION_BINARY_PROTOCOL_DESCRIPTOR descriptor;

static const char *get_name(void) {
    return "testapp protocol extension";
}

static void setup(void (*add)(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor,
                              uint8_t cmd,
                              BINARY_COMMAND_CALLBACK new_handler))
{
    add(&descriptor, adjust_timeofday_command, handle_adjust_time);
}


static ENGINE_ERROR_CODE handle_adjust_time(EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *descriptor,
                                            ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            protocol_binary_request_header *request,
                                            ADD_RESPONSE response)
{
    protocol_binary_adjust_time *req = (protocol_binary_adjust_time*)request;
    ENGINE_ERROR_CODE r = ENGINE_SUCCESS;
    uint64_t offset = 0;

    offset = ntohll(req->message.body.offset);

    if (r == ENGINE_SUCCESS) {

        if (r == ENGINE_SUCCESS) {
            if (request->request.opcode == adjust_timeofday_command) {
                cb_set_timeofday_offset(offset);

                if (!response(NULL, 0, NULL, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                0, cookie)) {
                    return ENGINE_DISCONNECT;
                }
            } else {
                if (!response(NULL, 0, NULL, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                0, cookie)) {
                    return ENGINE_DISCONNECT;
                }
            }

        }
    }
    return r;
}

MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE memcached_extensions_initialize(const char *config,
                                                     GET_SERVER_API get_server_api) {
    SERVER_HANDLE_V1 *server = get_server_api();
    server_api = get_server_api;
    descriptor.get_name = get_name;
    descriptor.setup = setup;

    if (server == NULL) {
        return EXTENSION_FATAL;
    }

    if (config != NULL) {
        size_t time_op = 0;
        struct config_item items[2];
        memset(&items, 0, sizeof(items));

        items[0].key = "t";
        items[0].datatype = DT_SIZE;
        items[0].value.dt_size = &time_op;

        if (server->core->parse_config(config, items, stderr) != 0) {
            return EXTENSION_FATAL;
        }

        if (items[0].found) {
            adjust_timeofday_command = (uint8_t)(time_op & 0xff);
        }
    }

    if (!server->extension->register_extension(EXTENSION_BINARY_PROTOCOL,
                                               &descriptor)) {
        return EXTENSION_FATAL;
    }

    return EXTENSION_SUCCESS;
}
