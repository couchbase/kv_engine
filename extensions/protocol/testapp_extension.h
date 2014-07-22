/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
    Memcached protocol extensions for testapp.

    Provides extended protocol commands to enable interesting testcases.

    1. Currently supports shifting Memcached's timeofday.
*/

#ifndef TESTAPP_EXTENSION_H
#define TESTAPP_EXTENSION_H

#include <memcached/protocol_binary.h>

/** The default command id for the adjust timeofday operation (override with t=) */
#define PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY (uint8_t)(0xE3 & 0xff)

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * Definition of the packet used by adjust timeofday
     */
    typedef union {
        struct {
            protocol_binary_request_header header;
            struct {
                uint64_t offset;
            } body;
        } message;
        uint8_t bytes[sizeof(protocol_binary_request_header) + 8];
    } protocol_binary_adjust_time;

    /**
     * Definition of the packet returned by adjust_timeofday
     */
    typedef protocol_binary_response_no_extras protocol_binary_adjust_time_response;

#ifdef __cplusplus
}
#endif

#endif
