/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/*
 *                "ewouldblock_engine"
 *
 * The "ewouldblock_engine" allows one to test how memcached responds when the
 * engine returns EWOULDBLOCK instead of the correct response.
 */
#pragma once

#include "config.h"

#include <memcached/protocol_binary.h>

/** Binary protocol command used to control this engine. */
const uint8_t PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL = 0xeb;

extern "C" {
    /**
     * Definition of the packet used to control this engine:
     */
    typedef union {
        struct {
            protocol_binary_request_header header;
            struct {
                uint32_t mode; // See EWB_Engine_Mode
                uint32_t value;
                uint32_t inject_error; // ENGINE_ERROR_CODE to inject.
            } body;
        } message;
        uint8_t bytes[sizeof(protocol_binary_request_header) +
                      sizeof(uint32_t) +
                      sizeof(uint32_t) +
                      sizeof(uint32_t)];
    } request_ewouldblock_ctl;

    /**
     * Definition of the packet returned by ewouldblock_ctl requests.
     */
    typedef protocol_binary_response_no_extras response_ewouldblock_ctl;
}


// The mode the engine is currently operating in. Determines when it will
// inject EWOULDBLOCK instead of the real return code.
enum EWBEngine_Mode {
    EWBEngineMode_NEXT_N, // Make the next_N calls into engine return
                          // EWOULDBLOCK. N specified by the {value} field.
  
    EWBEngineMode_RANDOM, // Randomly return EWOULDBLOCK. Chance to return
                          // EWOULDBLOCK is specified as an integer
                          // percentage (1,100) in the {value} field.
  
    EWBEngineMode_FIRST,  // The first call to a given function from each
                          // connection will return EWOULDBLOCK, with the
                          // next (and subsequent) calls to he *same*
                          // function operating normally. Calling a
                          // different function will reset back to
                          // failing again.  In other words, return
                          // EWOULDBLOCK iif the previous function was not
                          // this one.
    EWBEngineMode_SEQUENCE, // Make the next N calls return a sequence of either
                          // their normal value or the injected error code.
                          // The sequence can be up to 32 elements long.
    EWBEngineMode_CAS_MISMATCH // Simulate CAS mismatch - make the next N
                          // store operations return KEY_EEXISTS. N specified
                          // by the {value} field.
};
