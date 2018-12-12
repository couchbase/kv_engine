/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#pragma once

/*
 * This file contains the interface to the implementation of the memcached
 * binary protocol
 */

#include "connection.h"
#include "protocol/mcbp/engine_errc_2_mcbp.h"
#include <mcbp/protocol/datatype.h>
#include <memcached/engine_common.h>

/**
 * Add a header to the current memcached connection
 *
 * @param cookie the command context to add the header for
 * @param status The error code to use
 * @param extras The data to put in the extras field
 * @param key The data to put in the data field
 * @param body_len The length of the body field (without extras and key)
 * @param datatype The datatype to inject into the header
 * @throws std::bad_alloc
 */
void mcbp_add_header(Cookie& cookie,
                     cb::mcbp::Status status,
                     cb::const_char_buffer extras,
                     cb::const_char_buffer key,
                     uint32_t body_length,
                     uint8_t datatype);

/**
 * Format and put a response into the send buffer
 *
 * @param cookie The command we're sending the response for
 * @param status The status code for the response
 * @param extras The extras section to insert into the response
 * @param key The key to insert into the response
 * @param value The value to insert into the response
 * @param datatype The value to specify as the datatype of the response
 * @param sendbuffer An optional send buffer to chain into the response
 *                   if present (the view of the sendbuffer should match
 *                   value if a sendbuffer is provided)
 */
void mcbp_send_response(Cookie& cookie,
                        cb::mcbp::Status status,
                        cb::const_char_buffer extras,
                        cb::const_char_buffer key,
                        cb::const_char_buffer value,
                        uint8_t datatype,
                        std::unique_ptr<SendBuffer> sendbuffer);

extern AddResponseFn mcbpResponseHandlerFn;
