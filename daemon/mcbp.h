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
 * @param value_len The length of the value field (without extras and key)
 * @param datatype The datatype to inject into the header
 * @throws std::bad_alloc
 */
void mcbp_add_header(Cookie& cookie,
                     cb::mcbp::Status status,
                     cb::const_char_buffer extras,
                     cb::const_char_buffer key,
                     std::size_t value_length,
                     uint8_t datatype);

extern AddResponseFn mcbpResponseHandlerFn;
