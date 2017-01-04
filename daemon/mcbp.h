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


#include <memcached/protocol_binary.h>
#include "connection_mcbp.h"
#include "protocol/mcbp/engine_errc_2_mcbp.h"

/**
 * Add a header to the current memcached connection
 *
 * @param c the connection to add the header for
 * @param err The error code to use
 * @param ext_len The length of the ext field
 * @param key_len The length of the key field
 * @param body_len THe length of the body field
 * @param datatype The datatype to inject into the header
 * @throws std::bad_alloc
 */
void mcbp_add_header(McbpConnection* c,
                     uint16_t err,
                     uint8_t ext_len,
                     uint16_t key_len,
                     uint32_t body_len,
                     uint8_t datatype);

/* Form and send a response to a command over the binary protocol.
 * NOTE: Data from `d` is *not* immediately copied out (it's address is just
 *       added to an iovec), and thus must be live until transmit() is later
 *       called - (aka don't use stack for `d`).
 */
void mcbp_write_response(McbpConnection* c,
                         const void* d,
                         int extlen,
                         int keylen,
                         int dlen);

void mcbp_write_packet(McbpConnection* c, protocol_binary_response_status err);

bool mcbp_response_handler(const void* key, uint16_t keylen,
                           const void* ext, uint8_t extlen,
                           const void* body, uint32_t bodylen,
                           protocol_binary_datatype_t datatype, uint16_t status,
                           uint64_t cas, const void* cookie);


/* set up a connection to write a DynamicBuffer then free it once sent. */
void mcbp_write_and_free(McbpConnection* c, DynamicBuffer* buf);
