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
#include "connection.h"

int mcbp_add_header(Connection* c,
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
void mcbp_write_response(Connection* c,
                         const void* d,
                         int extlen,
                         int keylen,
                         int dlen);

void mcbp_write_packet(Connection* c, protocol_binary_response_status err);

/**
 * Convert an error code generated from the storage engine to the corresponding
 * error code used by the protocol layer.
 * @param e the error code as used in the engine
 * @return the error code as used by the protocol layer
 */
protocol_binary_response_status engine_error_2_mcbp_protocol_error(
    ENGINE_ERROR_CODE e);
