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

// Utility functions used to build up packets in the memcached binary protocol
#include <protocol/connection/client_connection.h>

#include <algorithm>
#include <cstdlib>
#include <sys/types.h>
#include <gtest/gtest.h>

#include <memcached/protocol_binary.h>

off_t mcbp_raw_command(Frame& frame,
                       uint8_t cmd,
                       const void* key,
                       size_t keylen,
                       const void* data,
                       size_t datalen);

/* Populate buf with a binary command with the given parameters. */
off_t mcbp_raw_command(char* buf, size_t bufsz,
                       uint8_t cmd,
                       const void* key, size_t keylen,
                       const void* dta, size_t dtalen);

off_t mcbp_flush_command(char* buf, size_t bufsz, uint8_t cmd, uint32_t exptime,
                         bool use_extra);

off_t mcbp_arithmetic_command(char* buf,
                              size_t bufsz,
                              uint8_t cmd,
                              const void* key,
                              size_t keylen,
                              uint64_t delta,
                              uint64_t initial,
                              uint32_t exp);

/**
 * Constructs a storage command using the give arguments into buf.
 *
 * @param buf the buffer to write the command into
 * @param bufsz the size of the buffer
 * @param cmd the command opcode to use
 * @param key the key to use
 * @param keylen the number of bytes in key
 * @param dta the value for the key
 * @param dtalen the number of bytes in the value
 * @param flags the value to use for the flags
 * @param exp the expiry time
 * @return the number of bytes in the storage command
 */
size_t mcbp_storage_command(char* buf,
                            size_t bufsz,
                            uint8_t cmd,
                            const void* key,
                            size_t keylen,
                            const void* dta,
                            size_t dtalen,
                            uint32_t flags,
                            uint32_t exp);

size_t mcbp_storage_command(Frame &frame,
                            uint8_t cmd,
                            const std::string &id,
                            const std::vector<uint8_t> &value,
                            uint32_t flags,
                            uint32_t exp);



/* Validate the specified response header against the expected cmd and status.
 */
void mcbp_validate_response_header(protocol_binary_response_no_extras* response,
                                   uint8_t cmd, uint16_t status);

::testing::AssertionResult mcbp_validate_response_header(const protocol_binary_response_header* header,
                                                         protocol_binary_command cmd, uint16_t status,
                                                         bool mutation_seqno_enabled);

void mcbp_validate_arithmetic(const protocol_binary_response_incr* incr,
                              uint64_t expected);
