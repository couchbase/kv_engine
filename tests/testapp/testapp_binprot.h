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

#include <cstdlib>
#include <sys/types.h>

#include <memcached/protocol_binary.h>

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

/* Validate the specified response header against the expected cmd and status.
 */
void mcbp_validate_response_header(protocol_binary_response_no_extras* response,
                                   uint8_t cmd, uint16_t status);

void mcbp_validate_arithmetic(const protocol_binary_response_incr* incr,
                              uint64_t expected);