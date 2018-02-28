/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include <memcached/mcd_util-visibility.h>

MCD_UTIL_PUBLIC_API
const char* memcached_opcode_2_text(uint8_t opcode);

MCD_UTIL_PUBLIC_API
uint8_t memcached_text_2_opcode(const char* txt);

// Maps a status to a string that is a description and contains whitespace
MCD_UTIL_PUBLIC_API
const char* memcached_status_2_text(protocol_binary_response_status status);
