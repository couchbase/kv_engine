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

/**
 * Sub-document API validator functions.
 */

#pragma once

#include <cstddef>

#include <memcached/protocol_binary.h>

/* Maximum sub-document path length */
const size_t SUBDOC_PATH_MAX_LENGTH = 1024;

/* Subdocument validator functions. Returns 0 if valid, else -1. */
protocol_binary_response_status subdoc_get_validator(void* packet);
protocol_binary_response_status subdoc_exists_validator(void* packet);
protocol_binary_response_status subdoc_dict_add_validator(void* packet);
protocol_binary_response_status subdoc_dict_upsert_validator(void* packet);
protocol_binary_response_status subdoc_delete_validator(void* packet);
protocol_binary_response_status subdoc_replace_validator(void* packet);
protocol_binary_response_status subdoc_array_push_last_validator(void* packet);
protocol_binary_response_status subdoc_array_push_first_validator(void* packet);
protocol_binary_response_status subdoc_array_insert_validator(void* packet);
protocol_binary_response_status subdoc_array_add_unique_validator(void* packet);
protocol_binary_response_status subdoc_counter_validator(void* packet);
protocol_binary_response_status subdoc_multi_lookup_validator(void* packet);
