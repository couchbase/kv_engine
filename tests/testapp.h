/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#pragma once

#include "config.h"

#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>

#include <memcached/protocol_binary.h>

#if defined(__cplusplus)
extern "C" {
#endif

enum test_return { TEST_SKIP, TEST_PASS, TEST_FAIL };

/* Compress the given document. Returns the size of the compressed document,
 * and deflated is updated to point to the compressed buffer.
 * 'deflated' should be freed by the caller when no longer needed.
 */
size_t compress_document(const char* data, size_t datalen, char** deflated);

/* Set the datatype feature on the connection to the specified value */
void set_datatype_feature(bool enable);

/* Attempts to get the given key and checks if it's value matches {value} */
enum test_return validate_object(const char *key, const char *value);

/* Attempts to store an object with the given key and value */
enum test_return store_object(const char *key, const char *value);

/* Attempts to delete the object with the given key */
enum test_return delete_object(const char *key);

/* Attempts to store an object with a datatype */
enum test_return store_object_w_datatype(const char *key,
                                         const void *data, size_t datalen,
                                         bool deflate, bool json);

/* Populate buf with a binary command with the given parameters. */
off_t raw_command(char* buf, size_t bufsz, uint8_t cmd, const void* key,
                  size_t keylen, const void* dta, size_t dtalen);

/* Send the specified buffer+len to memcached. */
void safe_send(const void* buf, size_t len, bool hickup);

/* Attempts to receive size bytes into buf. Returns true if successful.
 */
bool safe_recv_packet(void *buf, size_t size);

/* Validate the specified response header against the expected cmd and status.
 */
void validate_response_header(protocol_binary_response_no_extras *response,
                              uint8_t cmd, uint16_t status);

#if defined(__cplusplus)
} // extern "C"
#endif
