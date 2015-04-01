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
 * Helper functions for debug output.
 */

#pragma once

#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Insert a char* into a buffer, but replace all non-printable characters with
 * a '.'
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param src the string to add to the buffer
 * @param srcsz the number of bytes in src
 * @return number of bytes in dest if success, -1 otherwise
 */
ssize_t buf_to_printable_buffer(char *dest, size_t destsz,
                                const char *src, size_t srcsz);

/**
 * Insert a key into a buffer, but replace all non-printable characters
 * with a '.'.
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param prefix string to insert before the data
 * @param client the client we are serving
 * @param from_client set to true if this data is from the client
 * @param key the key to add to the buffer
 * @param nkey the number of bytes in the key
 * @return number of bytes in dest if success, -1 otherwise
 */
ssize_t key_to_printable_buffer(char *dest, size_t destsz, SOCKET client,
                                bool from_client, const char *prefix,
                                const char *key, size_t nkey);

#ifdef __cplusplus
} // extern "C"
#endif

