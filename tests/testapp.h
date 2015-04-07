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

#include <stdlib.h>

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

/* Attempts to store an object with a datatype */
enum test_return store_object_w_datatype(const char *key,
                                         const void *data, size_t datalen,
                                         bool deflate, bool json);

#if defined(__cplusplus)
} // extern "C"
#endif
