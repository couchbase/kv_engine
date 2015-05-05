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
 * Sub-document API support.
 */

#pragma once

#include "config.h"

#include "memcached.h"

#if defined(__cplusplus)
extern "C" {
#endif

/* Maximum sub-document path length */
const size_t SUBDOC_PATH_MAX_LENGTH = 1024;

/* Subdocument validator functions. Returns 0 if valid, else -1. */
int subdoc_get_validator(void* packet);
int subdoc_exists_validator(void* packet);
int subdoc_dict_add_validator(void* packet);
int subdoc_dict_upsert_validator(void* packet);

/* Subdocument executor functions. */
void subdoc_get_executor(conn *c, void *packet);
void subdoc_exists_executor(conn *c, void *packet);
void subdoc_dict_add_executor(conn *c, void *packet);
void subdoc_dict_upsert_executor(conn *c, void *packet);

#if defined(__cplusplus)
} // extern "C"
#endif
