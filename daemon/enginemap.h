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

#include "buckets.h"
#include <memcached/engine.h>

/**
 * Create a new instance of the given bucket type
 *
 * @param type the type of engine to create an instance of
 * @param get_server_api A function to get the server API (passed to the
 *                       underlying engine)
 * @return the allocated handle if successful
 * @throws std::bad_alloc for memory allocation failures,
 *         cb::engine_error for engine related errors
 */
EngineIface* new_engine_instance(BucketType type,
                                 GET_SERVER_API get_server_api);

/**
 * Convert from a module name to a bucket type
 *
 * @param module The engine's shared object name, e.g. BucketType::Couchstore is
 *               ep.so. The input will be processed by basename, e.g.
 *               /path/to/ep.so would be valid.
 * @return The BucketType for the given module, or BucketType::Unknown for
 *         invalid input.
 */
BucketType module_to_bucket_type(const std::string& module);

/**
 *  Create and initialize an instance of the crash engine which is used for
 *  breakpad testing
 */
void create_crash_instance();

/**
 * Call the engine shutdown function for all valid BucketTypes
 */
void shutdown_all_engines();
