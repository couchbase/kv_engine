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
 * @param name the name of the bucket to create
 * @param get_server_api A function to get the server API (passed to the
 *                       underlying engine)
 * @return the allocated handle if successful
 */
EngineIface* new_engine_instance(BucketType type,
                                 const std::string& name,
                                 GET_SERVER_API get_server_api);

/**
 * Try to convert from a module name to a bucket type
 *
 * @param module the name of the shared object to look up (e.g. ep.so)
 * @param return the constant representing the bucket or UNKNOWN for
 *               unknown shared objects
 */
BucketType module_to_bucket_type(const char* module);

/**
 * Initialize the engine map with the different types of supported
 * engine backends.
 *
 * This method is not MT safe
 *
 * @throws std::bad_alloc on memory failures
 *         std::runtime_error if an error occurs while initializing
 *                            an engine
 */
void initialize_engine_map();

/**
 * Release all allocated resources used by the engine map.
 *
 * This method is not MT safe
 */
void shutdown_engine_map();
