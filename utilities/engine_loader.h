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

#include <memcached/engine.h>
#include <platform/dynamic.h>

/*
    This type is allocated by load_engine and freed by unload_engine
    and is required to reference the methods exported by the engine.
*/
typedef struct engine_reference engine_reference;

/*
    Unload the engine.
    Triggers destroy_engine then closes the shared object finally freeing the reference.
*/
void unload_engine(engine_reference* engine);

/**
 * Load the specified engine shared object.
 *
 * @param soname The name of the shared object (cannot be NULL)
 * @param create_function The name of the function used to create the engine
 *                        (Set to NULL to use the "default" list of method
 *                        names)
 * @param logger Where to print error messages (cannot be NULL)
 * @return engine_reference* on success or NULL for failure.
 */
engine_reference* load_engine(const char* soname, const char* create_function)
        CB_ATTR_NONNULL(1);

/*
    Create an engine instance.
*/
bool create_engine_instance(engine_reference* engine,
                            SERVER_HANDLE_V1* (*get_server_api)(void),
                            EngineIface** engine_handle);
/*
    Initialise the engine handle using the engine's exported initialize method.
*/
bool init_engine_instance(EngineIface* engine, const char* config_str);
