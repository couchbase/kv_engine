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

#include <platform/platform.h>
#include "buckets.h"

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * Create a new instance of the given bucket type
     *
     * @param type the type of engine to create an instance of
     * @param get_server_api A function to get the server API (passed to the
     *                       underlying engine)
     * @param handle where to store the newly created engine
     * @return ENGINE_SUCCESS on success
     */
    bool new_engine_instance(BucketType type,
                             GET_SERVER_API get_server_api,
                             ENGINE_HANDLE **handle);

    /**
     * Try to convert from a module name to a bucket type
     *
     * @param module the name of the shared object to look up (e.g. ep.so)
     * @param return the constant representing the bucket or UNKNOWN for
     *               unknown shared objects
     */
    BucketType module_to_bucket_type(const char *module);

    /**
     * Initialize the engine map with the different types of supported
     * engine backends. The method will terminate the server upon errors
     *
     * This method is not MT safe
     *
     * @param msg where to store the error message
     * @return true on success, false on error (msg will give more information)
     */
    bool initialize_engine_map(char **msg, EXTENSION_LOGGER_DESCRIPTOR *log);

    /**
     * Release all allocated resources used by the engine map.
     *
     * This method is not MT safe
     */
    void shutdown_engine_map(void);

#ifdef __cplusplus
}
#endif
