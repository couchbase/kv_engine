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

/* The "nobucket" is a bucket that just returnes "ENGINE_NO_BUCKET". This
 * bucket may be set as the "default" bucket for connections to avoid
 * having to check if a bucket is selected or not.
 */
#ifndef MEMCACHED_NOBUCKET_H
#define MEMCACHED_NOBUCKET_H

#include "config.h"
#include <memcached/visibility.h>
#include <memcached/engine.h>

#ifdef __cplusplus
extern "C" {
#endif

    MEMCACHED_PUBLIC_API
    ENGINE_ERROR_CODE create_no_bucket_instance(uint64_t interface,
                                                GET_SERVER_API get_server_api,
                                                ENGINE_HANDLE **handle);


    MEMCACHED_PUBLIC_API
    void destroy_engine(void);

#ifdef __cplusplus
}
#endif

#endif
