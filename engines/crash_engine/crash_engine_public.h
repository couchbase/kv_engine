/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
 * Publicly visible symbols for crash_engine.so
 */
#pragma once
#include <memcached/engine.h>
#include <memcached/visibility.h>

extern "C" {
MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_crash_engine_instance(GET_SERVER_API gsa,
                                               EngineIface** handle);

MEMCACHED_PUBLIC_API
void destroy_crash_engine(void);
} // extern "C"