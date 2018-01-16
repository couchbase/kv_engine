/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <string>

#include <memcached/visibility.h>

namespace cb {
namespace tracing {

enum class TraceCode {
    REQUEST, /* Whole Request */

    ALLOCATE,
    BGFETCH,
    COMPRESS,
    FLUSH,
    GAT,
    GET,
    GETIF,
    GETLOCKED,
    GETMETA,
    GETSTATS,
    ITEMDELETE,
    LOCK,
    OBSERVE,
    REMOVE,
    SETITEMINFO,
    SETWITHMETA,
    STORE,
    STOREIF,
    UNLOCK,
};
} // namespace tracing
} // namespace cb

MEMCACHED_PUBLIC_API std::string to_string(
        const cb::tracing::TraceCode tracecode);
