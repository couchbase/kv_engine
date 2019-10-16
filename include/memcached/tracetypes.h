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

enum class TraceCode : uint8_t {
    REQUEST, /* Whole Request */

    /// Time spent waiting for a background fetch operation to be scheduled.
    BG_WAIT,
    /// Time spent performing the actual background load from disk.
    BG_LOAD,
    GET,
    GETIF,
    GETSTATS,
    SETWITHMETA,
    STORE,
    /// Time spent by a SyncWrite in Prepared state before being completed.
    SYNC_WRITE_PREPARE,
    /// Time when a SyncWrite local ACK is received by the Active.
    SYNC_WRITE_ACK_LOCAL,
    /// Time when a SyncWrite replica ACK is received by the Active.
    SYNC_WRITE_ACK_REMOTE,
};

using SpanId = std::size_t;

} // namespace tracing
} // namespace cb

MEMCACHED_PUBLIC_API std::string to_string(cb::tracing::TraceCode tracecode);
