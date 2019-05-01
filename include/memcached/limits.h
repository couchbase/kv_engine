/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include <platform/cb_arena_malloc_client.h>

#include <cstddef>

namespace cb {
namespace limits {

/// The total number of buckets which may be created on the server, limited by
/// how many concurrent arenas are available
constexpr std::size_t TotalBuckets = cb::ArenaMallocMaxClients;

/// The total number of bytes which may be used from privileged (system)
/// users in a document
constexpr std::size_t PrivilegedBytes = 1024 * 1024;

/// The maximum length of a DCP connection
constexpr std::size_t MaxDcpName = 200;

} // namespace limits
} // namespace cb
