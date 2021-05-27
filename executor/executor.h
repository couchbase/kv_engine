/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <memory>

// Use a namespace for now to make our life easier when we want to migrate
// over to the executor currently living in ep-engine
namespace cb::executor {
/// Create a new pool with up to numThreads threads
void create(size_t numThreads);
/// Shut down the pool
void shutdown();
/// Get the current executor pool
folly::CPUThreadPoolExecutor& get();
} // namespace cb::executor
