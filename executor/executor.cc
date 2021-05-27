/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "executor.h"

static std::unique_ptr<folly::CPUThreadPoolExecutor> pool;

namespace cb::executor {

void create(size_t numThreads) {
    pool = std::make_unique<folly::CPUThreadPoolExecutor>(
            numThreads, std::make_shared<folly::NamedThreadFactory>("mc:e:"));
}

void shutdown() {
    pool->stop();
    pool->join();
    pool.reset();
}

folly::CPUThreadPoolExecutor& get() {
    if (!pool) {
        throw std::logic_error("cb::executor::get(): pool not created");
    }
    return *pool;
}

} // namespace cb::executor
