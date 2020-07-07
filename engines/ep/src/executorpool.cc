/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "executorpool.h"
#include "cb3_executorpool.h"
#include "configuration.h"
#include "ep_engine.h"
#include "folly_executorpool.h"
#include "objectregistry.h"

std::mutex ExecutorPool::initGuard;
std::atomic<ExecutorPool*> ExecutorPool::instance;

static const size_t EP_MIN_NONIO_THREADS = 2;

static const size_t EP_MAX_AUXIO_THREADS = 8;
static const size_t EP_MAX_NONIO_THREADS = 8;

ExecutorPool *ExecutorPool::get() {
    auto* tmp = instance.load();
    if (tmp == nullptr) {
        LockHolder lh(initGuard);
        tmp = instance.load();
        if (tmp == nullptr) {
            // Double-checked locking if instance is null - ensure two threads
            // don't both create an instance.

            Configuration &config =
                ObjectRegistry::getCurrentEngine()->getConfiguration();
            NonBucketAllocationGuard guard;
            if (config.getExecutorPoolBackend() == "cb3") {
                tmp = new CB3ExecutorPool(config.getMaxThreads(),
                                          ThreadPoolConfig::ThreadCount(
                                                  config.getNumReaderThreads()),
                                          ThreadPoolConfig::ThreadCount(
                                                  config.getNumWriterThreads()),
                                          config.getNumAuxioThreads(),
                                          config.getNumNonioThreads());
            } else if (config.getExecutorPoolBackend() == "folly") {
                tmp = new FollyExecutorPool(
                        config.getMaxThreads(),
                        ThreadPoolConfig::ThreadCount(
                                config.getNumReaderThreads()),
                        ThreadPoolConfig::ThreadCount(
                                config.getNumWriterThreads()),
                        config.getNumAuxioThreads(),
                        config.getNumNonioThreads());
            } else {
                throw std::invalid_argument(
                        "ExecutorPool::get() Invalid executor_pool_backend '" +
                        config.getExecutorPoolBackend() + "'");
            }
            instance.store(tmp);
        }
    }
    return tmp;
}

void ExecutorPool::shutdown() {
    std::lock_guard<std::mutex> lock(initGuard);
    auto* tmp = instance.load();
    if (tmp != nullptr) {
        NonBucketAllocationGuard guard;
        delete tmp;
        instance = nullptr;
    }
}

ExecutorPool::ExecutorPool(size_t maxThreads)
    : maxGlobalThreads(maxThreads ? maxThreads
                                  : Couchbase::get_available_cpu_count()) {
}

size_t ExecutorPool::calcNumReaders(
        ThreadPoolConfig::ThreadCount threadCount) const {
    switch (threadCount) {
    case ThreadPoolConfig::ThreadCount::Default: {
        // Default: configure Reader threads based on CPU count; constraining
        // to between 4 and 16 threads (relatively conservative number).
        auto readers = maxGlobalThreads;
        readers = std::min(readers, size_t{16});
        readers = std::max(readers, size_t{4});
        return readers;
    }

    case ThreadPoolConfig::ThreadCount::DiskIOOptimized: {
        // Configure Reader threads based on CPU count; increased up
        // to a maximum of 64 threads.

        // Note: For maximum IO throughput we should create as many Reader
        // threads as concurrent iops the system can support, given we use
        // synchronous (blocking) IO and hence could utilise more threads than
        // CPU cores. However, knowing the number of concurrent IOPs the system
        // can support is hard, so we use #CPUs as a proxy for it - machines
        // with lots of CPU cores are more likely to have more IO than little
        // machines.
        // However given we don't have test environments larger than
        // 64 cores, limit to 64.
        auto readers = maxGlobalThreads;
        readers = std::min(readers, size_t{64});
        readers = std::max(readers, size_t{4});
        return readers;
    }

    default:
        // User specified an explicit value - use that unmodified.
        return static_cast<size_t>(threadCount);
    }
}
size_t ExecutorPool::calcNumWriters(
        ThreadPoolConfig::ThreadCount threadCount) const {
    switch (threadCount) {
    case ThreadPoolConfig::ThreadCount::Default:
        // Default: configure Writer threads to 4 (default from v3.0 onwards).
        return 4;

    case ThreadPoolConfig::ThreadCount::DiskIOOptimized: {
        // Configure Writer threads based on CPU count; up to a maximum of 64
        // threads.

        // Note: For maximum IO throughput we should create as many Writer
        // threads as concurrent iops the system can support, given we use
        // synchronous (blocking) IO and hence could utilise more threads than
        // CPU cores. However, knowing the number of concurrent IOPs the system
        // can support is hard, so we use #CPUs as a proxy for it - machines
        // with lots of CPU cores are more likely to have more IO than little
        // machines. However given we don't have test environments larger than
        // 64 cores, limit to 64.
        auto writers = maxGlobalThreads;
        writers = std::min(writers, size_t{64});
        writers = std::max(writers, size_t{4});
        return writers;
    }

    default:
        // User specified an explicit value - use that unmodified.
        return static_cast<size_t>(threadCount);
    }
}

size_t ExecutorPool::calcNumAuxIO(size_t threadCount) const {
    // 1. compute: ceil of 10% of total threads
    size_t count = maxGlobalThreads / 10;
    if (!count || maxGlobalThreads % 10) {
        count++;
    }
    // 2. adjust computed value to be within range
    if (count > EP_MAX_AUXIO_THREADS) {
        count = EP_MAX_AUXIO_THREADS;
    }
    // 3. Override with user's value if specified
    if (threadCount) {
        count = threadCount;
    }
    return count;
}

size_t ExecutorPool::calcNumNonIO(size_t threadCount) const {
    // 1. compute: 30% of total threads
    size_t count = maxGlobalThreads * 0.3;

    // 2. adjust computed value to be within range
    count = std::min(EP_MAX_NONIO_THREADS,
                     std::max(EP_MIN_NONIO_THREADS, count));

    // 3. pick user's value if specified
    if (threadCount) {
        count = threadCount;
    }
    return count;
}
