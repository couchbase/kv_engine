/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executorpool.h"
#include "cb3_executorpool.h"
#include "fake_executorpool.h"
#include "folly_executorpool.h"
#include "mock_executor_pool.h"

#include <folly/lang/Assume.h>
#include <vector>

static constexpr size_t EP_MIN_NONIO_THREADS = 2;

static constexpr size_t EP_MAX_NONIO_THREADS = 8;

void ExecutorPool::create(Backend backend,
                          size_t maxThreads,
                          ThreadPoolConfig::ThreadCount maxReaders,
                          ThreadPoolConfig::ThreadCount maxWriters,
                          ThreadPoolConfig::AuxIoThreadCount maxAuxIO,
                          ThreadPoolConfig::NonIoThreadCount maxNonIO,
                          ThreadPoolConfig::IOThreadsPerCore ioThreadsPerCore) {
    if (getInstance()) {
        throw std::logic_error("ExecutorPool::create() Pool already created");
    }

    switch (backend) {
    case Backend::Folly:
        getInstance() = std::make_unique<FollyExecutorPool>(maxThreads,
                                                            maxReaders,
                                                            maxWriters,
                                                            maxAuxIO,
                                                            maxNonIO,
                                                            ioThreadsPerCore);
        return;
    case Backend::CB3:
        getInstance() = std::make_unique<CB3ExecutorPool>(maxThreads,
                                                          maxReaders,
                                                          maxWriters,
                                                          maxAuxIO,
                                                          maxNonIO,
                                                          ioThreadsPerCore);
        return;
    case Backend::Fake:
        getInstance() = std::make_unique<SingleThreadedExecutorPool>();
        return;
    case Backend::Mock:
        getInstance() = std::make_unique<MockExecutorPool>();
        return;
    }

    throw std::runtime_error("ExecutorPool::create(): Unknown backend");
}

bool ExecutorPool::exists() {
    return getInstance().get() != nullptr;
}

ExecutorPool* ExecutorPool::get() {
    auto* tmp = getInstance().get();
    if (tmp == nullptr) {
        throw std::logic_error("ExecutorPool::get(): Pool not created");
    }
    return tmp;
}

void ExecutorPool::shutdown() {
    cb::NoArenaGuard guard;
    getInstance().reset();
}

Taskable& ExecutorPool::getDefaultTaskable() const {
    if (!defaultTaskable) {
        throw std::runtime_error("Default taskable not set!");
    }
    return *defaultTaskable;
}

void ExecutorPool::setDefaultTaskable(Taskable& taskable) {
    if (defaultTaskable) {
        throw std::runtime_error("Default taskable cannot be reset!");
    }
    defaultTaskable = &taskable;
}

size_t ExecutorPool::getNumIOThreadsPerCore() const {
    return std::underlying_type_t<ThreadPoolConfig::IOThreadsPerCore>(
            ioThreadsPerCore.load());
}

void ExecutorPool::setNumIOThreadPerCore(
        ThreadPoolConfig::IOThreadsPerCore val) {
    ioThreadsPerCore = val;
}

ExecutorPool::ExecutorPool(size_t maxThreads,
                           ThreadPoolConfig::IOThreadsPerCore ioThreadsPerCore)
    : maxGlobalThreads(maxThreads ? maxThreads
                                  : Couchbase::get_available_cpu_count()),
      ioThreadsPerCore(ioThreadsPerCore) {
}

size_t ExecutorPool::calcNumReaders(
        ThreadPoolConfig::ThreadCount threadCount) const {
    switch (threadCount) {
    case ThreadPoolConfig::ThreadCount::Balanced: {
        // Default: configure Reader threads based on CPU count; constraining
        // to between 4 and 16 threads (relatively conservative number).
        auto readers = maxGlobalThreads;
        readers = std::clamp(readers, size_t{4}, size_t{16});
        return readers;
    }

    case ThreadPoolConfig::ThreadCount::DiskIOOptimized: {
        // Configure Reader threads based on CPU count; increased up
        // to a maximum of 128 threads.

        // Note: For maximum IO throughput we should create as many Reader
        // threads as concurrent iops the system can support, given we use
        // synchronous (blocking) IO and hence could utilise more threads than
        // CPU cores. Threads are configured based on CPU count multiplied by
        // ioThreadsPerCore. However, given we don't have test environments
        // larger than ~128 cores, limit to 128.
        auto readers =
                maxGlobalThreads *
                std::underlying_type_t<ThreadPoolConfig::IOThreadsPerCore>(
                        ioThreadsPerCore.load());
        readers = std::clamp(readers, size_t{4}, size_t{128});
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
    case ThreadPoolConfig::ThreadCount::Balanced:
        // Default: configure Writer threads to 4 (default from v3.0 onwards).
        return 4;

    case ThreadPoolConfig::ThreadCount::DiskIOOptimized: {
        // Configure Writer threads based on CPU count; up to a maximum of 128
        // threads.

        // Note: For maximum IO throughput we should create as many Writer
        // threads as concurrent iops the system can support, given we use
        // synchronous (blocking) IO and hence could utilise more threads than
        // CPU cores. Threads are configured based on CPU count multiplied by
        // ioThreadsPerCore. However, given we don't have test environments
        // larger than ~128 cores, limit to 128.
        auto writers =
                maxGlobalThreads *
                std::underlying_type_t<ThreadPoolConfig::IOThreadsPerCore>(
                        ioThreadsPerCore.load());
        writers = std::clamp(writers, size_t{4}, size_t{128});
        return writers;
    }

    default:
        // User specified an explicit value - use that unmodified.
        return static_cast<size_t>(threadCount);
    }
}

size_t ExecutorPool::calcNumAuxIO(
        ThreadPoolConfig::AuxIoThreadCount threadCount) const {
    switch (threadCount) {
    case ThreadPoolConfig::AuxIoThreadCount::Default: {
        // Default: configure threads based on CPU count multiplied by
        // ioThreadsPerCore; constraining to between 2 and 128 threads
        // (similar to Reader/Writer thread counts above).
        auto auxIO = maxGlobalThreads *
                     std::underlying_type_t<ThreadPoolConfig::IOThreadsPerCore>(
                             ioThreadsPerCore.load());
        auxIO = std::clamp(auxIO, size_t{2}, size_t{128});
        return auxIO;
    }
    default:
        // User specified an explicit value - use that unmodified.
        return static_cast<size_t>(threadCount);
    }
}

size_t ExecutorPool::calcNumNonIO(
        ThreadPoolConfig::NonIoThreadCount threadCount) const {
    switch (threadCount) {
    case ThreadPoolConfig::NonIoThreadCount::Default: {
        // 1. compute: 30% of total threads
        size_t count = maxGlobalThreads * 0.3;

        // 2. adjust computed value to be within range
        return std::clamp(count, EP_MIN_NONIO_THREADS, EP_MAX_NONIO_THREADS);
    }
    default:
        // Pick user's value if specified
        return static_cast<size_t>(threadCount);
    }
}

int ExecutorPool::getThreadPriority(TaskType taskType) {
    // Decrease the priority of Writer and AuxIO threads to lessen their impact on
    // other threads (esp front-end workers which should be prioritized ahead
    // of non-critical path Writer tasks (both Flusher and Compaction) or AuxIO
    // tasks (such as Backfilling) which are not latency sensitive.
    // TODO: Investigate if it is worth increasing the priority of Flusher tasks
    // which _are_ on the critical path for front-end operations - i.e.
    // SyncWrites at level=persistMajority / persistActive.
    // This could be done by having two different priority thread pools (say
    // Low and High IO threads and putting critical path Reader (BGFetch) and
    // Writer (SyncWrite flushes) on the High IO thread pool; keeping
    // non-persist SyncWrites / normal mutations & compaction on the Low IO
    // pool.
#if defined(__linux__)
    // Only doing this for Linux at present:
    // - On Windows folly's getpriority() compatability function changes the
    //   priority of the entire process.
    // - On macOS setpriority(PRIO_PROCESS) affects the entire process (unlike
    //   Linux where it's only the current thread), hence calling setpriority()
    //   would be pointless.
    switch (taskType) {
    case TaskType::Writer:
    case TaskType::AuxIO:
        // Linux uses the range -20..19 (highest..lowest).
        return 19;
    case TaskType::Reader:
    case TaskType::NonIO:
        return 0;
    case TaskType::None:
    case TaskType::Count:
        // These are both invalid taskTypes.
        folly::assume_unreachable();
    }
#endif
    return 0;
}

std::unique_ptr<ExecutorPool>& ExecutorPool::getInstance() {
    static std::unique_ptr<ExecutorPool> instance;
    return instance;
}
