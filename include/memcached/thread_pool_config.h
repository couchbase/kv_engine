/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <iosfwd>

/**
 * Class to represent the number of reader and writer threads
 */
struct ThreadPoolConfig {
    /// Number of threads to be created for a given thread pool type
    /// (KV readers/writers).
    enum class ThreadCount : int {
        /// Number of threads optimized for disk IO latency - auto-selected
        /// based on available CPU core count.
        DiskIOOptimized = -1,
        /// pre MH compatible value.
        Default = 0,
        // Any other positive integer value is an explicit number of threads
        // to create.
    };

    /// Number of backend storage threads to be created
    enum class StorageThreadCount : int {
        /// Let the engine pick the default value
        Default = 0,
        // Any other positive integer value is an explicit number of threads
    };

    friend std::ostream& operator<<(std::ostream& os, const ThreadCount& tc);

    ThreadPoolConfig() = default;
    ThreadPoolConfig(int nr, int nw);

    ThreadCount num_readers{ThreadCount::Default};
    ThreadCount num_writers{ThreadCount::Default};
    StorageThreadCount num_storage_threads{StorageThreadCount::Default};
};
