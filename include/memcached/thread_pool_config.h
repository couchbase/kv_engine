/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#pragma once

#include <iosfwd>

/**
 * Class to represent the number of reader and writer threads
 */
struct ThreadPoolConfig {
    /// Number of threads to be created for a given thread pool type.
    enum class ThreadCount : int {
        /// Number of threads optimized for disk IO latency - auto-selected
        /// based on available CPU core count.
        DiskIOOptimized = -1,
        /// pre MH compatible value.
        Default = 0,
        // Any other positive integer value is an explicit number of threads
        // to create.
    };
    friend std::ostream& operator<<(std::ostream& os, const ThreadCount& tc);

    ThreadPoolConfig() = default;
    ThreadPoolConfig(int nr, int nw);

    ThreadCount num_readers{ThreadCount::Default};
    ThreadCount num_writers{ThreadCount::Default};
};
