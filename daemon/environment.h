/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

#include <atomic>
#include <cstddef>

namespace cb {

/**
 * The Environment class is intended to be a singleton that holds static,
 * process-wide, environment information relevant to the memcached process. This
 * information includes things such as file descriptor limits.
 */
struct Environment {
    /// The maximum number of file descriptors we may have. During startup we
    /// try to increase the allowed number of file handles to the limit
    /// specified for the current user.
    std::atomic<size_t> max_file_descriptors{0};

    /// The maximum number of files descriptors that the engines can
    /// (collectively) use.
    std::atomic<size_t> engine_file_descriptors{0};

    /// File descriptors reserved for the engine.
    /// This is the minimum number of file descriptors that we will give to the
    /// engines. In reality we probably want to give them more, but we need them
    /// to at least /work/ if the user has a restricted number of file
    /// descriptors. This is the number of vBuckets of an EPBucket (1024)
    /// rounded up to the nearest whole(ish) number for extra files the engine
    /// may need to open.
    const size_t min_engine_file_descriptors{1050};

    /// File descriptors reserved for memcached.
    /// Used for things such as log files, reading config, RBAC, and SSL certs.
    /// @TODO is this a reasonable number?
    const size_t memcached_reserved_file_descriptors{1000};
};
} // namespace cb

extern cb::Environment environment;
