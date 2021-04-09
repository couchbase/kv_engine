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
