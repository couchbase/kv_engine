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

#include <memcached/limits.h>

#include <cstddef>

struct Environment {
public:
    /**
     * Magic static getter for singleton Env.
     */
    static Environment& get();

    size_t getMaxBackendFileDescriptors() const;

    /**
     * The maximum number of file descriptors that the engines can
     * (collectively) use
     */
    std::atomic<size_t> engineFileDescriptors = 0;

    /**
     * We want to reserve /some/ files for each engine for tasks such as
     * StatSnap and AccessLog. 5 seems a reasonable amount.
     */
    static constexpr size_t reservedFileDescriptors =
            cb::limits::TotalBuckets * 5;
};
