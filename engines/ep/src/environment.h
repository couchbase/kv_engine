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
    size_t engineFileDescriptors = 0;

    /**
     * We want to reserve /some/ files for each engine for tasks such as
     * StatSnap and AccessLog. 5 seems a reasonable amount.
     */
    static constexpr size_t reservedFileDescriptors =
            cb::limits::TotalBuckets * 5;
};
