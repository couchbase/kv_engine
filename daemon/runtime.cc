/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015 Couchbase, Inc
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
#include "runtime.h"
#include <atomic>
#include <string>

static const bool unit_tests{getenv("MEMCACHED_UNIT_TESTS") != nullptr};

static std::atomic_bool default_bucket_enabled;
bool is_default_bucket_enabled() {
    if (unit_tests) {
        if (getenv("MEMCACHED_UNIT_TESTS_NO_DEFAULT_BUCKET")) {
            return false;
        }
    }
    return default_bucket_enabled.load(std::memory_order_relaxed);
}

void set_default_bucket_enabled(bool enabled) {
    default_bucket_enabled.store(enabled, std::memory_order_relaxed);
}
