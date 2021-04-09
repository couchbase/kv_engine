/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015-Present Couchbase, Inc.
*
*   Use of this software is governed by the Business Source License included
*   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
*   in that file, in accordance with the Business Source License, use of this
*   software will be governed by the Apache License, Version 2.0, included in
*   the file licenses/APL2.txt.
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
