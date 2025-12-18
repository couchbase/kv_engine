/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/Stdlib.h>
#pragma once

inline bool isUnitTestMode() {
    return getenv("MEMCACHED_UNIT_TESTS") != nullptr;
}

inline void setUnitTestMode(bool enabled) {
    if (enabled) {
        setenv("MEMCACHED_UNIT_TESTS", "true", 1);
    } else {
        unsetenv("MEMCACHED_UNIT_TESTS");
    }
}
