/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "common.h"
#include "lock_timer.h"
#include <folly/portability/GTest.h>
#include <thread>

TEST(LockTimerTest, LockHolder) {
    std::mutex m;
    {
        LockTimer<std::lock_guard<std::mutex>, 1, 1> lh(m, "LockHolder");
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}
