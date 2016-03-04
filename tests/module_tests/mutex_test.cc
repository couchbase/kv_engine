/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "config.h"

#include <iostream>
#include <thread>

#include "common.h"
#include "locks.h"

#include <gtest/gtest.h>

TEST(LockTimerTest, LockHolder) {
    std::mutex m;
    {
        LockTimer<LockHolder, 1, 1> lh(m, "LockHolder");
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

TEST(LockTimerTest, ReaderLockHolder) {
    RWLock m;
    {
        LockTimer<ReaderLockHolder, 1, 1> rlh(m, "ReaderLockHolder");
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

TEST(LockTimerTest, WriterLockHolder) {
    RWLock m;
    {
        LockTimer<WriterLockHolder, 1, 1> wlh(m, "WriterLockHolder");
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}
