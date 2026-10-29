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

#include "bucket_logger_test.h"
#include "lock_timer.h"
#include <folly/Synchronized.h>
#include <folly/portability/GTest.h>
#include <source_location>
#include <thread>

TEST(LockTimerTest, LockHolder) {
    std::mutex m;
    {
        LockTimer<std::lock_guard<std::mutex>, 1, 1> lh(m, "LockHolder");
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

TEST(LockTimerTest, SynchronizedWLock) {
    folly::Synchronized<int> sync{0};
    {
        const LockTimer<folly::Synchronized<int>::WLockedPtr> lh(
                &sync, "SynchronizedWLock");
        *lh = 1;
        EXPECT_FALSE(sync.tryRLock());
    }
    EXPECT_EQ(1, *sync.rlock());
}

TEST(LockTimerTest, SynchronizedRLock) {
    folly::Synchronized<int> sync{1};
    {
        const LockTimer<folly::Synchronized<int>::ConstRLockedPtr> lh(
                &sync, "SynchronizedRLock");
        EXPECT_EQ(1, *lh);
        EXPECT_TRUE(sync.tryRLock());
        EXPECT_FALSE(sync.tryWLock());
    }
    EXPECT_TRUE(sync.tryWLock());
}

class LockTimerLogTest : public BucketLoggerTest {};

TEST_F(LockTimerLogTest, SlowHoldLogged) {
    folly::Synchronized<int> sync{0};
    {
        LockTimer<folly::Synchronized<int>::WLockedPtr, 0, 0> lh(
                &sync, "SlowHoldLogged");
        std::this_thread::sleep_for(std::chrono::milliseconds(7));
    }
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should have a single logfile only";
    EXPECT_EQ(1,
              countInFile(files.front(),
                          "LockHolder<SlowHoldLogged> Held lock for too long"));
#if !(defined(__apple_build_version__) && __clang_major__ <= 15)
    EXPECT_EQ(1,
              countInFile(files.front(),
                          std::source_location::current().function_name()));
#endif
}

TEST_F(LockTimerLogTest, UnlockLogsOnce) {
    folly::Synchronized<int> sync{0};
    {
        LockTimer<folly::Synchronized<int>::WLockedPtr, 0, 0> lh(
                &sync, "UnlockLogsOnce");
        std::this_thread::sleep_for(std::chrono::milliseconds(7));
        lh.unlock();
        EXPECT_TRUE(sync.tryWLock());
        // second unlock should do nothing
        lh.unlock();
    }
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should have a single logfile only";
    EXPECT_EQ(1,
              countInFile(files.front(),
                          "LockHolder<UnlockLogsOnce> Held lock for too long"));
}
