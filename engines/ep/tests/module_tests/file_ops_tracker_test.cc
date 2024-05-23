/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>

#include "ep_time.h"
#include "executor/task_type.h"
#include "file_ops_tracker.h"
#include "tests/module_tests/thread_gate.h"
#include <platform/cb_arena_malloc.h>
#include <platform/cb_arena_malloc_client.h>
#include <chrono>

class FileOpsTrackerTest : public ::testing::Test {
public:
    void SetUp() override {
        testClient = cb::ArenaMalloc::registerClient();
        tracker = std::make_unique<FileOpsTracker>();
    }

    void TearDown() override {
        // It's important that the thread locals in FileOpsTracker never
        // allocate from the bucket's arena.
        EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(testClient));
        cb::ArenaMalloc::unregisterClient(testClient);
    }

    using VisitorData = std::tuple<task_type_t, std::string, FileOp>;
    std::vector<VisitorData> getRequests();

    // Arena client tests can use to verify the FileOpsTracker did not allocate
    // into another arena.
    cb::ArenaMallocClient testClient;
    std::unique_ptr<FileOpsTracker> tracker;
};

std::vector<FileOpsTrackerTest::VisitorData> FileOpsTrackerTest::getRequests() {
    std::vector<FileOpsTrackerTest::VisitorData> v;
    // cb::ArenaMallocGuard guard(testClient);
    tracker->visitThreads([&v](auto type, auto name, auto req) {
        // cb::NoArenaGuard guard2;
        v.emplace_back(std::make_tuple(type, std::string(name), req));
    });
    return v;
}

TEST_F(FileOpsTrackerTest, BasicTest) {
    ASSERT_EQ(0, getRequests().size());

    {
        cb::ArenaMallocGuard guard(testClient);
        tracker->start(FileOp::open());
    }
    auto requests = getRequests();

    ASSERT_EQ(1, requests.size());
    auto [type, _, req] = requests[0];
    EXPECT_EQ(NO_TASK_TYPE, type);
    EXPECT_EQ(FileOp::Type::Open, req.type);
    EXPECT_LE(std::chrono::seconds(0), ep_uptime_now() - req.startTime);
    EXPECT_GT(std::chrono::seconds(5), ep_uptime_now() - req.startTime);

    {
        cb::ArenaMallocGuard guard(testClient);
        tracker->complete();
    }
    EXPECT_EQ(0, getRequests().size());
}

TEST_F(FileOpsTrackerTest, ExitGuard) {
    ASSERT_EQ(0, getRequests().size());

    {
        cb::ArenaMallocGuard guard(testClient);
        auto scopeGuard = tracker->startWithScopeGuard(FileOp::read(1));
        cb::NoArenaGuard guard2;
        EXPECT_EQ(1, getRequests().size());
    }

    EXPECT_EQ(0, getRequests().size());
}

TEST_F(FileOpsTrackerTest, Interleaving) {
    constexpr size_t NumThreads = 3;
    constexpr auto Timeout = std::chrono::seconds(5);
    ASSERT_EQ(0, getRequests().size());

    ThreadGate tg(NumThreads);
    ThreadGate waitForComplete(1);
    std::vector<std::thread> threads;

    for (size_t i = 0; i < NumThreads; i++) {
        threads.emplace_back([&]() {
            auto scopeGuard = tracker->startWithScopeGuard(FileOp::read(1));
            tg.threadUp();
            waitForComplete.waitFor(Timeout);
        });
    }

    tg.waitFor(Timeout);
    const auto requests = getRequests();

    waitForComplete.threadUp();
    for (auto&& t : threads) {
        t.join();
    }
    EXPECT_EQ(0, getRequests().size());

    EXPECT_EQ(3, requests.size());
    for (size_t i = 0; i < NumThreads; i++) {
        EXPECT_EQ(NO_TASK_TYPE, std::get<0>(requests[i]));
        EXPECT_EQ(FileOp::Type::Read, std::get<2>(requests[i]).type);
    }
}

TEST_F(FileOpsTrackerTest, SyncNBytes) {
    // Check that Sync nbytes counts the cumulative non-sync bytes.
    tracker->startWithScopeGuard(FileOp::write(100));
    tracker->startWithScopeGuard(FileOp::write(100));
    tracker->startWithScopeGuard(FileOp::write(100));
    {
        auto g = tracker->startWithScopeGuard(FileOp::sync());
        auto [type, thread, op] = getRequests().at(0);
        EXPECT_EQ(FileOp::Type::Sync, op.type);
        EXPECT_EQ(300, op.nbytes);
    }
    // Check that it gets reset.
    tracker->startWithScopeGuard(FileOp::write(100));
    {
        auto g = tracker->startWithScopeGuard(FileOp::sync());
        auto [type, thread, op] = getRequests().at(0);
        EXPECT_EQ(FileOp::Type::Sync, op.type);
        EXPECT_EQ(100, op.nbytes);
    }
}
