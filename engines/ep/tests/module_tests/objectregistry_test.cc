/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

// We need folly's Windows.h and not spdlog's so include folly's portability
// header before anything that includes spdlog (bucket_logger etc.)
#include <folly/portability/GTest.h>

#include "bucket_logger.h"
#include "bucket_logger_test.h"
#include "fakes/fake_executorpool.h"
#include "item.h"
#include "objectregistry.h"
#include "test_helpers.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

#include <spdlog/async.h>

class ObjectRegistryTest : virtual public ::testing::Test {
protected:
    void SetUp() override {
        const auto dbname = dbnameFromCurrentGTestInfo();
        removePathIfExists(dbname);

        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        const auto extraConfig = "dbname=" + dbname;
        engine = SynchronousEPEngine::build(extraConfig);
    }

    void TearDown() override {
        engine.reset();
        ExecutorPool::shutdown();
        removePathIfExists(dbnameFromCurrentGTestInfo());
    }

    SynchronousEPEngineUniquePtr engine;
};

// Check that constructing & destructing an Item is correctly tracked in
// EpStats::numItem via ObjectRegistry::on{Create,Delete}Item.
TEST_F(ObjectRegistryTest, NumItem) {
    ASSERT_EQ(0, engine->getEpStats().getNumItem());

    {
        auto item = make_item(Vbid(0), makeStoredDocKey("key"), "value");
        EXPECT_EQ(1, engine->getEpStats().getNumItem());
    }
    EXPECT_EQ(0, engine->getEpStats().getNumItem());
}

// Check that constructing & destructing an Item is correctly tracked in
// EpStats::memOverhead via ObjectRegistry::on{Create,Delete}Item.
TEST_F(ObjectRegistryTest, MemOverhead) {
    auto baseline = engine->getEpStats().getMemOverhead();

    {
        auto item = make_item(Vbid(0), makeStoredDocKey("key"), "value");
        // Currently just checking the overhead is non-zero; could expand
        // to calculate expected size based on the Item's size.
        EXPECT_NE(baseline, engine->getEpStats().getMemOverhead());
    }
    EXPECT_EQ(baseline, engine->getEpStats().getMemOverhead());
}

/**
 * Test fixture for ObjectRegistry + BucketLogger tests.
 *
 * Memory tracking with spdlog can be somewhat complex as spdlog can allocate
 * memory in the thread calling spdlog->warn(...), but then releases that memory
 * from a different background thread which actually writes the log message to
 * disk. Therefore We must ensure that these allocations match up.
 */
class ObjectRegistrySpdlogTest : public BucketLoggerTest,
                                 public ObjectRegistryTest {
protected:
    void SetUp() override {
        // Override some logger config params before calling parent class
        // Setup():

        // 1. Write to a different file in case other related class fixtures are
        // running in parallel
        config.filename = "objectregistry_spdlogger_test";

        // 2. Set up logger with the async logger (which uses a seperate thread
        // to pring log messages and hence free temporary buffers), but with
        // only a single buffer so acts more synchronous to make it easier to
        // test & have messages printed sooner after logged.
        config.buffersize = 1;
        config.unit_test = false;

        BucketLoggerTest::SetUp();
        ObjectRegistryTest::SetUp();
        ObjectRegistry::onSwitchThread(engine.get());
    }

    void TearDown() override {
        // Parent classes TearDown methods are sufficient here.
        BucketLoggerTest::TearDown();
        // called last so that the engine is destroyed last
        ObjectRegistryTest::TearDown();
    }
};

// Check that memory allocated by our logger (spdlog) is correctly tracked.
TEST_F(ObjectRegistrySpdlogTest, SpdlogMemoryTrackedCorrectly) {
    ASSERT_TRUE(ObjectRegistry::getCurrentEngine());
    const char* testName =
            ::testing::UnitTest::GetInstance()->current_test_info()->name();

    // const char* - uses the single argument overload of warn().
    auto logger = BucketLogger::createBucketLogger(testName);
    auto baselineMemory = engine->getEpStats().getPreciseTotalMemoryUsed();
    {
        logger->log(spdlog::level::warn, "const char* message");
        logger->flush();
    }
    EXPECT_EQ(baselineMemory, engine->getEpStats().getPreciseTotalMemoryUsed());

    // multiple arguments using format string, with a short (< sizeof(sync_msg)
    // log string.
    // Check that we correctly account even when multiple messages are created
    // & destroyed.
    // "short" - messages less than the aync_msg's buffer are stored as inside
    // async_msg object directly, and don't need additional heap allocation.

    // The actual message capacity is slightly less than sizeof(async_msg.raw) -
    // should be the SIZE template parameter but we don't have access to that
    // so estimate as 50% of the object size.
    spdlog::details::async_msg msg;
    const auto asyncMsgCapacity = sizeof(msg.payload) / 2;
    {
        logger->warn("short+variable ({}) {} ",
                     asyncMsgCapacity,
                     std::string(asyncMsgCapacity, 's'));
        logger->flush();
    }
    EXPECT_EQ(baselineMemory, engine->getEpStats().getPreciseTotalMemoryUsed());

    // As previous, but looping with multiple warn() calls - check that we
    // correctly account even when multiple messages are created & destroyed.
    {
        auto afterLoggerMemory =
                engine->getEpStats().getPreciseTotalMemoryUsed();

        for (int i = 0; i < 100; i++) {
            logger->warn("short+variable loop ({}) {} ",
                         i,
                         std::string(asyncMsgCapacity, 's'));
            logger->flush();
            EXPECT_EQ(afterLoggerMemory,
                      engine->getEpStats().getPreciseTotalMemoryUsed());
        }
    }
    EXPECT_EQ(baselineMemory, engine->getEpStats().getPreciseTotalMemoryUsed());

    // Multiple arguments with a very long string (greater than
    // asyncMsgCapacity)
    // Expect it to heap-allocate mmemory for the message in the calling
    // thread, which will not be freed until the message is flushed by the
    // background thread.
    {
        logger->warn("long+variable ({}) {}",
                     asyncMsgCapacity * 2,
                     std::string(asyncMsgCapacity * 2, 'x'));
        logger->flush();
    }
    EXPECT_EQ(baselineMemory, engine->getEpStats().getPreciseTotalMemoryUsed());

    // Multiple log messages; each with a log string - check that we correctly
    // account even when multiple messages are created & destroyed.
    {
        auto afterLoggerMemory =
                engine->getEpStats().getPreciseTotalMemoryUsed();

        for (int i = 0; i < 100; i++) {
            logger->warn("long+variable loop ({}) {}",
                         i,
                         std::string(asyncMsgCapacity * 2, 'x'));
            logger->flush();
            EXPECT_EQ(afterLoggerMemory,
                      engine->getEpStats().getPreciseTotalMemoryUsed());
        }
    }
    EXPECT_EQ(baselineMemory, engine->getEpStats().getPreciseTotalMemoryUsed());
}
