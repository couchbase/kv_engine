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
#include "hash_table.h"
#include "item.h"
#include "kv_bucket.h"
#include "objectregistry.h"
#include "stored_value_factories.h"
#include "test_helpers.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "vbucket.h"

#include <executor/fake_executorpool.h>
#include <folly/Portability.h>
#include <programs/engine_testapp/mock_server.h>
#include <spdlog/async.h>

#include <optional>

class ObjectRegistryTest : virtual public ::testing::Test {
protected:
    void SetUp() override {
        // build() returns with this thread associated with the engine. The
        // strings below are allocated outside the engine and must be
        // released outside the engine.
        std::optional<NonBucketAllocationGuard> releaseOutsideEngine;
        const auto dbname = getProcessUniqueDatabaseName();
        removePathIfExists(dbname);

        {
            NonBucketAllocationGuard guard;
            ExecutorPool::create(ExecutorPool::Backend::Fake);
        }

        const auto extraConfig = "dbname=" + dbname;
        engine = SynchronousEPEngine::build(extraConfig);
        releaseOutsideEngine.emplace();
    }

    void TearDown() override {
        engine.reset();
        ExecutorPool::shutdown();
        removePathIfExists(getProcessUniqueDatabaseName());
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
 * BucketAllocationGuard must restore the exact ArenaMalloc client (and
 * domain) which was active on construction, even if it differs from what
 * the ObjectRegistry's current engine implies (e.g. when running under a
 * cb::NoArenaGuard). Previously it restored the previous engine's arena,
 * causing memory allocated under cb::NoArenaGuard to be freed against the
 * bucket (as seen in FollyExecutorPool::doTasksStat()).
 */
TEST_F(ObjectRegistryTest, BucketAllocationGuardRestoresArenaClient) {
    const auto engineIndex = engine->getArenaMallocClient().index;
    ASSERT_EQ(engine.get(), ObjectRegistry::getCurrentEngine());
    ASSERT_EQ(engineIndex, cb::ArenaMalloc::getCurrentClientIndex());

    {
        cb::NoArenaGuard noArena;
        ASSERT_EQ(cb::NoClientIndex, cb::ArenaMalloc::getCurrentClientIndex());
        {
            BucketAllocationGuard guard(engine.get());
            EXPECT_EQ(engineIndex, cb::ArenaMalloc::getCurrentClientIndex());
        }
        EXPECT_EQ(cb::NoClientIndex, cb::ArenaMalloc::getCurrentClientIndex());
        EXPECT_EQ(engine.get(), ObjectRegistry::getCurrentEngine());
    }
    EXPECT_EQ(engineIndex, cb::ArenaMalloc::getCurrentClientIndex());

    {
        cb::ArenaDomainGuard<cb::MemoryDomain::Secondary> secondary;
        {
            BucketAllocationGuard guard(engine.get());
        }
        EXPECT_EQ(cb::MemoryDomain::Secondary,
                  cb::ArenaMalloc::getCurrentClient().domain);
    }
}

/**
 * NonBucketAllocationGuard must restore the exact ArenaMalloc client (and
 * domain) which was active on construction, even if it differs from what
 * the ObjectRegistry's current engine implies (e.g. when running under a
 * cb::NoArenaGuard).
 */
TEST_F(ObjectRegistryTest, NonBucketAllocationGuardRestoresArenaClient) {
    const auto engineIndex = engine->getArenaMallocClient().index;
    ASSERT_EQ(engine.get(), ObjectRegistry::getCurrentEngine());
    ASSERT_EQ(engineIndex, cb::ArenaMalloc::getCurrentClientIndex());

    {
        cb::NoArenaGuard noArena;
        ASSERT_EQ(cb::NoClientIndex, cb::ArenaMalloc::getCurrentClientIndex());
        {
            NonBucketAllocationGuard guard;
            EXPECT_EQ(nullptr, ObjectRegistry::getCurrentEngine());
            EXPECT_EQ(cb::NoClientIndex,
                      cb::ArenaMalloc::getCurrentClientIndex());
        }
        EXPECT_EQ(cb::NoClientIndex, cb::ArenaMalloc::getCurrentClientIndex());
        EXPECT_EQ(engine.get(), ObjectRegistry::getCurrentEngine());
    }
    EXPECT_EQ(engineIndex, cb::ArenaMalloc::getCurrentClientIndex());

    {
        cb::ArenaDomainGuard<cb::MemoryDomain::Secondary> secondary;
        {
            NonBucketAllocationGuard guard;
            EXPECT_EQ(cb::NoClientIndex,
                      cb::ArenaMalloc::getCurrentClientIndex());
        }
        EXPECT_EQ(engineIndex, cb::ArenaMalloc::getCurrentClientIndex());
        EXPECT_EQ(cb::MemoryDomain::Secondary,
                  cb::ArenaMalloc::getCurrentClient().domain);
    }
}

TEST_F(ObjectRegistryTest, StoredValStats) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }

    auto baselineSize = engine->getEpStats().getStoredValSize();
    auto baselineOverhead = engine->getEpStats().getStoredValOverhead();
    auto baselineNum = engine->getEpStats().getNumStoredVal();

    {
        auto sv = StoredValueFactory()(
                make_item(Vbid(0), makeStoredDocKey("key"), ""), nullptr);
        sv->resetValue();
        EXPECT_EQ(baselineNum + 1, engine->getEpStats().getNumStoredVal());
        EXPECT_LT(baselineSize + sv->getObjectSize(),
                  engine->getEpStats().getStoredValSize());
        EXPECT_LT(baselineOverhead,
                  engine->getEpStats().getStoredValOverhead());
    }
    EXPECT_EQ(baselineSize, engine->getEpStats().getStoredValSize());
    EXPECT_EQ(baselineNum, engine->getEpStats().getNumStoredVal());
    EXPECT_EQ(baselineOverhead, engine->getEpStats().getStoredValOverhead());
}

TEST_F(ObjectRegistryTest, BlobStats) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }

    auto baselineSize = engine->getEpStats().getTotalValueSize();
    auto baselineOverhead = engine->getEpStats().getBlobOverhead();
    auto baselineNum = engine->getEpStats().getNumBlob();

    {
        std::unique_ptr<Blob> blob(Blob::New(1));
        EXPECT_EQ(baselineNum + 1, engine->getEpStats().getNumBlob());
        EXPECT_LT(baselineSize + blob->getSize(),
                  engine->getEpStats().getTotalValueSize());
        EXPECT_LT(baselineOverhead, engine->getEpStats().getBlobOverhead());
    }
    EXPECT_EQ(baselineSize, engine->getEpStats().getTotalValueSize());
    EXPECT_EQ(baselineNum, engine->getEpStats().getNumBlob());
    EXPECT_EQ(baselineOverhead, engine->getEpStats().getBlobOverhead());
}

// Check that EPStats::skipStoredValueAndBlobDeleteAccounting disables the stats
// accounting of StoredValue and Blob deletion (but not Item deletion).
TEST_F(ObjectRegistryTest, SkipStoredValueAndBlobDeleteAccounting) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }

    auto& stats = engine->getEpStats();
    // Create and destroy one object of each type, returning by how much each
    // count changed. Note creating the StoredValue also creates an Item and
    // a Blob (the value) which must be accounted for by the caller.
    struct Deltas {
        int64_t blob;
        int64_t sv;
        int64_t item;
    };
    auto createAndDestroy = [&stats]() {
        const int64_t numBlob = stats.getNumBlob();
        const int64_t numSV = stats.getNumStoredVal();
        const int64_t numItem = stats.getNumItem();
        // Creates and destroys 1 Item, 1 StoredValue and 2 Blobs.
        StoredValueFactory()(make_item(Vbid(0), makeStoredDocKey("key"), ""),
                             nullptr)
                .reset();
        std::unique_ptr<Blob>(Blob::New(1)).reset();
        return Deltas{.blob = int64_t(stats.getNumBlob()) - numBlob,
                      .sv = int64_t(stats.getNumStoredVal()) - numSV,
                      .item = int64_t(stats.getNumItem()) - numItem};
    };

    auto deltas = createAndDestroy();
    EXPECT_EQ(0, deltas.blob);
    EXPECT_EQ(0, deltas.sv);
    EXPECT_EQ(0, deltas.item);

    stats.skipStoredValueAndBlobDeleteAccounting = true;
    deltas = createAndDestroy();
    EXPECT_EQ(2, deltas.blob);
    EXPECT_EQ(1, deltas.sv);
    EXPECT_EQ(0, deltas.item);
}

// Check that when the engine is being destroyed (isShutdown and StoredValue/
// Blob deletion accounting skipped, as destroyInner does), destroying a
// HashTable skips the per-item stats accounting.
TEST_F(ObjectRegistryTest, HashTableDestroySkipsAccounting) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }

    auto& stats = engine->getEpStats();
    const auto baselineNumSV = stats.getNumStoredVal();
    const int numItems = 10;
    auto ht = std::make_unique<HashTable>(
            stats, std::make_unique<StoredValueFactory>(), 5, 1, 0, 10);
    for (int index = 0; index < numItems; ++index) {
        auto item = make_item(
                Vbid(0), makeStoredDocKey(std::to_string(index)), "value");
        ASSERT_EQ(MutationStatus::WasClean, ht->set(item));
    }
    ASSERT_EQ(baselineNumSV + numItems, stats.getNumStoredVal());

    stats.isShutdown = true;
    stats.bucketDestroyOptimisations = true;
    stats.skipStoredValueAndBlobDeleteAccounting = true;
    ht.reset();
    // StoredValue deletion was not accounted
    EXPECT_EQ(baselineNumSV + numItems, stats.getNumStoredVal());
}

// Check that with the bucket destroy optimisations disabled, destroying a
// HashTable at shutdown performs all of the per-item stats accounting.
TEST_F(ObjectRegistryTest, HashTableDestroyWithoutOptimisations) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }

    auto& stats = engine->getEpStats();
    const auto baselineNumSV = stats.getNumStoredVal();
    const int numItems = 10;
    auto ht = std::make_unique<HashTable>(
            stats, std::make_unique<StoredValueFactory>(), 5, 1, 0, 10);
    for (int index = 0; index < numItems; ++index) {
        auto item = make_item(
                Vbid(0), makeStoredDocKey(std::to_string(index)), "value");
        ASSERT_EQ(MutationStatus::WasClean, ht->set(item));
    }
    ASSERT_EQ(baselineNumSV + numItems, stats.getNumStoredVal());

    stats.isShutdown = true;
    stats.bucketDestroyOptimisations = false;
    ht.reset();
    // StoredValue deletion was accounted
    EXPECT_EQ(baselineNumSV, stats.getNumStoredVal());
}

// Check that destroyInner() applies the (dynamic) bucket_destroy_optimisations
// configuration parameter.
TEST_F(ObjectRegistryTest, DestroyInnerHonoursBucketDestroyOptimisations) {
    auto& stats = engine->getEpStats();
    std::string msg;
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam("bucket_destroy_optimisations", "false", msg))
            << msg;
    engine->destroyInner(false);
    EXPECT_TRUE(stats.isShutdown);
    EXPECT_FALSE(stats.bucketDestroyOptimisations);
    EXPECT_FALSE(stats.skipStoredValueAndBlobDeleteAccounting);
}

TEST_F(ObjectRegistryTest, DestroyInnerAppliesBucketDestroyOptimisations) {
    auto& stats = engine->getEpStats();
    ASSERT_TRUE(engine->getConfiguration().isBucketDestroyOptimisations());
    engine->destroyInner(false);
    EXPECT_TRUE(stats.isShutdown);
    EXPECT_TRUE(stats.bucketDestroyOptimisations);
    EXPECT_TRUE(stats.skipStoredValueAndBlobDeleteAccounting);
}

/**
 * Tests which destroy (destroyInner) a bucket with a populated HashTable.
 */
class DestroyPopulatedBucketTest : public ObjectRegistryTest {
protected:
    void SetUp() override {
        ObjectRegistryTest::SetUp();
        baselineNumSV = engine->getEpStats().getNumStoredVal();
        ASSERT_EQ(cb::engine_errc::success,
                  engine->getKVBucket()->setVBucketState(vbid,
                                                         vbucket_state_active));
        vb = engine->getKVBucket()->getVBucket(vbid);
        ASSERT_TRUE(vb);
        for (int index = 0; index < numItems; ++index) {
            auto item = make_item(
                    vbid, makeStoredDocKey(std::to_string(index)), "value");
            ASSERT_EQ(MutationStatus::WasClean, vb->ht.set(item));
        }
    }

    void TearDown() override {
        mock_set_process_shutting_down(false);
        vb.reset();
        ObjectRegistryTest::TearDown();
    }

    const Vbid vbid{0};
    const int numItems{10};
    size_t baselineNumSV{0};
    VBucketPtr vb;
};

// Check that destroying a bucket frees the HashTable via the optimised path,
// which skips the accounting of each StoredValue.
TEST_F(DestroyPopulatedBucketTest, SkipsAccounting) {
    auto& stats = engine->getEpStats();
    if (!stats.isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }
    ASSERT_EQ(baselineNumSV + numItems, stats.getNumStoredVal());

    // The KVBucket must be the only owner so that destroyInner destroys the
    // VBucket and its HashTable.
    vb.reset();
    engine->destroyInner(false);
    EXPECT_EQ(baselineNumSV + numItems, stats.getNumStoredVal());
}

// Check that with bucket_destroy_optimisations disabled, destroying a bucket
// accounts for every StoredValue it frees.
TEST_F(DestroyPopulatedBucketTest, AccountsWithoutOptimisations) {
    auto& stats = engine->getEpStats();
    if (!stats.isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }
    std::string msg;
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam("bucket_destroy_optimisations", "false", msg))
            << msg;
    ASSERT_EQ(baselineNumSV + numItems, stats.getNumStoredVal());

    vb.reset();
    engine->destroyInner(false);
    EXPECT_EQ(baselineNumSV, stats.getNumStoredVal());
}

// Check that destroying a bucket while the process is shutting down releases
// the HashTables. The test keeps the VBucket alive so that the HashTable can
// be inspected after destroyInner() has destroyed the KVBucket. The lookups
// use TrackReference::No, as updating the frequency counter calls back into
// the (destroyed) KVBucket.
TEST_F(DestroyPopulatedBucketTest, ProcessShuttingDownReleasesHashTables) {
    if (folly::kIsSanitize) {
        GTEST_SKIP() << "Sanitizer builds don't release the HashTables";
    }
    const auto key = makeStoredDocKey("0");
    mock_set_process_shutting_down(true);
    engine->destroyInner(false);
    // Released, which deactivates the HashTable
    EXPECT_THROW(vb->ht.findForRead(key, TrackReference::No), std::logic_error);
}

// Check that a bucket deletion (the process keeps running) does not release
// the HashTables.
TEST_F(DestroyPopulatedBucketTest, ProcessRunningKeepsHashTables) {
    const auto key = makeStoredDocKey("0");
    engine->destroyInner(false);
    EXPECT_TRUE(vb->ht.findForRead(key, TrackReference::No).storedValue);
}

// Check that the HashTables are not released at process shutdown when
// bucket_destroy_optimisations is disabled.
TEST_F(DestroyPopulatedBucketTest,
       ProcessShuttingDownWithoutOptimisationsKeepsHashTables) {
    const auto key = makeStoredDocKey("0");
    std::string msg;
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam("bucket_destroy_optimisations", "false", msg))
            << msg;
    mock_set_process_shutting_down(true);
    engine->destroyInner(false);
    EXPECT_TRUE(vb->ht.findForRead(key, TrackReference::No).storedValue);
}

TEST_F(ObjectRegistryTest, CompressionValueStatsTest) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }
    const auto baselineTotalCompressedValueSize =
            engine->getEpStats().getTotalCompressedValueSize();
    const auto baselineTotalDecompressedValueSize =
            engine->getEpStats().getTotalDecompressedValueSize();

    const auto value = std::string(500, 'a');
    auto item = make_item(Vbid(0), makeStoredDocKey("key"), value);
    item.compressValue(true);
    const auto sv = StoredValueFactory()(item, nullptr);

    EXPECT_EQ(baselineTotalCompressedValueSize + sv->valuelen(),
              engine->getEpStats().getTotalCompressedValueSize());
    EXPECT_EQ(baselineTotalDecompressedValueSize + sv->uncompressedValuelen(),
              engine->getEpStats().getTotalDecompressedValueSize());
}

TEST_F(ObjectRegistryTest, UncompressedValueDoesNotUpdateCompressionStats) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }
    const auto baselineTotalCompressedValueSize =
            engine->getEpStats().getTotalCompressedValueSize();
    const auto baselineTotalDecompressedValueSize =
            engine->getEpStats().getTotalDecompressedValueSize();

    const auto value = std::string(500, 'a');
    auto item = make_item(Vbid(0), makeStoredDocKey("key"), value);
    const auto sv = StoredValueFactory()(item, nullptr);

    EXPECT_EQ(baselineTotalCompressedValueSize,
              engine->getEpStats().getTotalCompressedValueSize());
    EXPECT_EQ(baselineTotalDecompressedValueSize,
              engine->getEpStats().getTotalDecompressedValueSize());
}

TEST_F(ObjectRegistryTest, UpdatingStoredValueUpdatesCompressionStats) {
    if (!engine->getEpStats().isMemoryTrackingEnabled()) {
        GTEST_SKIP();
    }
    auto totalCompressedValueSize =
            engine->getEpStats().getTotalCompressedValueSize();
    auto totalDecompressedValueSize =
            engine->getEpStats().getTotalDecompressedValueSize();

    auto value = std::string(500, 'a');
    auto item = make_item(Vbid(0), makeStoredDocKey("key"), value);
    item.compressValue(true);
    auto sv = StoredValueFactory()(item, nullptr);

    totalCompressedValueSize += sv->valuelen();
    totalDecompressedValueSize += sv->uncompressedValuelen();
    EXPECT_EQ(totalCompressedValueSize,
              engine->getEpStats().getTotalCompressedValueSize());
    EXPECT_EQ(totalDecompressedValueSize,
              engine->getEpStats().getTotalDecompressedValueSize());

    // Now update the StoredValue with a new value
    value = std::string(1000, 'b');
    auto item2 = make_item(Vbid(0), makeStoredDocKey("key"), value);
    item2.compressValue(true);
    sv->setValue(item2);

    totalCompressedValueSize += sv->valuelen();
    totalDecompressedValueSize += sv->uncompressedValuelen();
    EXPECT_EQ(totalCompressedValueSize,
              engine->getEpStats().getTotalCompressedValueSize());
    EXPECT_EQ(totalDecompressedValueSize,
              engine->getEpStats().getTotalDecompressedValueSize());
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
    logger->unregister();
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
