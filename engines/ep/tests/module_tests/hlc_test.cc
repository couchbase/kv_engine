/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "hlc.h"

#include "checkpoint_manager.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "kv_bucket.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/thread_gate.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>
#include <utilities/test_manifest.h>

#include <thread>

struct MockClock {
    static std::chrono::milliseconds currentTime;

    static std::chrono::time_point<std::chrono::system_clock> now() {
        return std::chrono::time_point<std::chrono::system_clock>{
                std::chrono::system_clock::duration{currentTime}};
    }
};

std::chrono::milliseconds MockClock::currentTime{1};

template <class Clock>
class MockHLC : public HLCT<Clock> {
public:
    MockHLC(uint64_t initHLC,
            int64_t epochSeqno,
            std::chrono::microseconds aheadThreshold,
            std::chrono::microseconds behindThreshold,
            std::chrono::microseconds maxHlcFutureThreshold)
        : HLCT<Clock>(initHLC,
                      epochSeqno,
                      aheadThreshold,
                      behindThreshold,
                      maxHlcFutureThreshold) {
    }

    void setNonLogicalClockGetNextCasHook(std::function<void()> hook) {
        HLCT<Clock>::nonLogicalClockGetNextCasHook = hook;
    }
    void setLogicalClockGetNextCasHook(std::function<void()> hook) {
        HLCT<Clock>::logicalClockGetNextCasHook = hook;
    }
    uint64_t getLogicalClockTicks() const {
        return HLCT<Clock>::logicalClockTicks;
    }
};

class HLCTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Currently don't care what threshold we have for ours tests,
        std::chrono::microseconds threshold;
        testHLC = std::make_unique<MockHLC<MockClock>>(
                0, 0, threshold, threshold, threshold);
    }

    std::unique_ptr<MockHLC<MockClock>> testHLC;
};

TEST_F(HLCTest, RaceLogicalClockIncrementAtSameTime) {
    testHLC->forceMaxHLC(std::numeric_limits<uint32_t>::max());

    ThreadGate tg1(2);

    testHLC->setLogicalClockGetNextCasHook([&tg1]() { tg1.threadUp(); });

    uint64_t threadCas;
    auto thread = std::thread(
            [this, &threadCas]() { threadCas = testHLC->nextHLC(); });

    auto thisCas = testHLC->nextHLC();

    thread.join();

    EXPECT_NE(threadCas, thisCas);
}

TEST_F(HLCTest, RaceSameClockTime) {
    uint64_t threadCas;
    auto thread = std::thread(
            [this, &threadCas]() { threadCas = testHLC->nextHLC(); });

    auto thisCas = testHLC->nextHLC();

    thread.join();

    EXPECT_NE(threadCas, thisCas);
    EXPECT_EQ(1, testHLC->getLogicalClockTicks());
}

class HLCBucketTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }

    // Use setWithMeta to simulate the fast-forwarding of maxCas
    uint64_t poisonMaxCas(bool flush = true) {
        auto item = make_item(vbid, makeStoredDocKey("posion"), "value");
        auto cas = std::numeric_limits<int64_t>::max() & ~0xffffull;
        item.setCas(cas);
        uint64_t seqno;
        EXPECT_EQ(cb::engine_errc::success,
                  store->setWithMeta(std::ref(item),
                                     0,
                                     &seqno,
                                     cookie,
                                     {vbucket_state_active},
                                     CheckConflicts::No,
                                     true,
                                     GenerateBySeqno::Yes,
                                     GenerateCas::No));
        if (flush) {
            flushVBucketToDiskIfPersistent(vbid, 1);
        }
        return cas;
    }
};

// This test validates that disk "poisoning" is real.
TEST_P(HLCBucketTest, poisonDisk) {
    auto maxCas = poisonMaxCas();
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(maxCas, vb->getMaxCas());
    vb.reset();

    if (!persistent()) {
        // Ephemeral cannot warm-up
        return;
    }

    resetEngineAndWarmup();
    vb = store->getVBucket(vbid);
    // Note that in warmup a set-vb-state occurs to flush the failover table.
    // This triggers a read of the HLC, so maxCas is now + 1
    EXPECT_EQ(maxCas + 1, vb->getMaxCas());
}

TEST_P(HLCBucketTest, fixPoisonedDisk) {
    auto poisonedMaxCas = poisonMaxCas();
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(poisonedMaxCas, vb->getMaxCas());

    // What value to use in the fix? Anything equal or below the current real
    // time will cause HLC to return to real-time
    store->forceMaxCas(vbid, 1);

    // At this stage memory is fixed
    auto fixedMaxCas = vb->getMaxCas();
    EXPECT_LT(fixedMaxCas, poisonedMaxCas);

    if (!persistent()) {
        return;
    }

    // And the flusher runs and fixes disk
    flushVBucketToDiskIfPersistent(vbid, 0);

    // Which can be verified with a warmup.
    vb.reset();
    resetEngineAndWarmup();

    vb = store->getVBucket(vbid);
    // Note that in warmup a set-vb-state occurs to flush the failover table.
    // This triggers a read of the HLC, so maxCas is now larger than when fixed
    // but less than when poisoned
    auto newMaxCas = vb->getMaxCas();
    EXPECT_GT(newMaxCas, fixedMaxCas);
    EXPECT_LT(newMaxCas, poisonedMaxCas);
}

// This test "races" the forceMaxCas with vbucket mutations. In an early version
// of the fix for MB-56181 the fact that the flush-batch reorders caused trouble
// when considering the maxCas to flush. E.g. sometimes a "poisoned" item was
// processed after the set-vb-state, even though by-seqno they are ordered the
// other way - this undid the fix when considering seqno ordering. Now seqno
// ordering is respected
TEST_P(HLCBucketTest, fixPoisonedDiskWithData) {
    auto poisonedMaxCas = poisonMaxCas();
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(poisonedMaxCas, vb->getMaxCas());

    // Add a collection - as this is useful because any items in the vegetable
    // collection get ordered to be processed after set-vb-state, proving that
    // the forceMaxCas works even if a poisoned item was processed last in the
    // flusher
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // These 3 items when flushed are processed as key, set-vb-state, carrot
    // because they are in collections 0, 1 and 10
    store_item(vbid,
               makeStoredDocKey("carrot", CollectionEntry::vegetable),
               "value");
    store->forceMaxCas(vbid, 1);
    // At this stage memory is fixed
    auto fixedMaxCas = vb->getMaxCas();
    EXPECT_LT(fixedMaxCas, poisonedMaxCas);

    auto item = store_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_LT(item.getCas(), poisonedMaxCas);
    EXPECT_GT(item.getCas(), fixedMaxCas);

    if (!persistent()) {
        return;
    }

    // Now disk is fixed when the flusher writes the 2 items (and set-vb-state)
    flushVBucketToDiskIfPersistent(vbid, 2);
    vb.reset();
    resetEngineAndWarmup();

    vb = store->getVBucket(vbid);
    // Note that in warmup a set-vb-state occurs to flush the failover table.
    // This triggers a read of the HLC, so maxCas is now larger than when fixed
    // but less than when poisoned
    auto newMaxCas = vb->getMaxCas();
    EXPECT_GT(newMaxCas, fixedMaxCas);
    EXPECT_LT(newMaxCas, poisonedMaxCas);
}

// This test covers the fact that even if there is an attempt to fix the maxCas
// a subsequent setWithMeta can undo the fix. From a field perspective there is
// an element of whack-a-mole if fixing poisoned max_cas in a live system.
TEST_P(HLCBucketTest, undoFixPoisoned) {
    auto poisonedMaxCas = poisonMaxCas();
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(poisonedMaxCas, vb->getMaxCas());

    // Add a collection - as this is useful because any items in the vegetable
    // collection get ordered to be processed after set-vb-state, proving that
    // the forceMaxCas works even if a poisoned item was processed last in the
    // flusher
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // These 3 items when flushed are processed as key, set-vb-state, carrot
    // because they are in collections 0, 1 and 10
    store_item(vbid,
               makeStoredDocKey("carrot", CollectionEntry::vegetable),
               "value");
    store->forceMaxCas(vbid, 1);
    // At this stage memory is fixed
    auto fixedMaxCas = vb->getMaxCas();
    EXPECT_LT(fixedMaxCas, poisonedMaxCas);

    // But if a bad set-with-meta occurs, the fix is undone in memory and disk
    // KV can't really say it is "bad", it's just a new max so it's accepted to
    // keep the HLC ordering
    poisonedMaxCas = poisonMaxCas(false /* no flush */);
    EXPECT_EQ(poisonedMaxCas, vb->getMaxCas());

    if (!persistent()) {
        return;
    }

    // Now disk is "broke" when the flusher writes the items (and set-vb-state)
    flushVBucketToDiskIfPersistent(vbid, 2);
    vb.reset();
    resetEngineAndWarmup();

    vb = store->getVBucket(vbid);
    EXPECT_EQ(poisonedMaxCas + 1, vb->getMaxCas());
}

TEST_P(HLCBucketTest, fixPoisonedReplica) {
    auto poisonedMaxCas = poisonMaxCas();
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(poisonedMaxCas, vb->getMaxCas());

    // Add a collection - as this is useful because any items in the vegetable
    // collection get ordered to be processed after set-vb-state, proving that
    // the forceMaxCas works even if a poisoned item was processed last in the
    // flusher
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable));
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid,
               makeStoredDocKey("carrot", CollectionEntry::vegetable),
               "value");

    setVBucketState(vbid, vbucket_state_replica);

    store->forceMaxCas(vbid, 1);
    // At this stage memory is fixed
    auto fixedMaxCas = vb->getMaxCas();
    EXPECT_LT(fixedMaxCas, poisonedMaxCas);

    if (!persistent()) {
        return;
    }

    // Now disk is fixed when the flusher writes the 2 items (and set-vb-state)
    flushVBucketToDiskIfPersistent(vbid, 1);
    vb.reset();
    resetEngineAndWarmup();

    vb = store->getVBucket(vbid);
    // Note that in warmup a set-vb-state occurs to flush the failover table.
    // This triggers a read of the HLC, so maxCas is now larger than when fixed
    // but less than when poisoned
    auto newMaxCas = vb->getMaxCas();
    EXPECT_GT(newMaxCas, fixedMaxCas);
    EXPECT_LT(newMaxCas, poisonedMaxCas);
}

// Test that a setVbState triggered for other reasons and the system remains
// sane. Note in this test we do begin with a CAS poison just so later checks
// all use logical-clock ticks (which are predictable/testable)
TEST_P(HLCBucketTest, setVbState) {
    auto poisonedMaxCas = poisonMaxCas();
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(poisonedMaxCas, vb->getMaxCas());

    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable));
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(poisonedMaxCas + 1, vb->getMaxCas());
    EXPECT_EQ(poisonedMaxCas + 2,
              store_item(vbid,
                         makeStoredDocKey("carrot", CollectionEntry::vegetable),
                         "value")
                      .getCas());

    EXPECT_EQ(poisonedMaxCas + 2, vb->getMaxCas());

    // An everyday set-vb-state trigger is a topology change
    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // setVbucketState read the HLC, 1 more tick
    EXPECT_EQ(poisonedMaxCas + 3, vb->getMaxCas());

    EXPECT_EQ(poisonedMaxCas + 4,
              store_item(vbid, makeStoredDocKey("key"), "value").getCas());

    if (!persistent()) {
        return;
    }

    // Now flush and warmup - maxCas should remain "monotonic"
    flushVBucketToDiskIfPersistent(vbid, 2);
    vb.reset();
    resetEngineAndWarmup();

    vb = store->getVBucket(vbid);
    // Recall how warmup reads the HLC, so post warmup and the maxCas has ticked
    // once compared to the +4 value we had before warmup
    EXPECT_EQ(poisonedMaxCas + 5, vb->getMaxCas());
}

class HLCInvalidStraegyTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        config_string += "hlc_invalid_strategy=error;";
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }
};

// This test validates the behaviour of a setWithMeta commands with invalid CAS
// values when hlc_invalid_strategy is set to error and replace
TEST_P(HLCInvalidStraegyTest, setWithMetaHLCInvalidStrategyTest) {
    auto key = makeStoredDocKey("setWithMeta");
    auto item = make_item(vbid, key, "value");
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    item.setCas(poisonedCas);

    uint64_t seqno;
    // setWithMeta will fail due to an invalid CAS value
    EXPECT_EQ(cb::engine_errc::cas_value_invalid,
              store->setWithMeta(std::ref(item),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::No,
                                 true,
                                 GenerateBySeqno::Yes,
                                 GenerateCas::No));

    // set hlc_invalid_strategy to replace
    config_string += ";hlc_invalid_strategy=replace";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // setWithMeta succeeds by generating a new valid CAS
    EXPECT_EQ(cb::engine_errc::success,
              store->setWithMeta(std::ref(item),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::No,
                                 true,
                                 GenerateBySeqno::Yes,
                                 GenerateCas::No));

    auto rv = engine->get(*cookie, key, vbid, DocStateFilter::Alive);
    EXPECT_EQ(cb::engine_errc::success, rv.first);
    EXPECT_LT(rv.second->getCas(), poisonedCas);
}

// This test validates the behaviour of a deleteWithMeta commands with an
// invalid CAS value when hlc_invalid_strategy is set to error and replace
TEST_P(HLCInvalidStraegyTest, deleteWithMetaHLCInvalidStrategyTest) {
    // store item to delete
    auto key = makeStoredDocKey("delete");
    store_item(vbid, key, "value");

    ItemMetaData meta;
    uint64_t cas = 0;
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;
    meta.cas = poisonedCas;
    // deleteWithMeta fails with an invalid CAS value
    EXPECT_EQ(cb::engine_errc::cas_value_invalid,
              store->deleteWithMeta(key,
                                    cas,
                                    nullptr,
                                    vbid,
                                    cookie,
                                    {vbucket_state_active},
                                    CheckConflicts::No,
                                    meta,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::No,
                                    0,
                                    nullptr /* extended metadata */,
                                    DeleteSource::Explicit,
                                    EnforceMemCheck::Yes));

    // set hlc_invalid_strategy to replace
    config_string += ";hlc_invalid_strategy=replace";
    reinitialise(config_string);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // deleteWithMeta succeeds by generating a new valid CAS
    EXPECT_EQ(cb::engine_errc::success,
              store->deleteWithMeta(key,
                                    cas,
                                    nullptr,
                                    vbid,
                                    cookie,
                                    {vbucket_state_active},
                                    CheckConflicts::No,
                                    meta,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::No,
                                    0,
                                    nullptr /* extended metadata */,
                                    DeleteSource::Explicit,
                                    EnforceMemCheck::Yes));
}

INSTANTIATE_TEST_SUITE_P(HLCBucketTests,
                         HLCBucketTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(HLCInvalidStraegyTest,
                         HLCInvalidStraegyTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
