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

/*
 * Benchmarks relating to the VBucket class.
 */

#include "benchmark_memory_tracker.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_types.h"
#include "engine_fixture.h"
#include "item.h"
#include "kv_bucket.h"
#include "stored_value_factories.h"
#include "vbucket.h"

#include "../tests/module_tests/checkpoint_utils.h"
#include "../tests/module_tests/thread_gate.h"

#include <executor/fake_executorpool.h>
#include <folly/portability/GTest.h>
#include <programs/engine_testapp/mock_server.h>
#include <algorithm>
#include <random>
#include <thread>

enum class Store { Couchstore = 0, RocksDB = 1, Magma = 2 };

static std::string to_string(Store store) {
    switch (store) {
    case Store::Couchstore:
        return "couchdb";
    case Store::RocksDB:
        return "rocksdb";
    case Store::Magma:
        return "magma";
    }
    throw std::invalid_argument("to_string(Store): invalid enumeration " +
                                std::to_string(int(store)));
}

enum class FlushMode { Insert = 0, Replace = 1 };

static std::string to_string(FlushMode mode) {
    switch (mode) {
    case FlushMode::Insert:
        return "insert";
    case FlushMode::Replace:
        return "replace";
    }
    throw std::invalid_argument("to_string(FlushMode): invalid enumeration " +
                                std::to_string(int(mode)));
}

class VBucketBench : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        store = Store(state.range(0));
        varConfig = "backend=" + to_string(store) +
                    // A number of benchmarks require more than the default
                    // 100MB bucket quota - bump to ~1GB.
                    ";max_size=1000000000";
        EngineFixture::SetUp(state);
        if (state.thread_index() == 0) {
            engine->getKVBucket()->setVBucketState(Vbid(0),
                                                   vbucket_state_active);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index() == 0) {
            ASSERT_EQ(cb::engine_errc::success,
                      engine->getKVBucket()->deleteVBucket(vbid, nullptr));
            executorPool->runNextTask(
                    AUXIO_TASK_IDX,
                    "Removing (dead) vb:0 from memory and disk");
        }
        EngineFixture::TearDown(state);
    }

    Store store;
};

/**
 * Benchmark fixture for VBucket tests which includes a memoryTracker to
 * allow monitoring of current/peak memory usage.
 */
class MemTrackingVBucketBench : public VBucketBench {
protected:
    void SetUp(const benchmark::State& state) override {
        if (state.thread_index() == 0) {
            memoryTracker = BenchmarkMemoryTracker::getInstance();
            memoryTracker->reset();
        }
        VBucketBench::SetUp(state);
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index() == 0) {
            memoryTracker->destroyInstance();
        }
        VBucketBench::TearDown(state);
    }

    BenchmarkMemoryTracker* memoryTracker = nullptr;
};

/*
 * Fixture for CheckpointManager benchmarks
 */
class CheckpointBench : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        // Allow many checkpoints
        varConfig =
                "max_size=1000000000;max_checkpoints=100000000;checkpoint_max_"
                "size=1";

        EngineFixture::SetUp(state);
        if (state.thread_index() == 0) {
            engine->getKVBucket()->setVBucketState(Vbid(0),
                                                   vbucket_state_active);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index() == 0) {
            engine->getKVBucket()->deleteVBucket(vbid, nullptr);
        }
        EngineFixture::TearDown(state);
    }

    /**
     * Store the given key/value pair.
     *
     * @param key
     * @param value
     */
    void queueItem(const std::string& key, const std::string& value);

    /**
     * Loads the given number of items in CM and moves cursor to the end of the
     * open checkpoint queue.
     *
     * @param numItems
     * @param valueSize
     */
    void loadItemsAndMovePersistenceCursor(size_t numItems, size_t valueSize);

    CheckpointList extractClosedUnrefCheckpoints(CheckpointManager&);

    CheckpointManager::ExtractItemsResult extractItemsToExpel(
            CheckpointManager&);

    std::shared_ptr<CheckpointCursor> getLowestCursor(
            CheckpointManager& manager);
};

/**
 * Benchmark queueing items into a vBucket.
 * Items have a 10% chance of being a duplicate key of a previous item (to
 * model de-dupe).
 */
BENCHMARK_DEFINE_F(MemTrackingVBucketBench, QueueDirty)
(benchmark::State& state) {
    const auto itemCount = state.range(1);

    std::default_random_engine gen;
    auto makeKeyWithDuplicates = [&gen](int i) {
        // 10% of the time; return a key which is the same as a previous one.
        std::uniform_real_distribution<> dis(0, 1.0);
        if (dis(gen) < 0.1) {
            return std::string("key") + std::to_string((i + 1) / 2);
        } else {
            return std::string("key") + std::to_string(i);
        }
    };

    int itemsQueuedTotal = 0;

    // Pre-size the VBucket's hashtable to a sensible size.
    auto* vb = engine->getKVBucket()->getVBucket(vbid).get();
    vb->ht.resize(itemCount);

    // Memory size before queuing.
    const size_t baseBytes = memoryTracker->getCurrentAlloc();

    // Maximum memory during queueing.
    size_t peakBytes = 0;

    const std::string value(1, 'x');
    while (state.KeepRunning()) {
        // Benchmark: Add the given number of items to checkpoint manager.
        // Note we don't include the time taken to make the item.
        for (int i = 0; i < itemCount; ++i) {
            state.PauseTiming();
            const auto key = makeKeyWithDuplicates(i);
            auto item = make_item(vbid, key, value);
            state.ResumeTiming();
            ASSERT_EQ(cb::engine_errc::success,
                      engine->getKVBucket()->set(item, cookie));
            ++itemsQueuedTotal;
        }

        state.PauseTiming();
        peakBytes = std::max(peakBytes, memoryTracker->getMaxAlloc());
        /// Cleanup VBucket
        vb->ht.clear();
        vb->checkpointManager->clear(0);
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemsQueuedTotal);
    // Peak memory usage while queuing, minus baseline.
    state.counters["PeakQueueBytes"] = peakBytes - baseBytes;
    state.counters["PeakBytesPerItem"] = (peakBytes - baseBytes) / itemCount;
}

BENCHMARK_DEFINE_F(MemTrackingVBucketBench, FlushVBucket)
(benchmark::State& state) {
    const auto itemCount = state.range(1);
    int itemsFlushedTotal = 0;
    auto mode = FlushMode(state.range(2));

    // Memory size before flushing.
    size_t baseBytes = 0;

    // Maximum memory during flushing.
    size_t peakBytes = 0;

    // Pre-size the VBucket's hashtable to a sensible size or things are going
    // to get slow for large numbers of items.
    engine->getKVBucket()->getVBucket(vbid)->ht.resize(itemCount);

    std::string value(1, 'x');
    if (mode == FlushMode::Replace) {
        for (int i = 0; i < itemCount; ++i) {
            auto item = make_item(
                    vbid, std::string("key") + std::to_string(i), value);
            ASSERT_EQ(cb::engine_errc::success,
                      engine->getKVBucket()->set(item, cookie));
        }

        // Make sure we have something in the vBucket the first time round
        size_t itemsFlushed = flushAllItems(vbid);
        ASSERT_EQ(itemCount, itemsFlushed);
    }

    while (state.KeepRunning()) {
        // Add the given number of items to checkpoint manager.
        state.PauseTiming();
        if (mode == FlushMode::Insert) {
            // Delete the vBucket so that we can measure the Insert path
            auto result = engine->getKVBucket()->deleteVBucket(vbid, cookie);
            if (result != cb::engine_errc::success) {
                // Deferred deletion is running, wait until complete
                EXPECT_EQ(cb::engine_errc::would_block, result);
                executorPool->runNextTask(
                        AUXIO_TASK_IDX,
                        "Removing (dead) vb:0 from memory and disk");
            }
            engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);

            {
                auto vb = engine->getVBucket(vbid);
                EXPECT_EQ(0, vb->getNumItems());
            }

            // Pre-size the VBucket's hashtable to a sensible size or things are
            // going to get slow for large numbers of items.
            engine->getKVBucket()->getVBucket(vbid)->ht.resize(itemCount);
        }

        for (int i = 0; i < itemCount; ++i) {
            auto item = make_item(
                    vbid, std::string("key") + std::to_string(i), value);
            ASSERT_EQ(cb::engine_errc::success,
                      engine->getKVBucket()->set(item, cookie));
        }

        baseBytes = memoryTracker->getCurrentAlloc();
        state.ResumeTiming();

        // Benchmark.
        size_t itemsFlushed = flushAllItems(vbid);

        ASSERT_EQ(itemCount, itemsFlushed);
        peakBytes = std::max(peakBytes, memoryTracker->getMaxAlloc());
        itemsFlushedTotal += itemsFlushed;
    }
    state.SetItemsProcessed(itemsFlushedTotal);
    state.SetLabel(std::string("store:" + to_string(store) +
                               " mode:" + to_string(mode))
                           .c_str());
    // Peak memory usage while flushing, minus baseline.
    state.counters["PeakFlushBytes"] = peakBytes - baseBytes;
    state.counters["PeakBytesPerItem"] = (peakBytes - baseBytes) / itemCount;
}

BENCHMARK_DEFINE_F(VBucketBench, CreateDeleteStoredValue)
(benchmark::State& state) {
    auto factory = std::make_unique<StoredValueFactory>(engine->getEpStats());

    const std::string value(1, 'x');
    while (state.KeepRunning()) {
        auto item = make_item(vbid, "key", value);
        benchmark::DoNotOptimize(item);
        auto sv = (*factory)(item, nullptr);
        benchmark::DoNotOptimize(sv);
    }
}

CheckpointList CheckpointBench::extractClosedUnrefCheckpoints(
        CheckpointManager& manager) {
    std::lock_guard<std::mutex> lh(manager.queueLock);
    return manager.extractClosedUnrefCheckpoints(lh);
}

CheckpointManager::ExtractItemsResult CheckpointBench::extractItemsToExpel(
        CheckpointManager& manager) {
    std::lock_guard<std::mutex> lh(manager.queueLock);
    return manager.extractItemsToExpel(lh);
}

std::shared_ptr<CheckpointCursor> CheckpointBench::getLowestCursor(
        CheckpointManager& manager) {
    std::lock_guard<std::mutex> lh(manager.queueLock);
    return manager.getLowestCursor(lh);
}

void CheckpointBench::queueItem(const std::string& key,
                                const std::string& value) {
    queued_item item{new Item(StoredDocKey(key, CollectionID::Default),
                              0,
                              0,
                              value.c_str(),
                              value.size(),
                              PROTOCOL_BINARY_RAW_BYTES)};
    item->setVBucketId(vbid);
    item->setQueuedTime();
    auto& manager = *engine->getKVBucket()->getVBucket(vbid)->checkpointManager;
    EXPECT_TRUE(manager.queueDirty(
            item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
}

void CheckpointBench::loadItemsAndMovePersistenceCursor(size_t numItems,
                                                        size_t valueSize) {
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;

    manager.clear(0 /*seqno*/);
    ASSERT_EQ(0, manager.getHighSeqno());
    ASSERT_EQ(1, manager.getNumItems());

    const std::string value(valueSize, 'x');
    for (size_t i = 0; i < numItems; ++i) {
        queueItem("key" + std::to_string(i), value);
    }
    ASSERT_EQ(numItems, manager.getHighSeqno());

    // Make all possible items eligible for removal
    flushAllItems(vbid);
}

/**
 * Removing checkpoints is logically split in two parts:
 *
 * 1. Extracting the checkpoints to remove from the CM list
 * 2. Releasing the checkpoints
 *
 * (1) is what executes under CM lock and must be fast enough for not blocking
 * frontend operations and avoiding frontend throughput degradation.
 *
 * At the time of introducing this bench, (1) is O(N) in the size of the
 * checkpoint list. The bench measures the runtime of (1) at increasing num of
 * checkpoints and shows that the runtime increases linearly.
 * Then under MB-47386 (1) will be made O(1), so the same bench will show
 * constant runtimes for any workload.
 */
BENCHMARK_DEFINE_F(CheckpointBench, ExtractClosedUnrefCheckpoints)
(benchmark::State& state) {
    const size_t numCheckpoints = state.range(0);
    auto& manager = *engine->getKVBucket()->getVBucket(vbid)->checkpointManager;

    ASSERT_EQ(1, manager.getCheckpointConfig().getCheckpointMaxSize());

    while (state.KeepRunning()) {
        state.PauseTiming();

        // Open checkpoint never removed, so create numCheckpoints+1 for
        // removing numCheckpoints
        loadItemsAndMovePersistenceCursor(numCheckpoints + 1, 0);
        ASSERT_EQ(numCheckpoints + 1, manager.getNumCheckpoints());

        // Benchmark
        {
            state.ResumeTiming();
            const auto list = extractClosedUnrefCheckpoints(manager);
            // Don't account checkpoints deallocation, so pause before list goes
            // out of scope
            state.PauseTiming();

            EXPECT_EQ(numCheckpoints, list.size());
        }

        // Need to resume here, gbench will fail when it's time to exit the
        // loop otherwise.
        state.ResumeTiming();
    }
}

/**
 * Getting the list of cursors to drop executes under CM::lock, and at the time
 * of introducing this bench the operation is O(N) in the size of the checkpoint
 * list. The function is being made O(1) under MB-47386.
 */
BENCHMARK_DEFINE_F(CheckpointBench, GetCursorsToDrop)
(benchmark::State& state) {
    const size_t numCheckpoints = state.range(0);
    auto& manager = *engine->getKVBucket()->getVBucket(vbid)->checkpointManager;

    ASSERT_EQ(1, manager.getCheckpointConfig().getCheckpointMaxSize());

    while (state.KeepRunning()) {
        state.PauseTiming();

        loadItemsAndMovePersistenceCursor(numCheckpoints, 0);
        ASSERT_EQ(numCheckpoints, manager.getNumCheckpoints());

        // Benchmark
        {
            state.ResumeTiming();
            const auto cursors = manager.getListOfCursorsToDrop();
            state.PauseTiming();

            EXPECT_EQ(0, cursors.size());
        }

        // Need to resume here, gbench will fail when it's time to exit the
        // loop otherwise.
        state.ResumeTiming();
    }
}

/**
 * Benchmark looking up the lowest cursor, when there are two cursors at the
 * same position. The lowest cursor is needed during item expel (to know
 * where to expel up to).
 * Prior to MB-52131 this was an O(n) operation where N is the distance of the
 * cursor from the beginning of the checkpoint; so for large checkpoints where
 * two cursors were both "up to date" (e.g. replication cursors which are both
 * pointing at high-seqno) then getLowestCursor was costly.
 */
BENCHMARK_DEFINE_F(CheckpointBench, GetLowestCursor)
(benchmark::State& state) {
    // We want a single checkpoint, but with a large number of items in it.
    const size_t _1B = 1000 * 1000 * 1000;
    engine->getConfiguration().setCheckpointMaxSize(_1B);
    const size_t numItems = state.range(0);
    auto& manager = *engine->getKVBucket()->getVBucket(vbid)->checkpointManager;

    // Register a second cursor (modelling replication), and then move both
    // persistence and test cursor to end of Checkpoint.
    auto resisterResult = manager.registerCursorBySeqno(
            "test_cursor", 0, CheckpointCursor::Droppable::Yes);
    loadItemsAndMovePersistenceCursor(numItems, 0);
    std::vector<queued_item> items;
    auto cursor = resisterResult.cursor.lock();
    ASSERT_TRUE(cursor);
    manager.getItemsForCursor(
            *cursor, items, std::numeric_limits<size_t>::max());

    // Benchmark: Request the lowest cursor. Before the fix this was O(numItems)
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(getLowestCursor(manager));
    }
}

BENCHMARK_DEFINE_F(CheckpointBench, ExtractItemsToExpel)
(benchmark::State& state) {
    const auto ckptType = CheckpointType(state.range(0));
    const auto ckptState = checkpoint_state(state.range(1));
    const size_t numItems = state.range(2);

    // Ensure all items in the open checkpoint - avoid checkpoint creation
    auto& config = engine->getConfiguration();
    const size_t _1B = 1000 * 1000 * 1000;
    config.setCheckpointMaxSize(_1B);

    auto& bucket = *engine->getKVBucket();
    auto& manager = *bucket.getVBucket(vbid)->checkpointManager;
    const auto& ckptConfig = manager.getCheckpointConfig();
    ASSERT_EQ(_1B, ckptConfig.getCheckpointMaxSize());

    while (state.KeepRunning()) {
        state.PauseTiming();

        // Checkpoint high-seqno never expelled, so load numItems+1 for
        // expelling numItems
        loadItemsAndMovePersistenceCursor(numItems + 1, 1024);
        ASSERT_EQ(1, manager.getNumCheckpoints());
        ASSERT_EQ(numItems + 1, manager.getNumOpenChkItems());
        // Note: Checkpoint type set after loading items, as the above call
        //  resets the CM before loading, so any previous setup is lost
        CheckpointManagerTestIntrospector::setOpenCheckpointType(manager,
                                                                 ckptType);
        ASSERT_EQ(ckptType, manager.getOpenCheckpointType());

        switch (ckptState) {
        case CHECKPOINT_OPEN: {
            // Nothing else to do
            break;
        }
        case CHECKPOINT_CLOSED: {
            // Expel operates always on the oldest checkpoint and only if it is
            // referenced, so:
            //  - Load items in the current open checkpoint (already done above)
            //  - Load 1 extra item in the same checkpoint to prevent the cursor
            //    leaving the checkpoint (see next step)
            //  - Close the checkpoint. This creates a new open/empty checkpoint
            //    and the cursor stays in the closed one as there is the 1 extra
            //    item for the cursor to process in that closed checkpoint.
            // All the items eligible for expel will be in the closed checkpoint
            queueItem("extra", "");
            manager.createNewCheckpoint();
            ASSERT_EQ(2, manager.getNumCheckpoints());
            ASSERT_EQ(0, manager.getNumOpenChkItems());
            // numItems + 1
            // + extra
            // + 2 meta-items in closed checkpoint
            // + 1 meta-items in open checkpoint
            ASSERT_EQ(numItems + 5, manager.getNumItems());
            break;
        }
        }

        // Benchmark
        {
            state.ResumeTiming();
            auto res = extractItemsToExpel(manager);
            // Don't account deallocation, so pause before res goes out of scope
            state.PauseTiming();

            EXPECT_EQ(numItems, res.getNumItems());
            EXPECT_GT(res.deleteItems(), 0);
        }

        // Need to resume here, gbench will fail when it's time to exit the
        // loop otherwise.
        state.ResumeTiming();
    }

    state.SetLabel(("type:" + to_string(ckptType) + " state:" +
                    to_string(ckptState) + " items:" + std::to_string(numItems))
                           .c_str());
}

// Run with couchstore backend(0); item counts from 1..10,000,000
BENCHMARK_REGISTER_F(MemTrackingVBucketBench, QueueDirty)
        ->Args({0, 1})
        ->Args({0, 100})
        ->Args({0, 10000})
        ->Args({0, 1000000});

static void FlushArguments(benchmark::internal::Benchmark* b) {
    // Add couchstore (0), rocksdb (1), and magma (2) variants for a range of
    // sizes.
    for (auto items = 1; items <= 1000000; items *= 100) {
        // Insert mode
        b->Args({std::underlying_type<Store>::type(Store::Couchstore),
                 items,
                 0});
        // Replace mode
        b->Args({std::underlying_type<Store>::type(Store::Couchstore),
                 items,
                 1});
#ifdef EP_USE_ROCKSDB
        b->Args({std::underlying_type<Store>::type(Store::RocksDB), items, 0});
        b->Args({std::underlying_type<Store>::type(Store::RocksDB), items, 1});
#endif
#ifdef EP_USE_MAGMA
        b->Args({std::underlying_type<Store>::type(Store::Magma), items, 0});
        b->Args({std::underlying_type<Store>::type(Store::Magma), items, 1});
#endif
    }
}

BENCHMARK_REGISTER_F(MemTrackingVBucketBench, FlushVBucket)
        ->Apply(FlushArguments);

// The following benchs aim to show the asymptotic behaviour of the specific
// function under test. In particular, we want to show that functions are
// constant-complexity and don't degrade when the number of checkpoints in CM
// gets high.
// Notes:
// - I set iterations:1 because this bench tend to spend most of the time in the
//   setup phase and runtimes become high with the GBench auto-iterations
// - The GBench auto-iterations is useful to produce high-accuracy results (eg,
//   stddev below a certain threshold), which we don't need here.
// - For producing usable results I'm still using a fixed number (> 1) of
//   Repetitions (eg, 10). That way I get a stddev~15%, which is perfectly fine
//   for measuring the asymptotic behaviour of our code.
// - I prefer Repetitions over Iterations because that automatically gives us
//   mean/median/stddev in the results.
//
// Example of output when running 10 Repetitions:
//
// -----------------------------------------------------------------------------------------------------
// Benchmark                                                           Time             CPU   Iterations
// -----------------------------------------------------------------------------------------------------
// CheckpointBench/GetCursorsToDrop/100/iterations:1_mean           7160 ns         5470 ns           10
// CheckpointBench/GetCursorsToDrop/100/iterations:1_median         6597 ns         5116 ns           10
// CheckpointBench/GetCursorsToDrop/100/iterations:1_stddev         1331 ns          776 ns           10
//
// CheckpointBench/GetCursorsToDrop/1000/iterations:1_mean          7762 ns         6209 ns           10
// CheckpointBench/GetCursorsToDrop/1000/iterations:1_median        7190 ns         5517 ns           10
// CheckpointBench/GetCursorsToDrop/1000/iterations:1_stddev        1266 ns         1713 ns           10

// Arguments: numCheckpoints
BENCHMARK_REGISTER_F(CheckpointBench, ExtractClosedUnrefCheckpoints)
        ->Args({1})
        ->Args({10})
        ->Args({100})
        ->Args({1000})
        ->Args({10000})
        ->Iterations(1);

// Arguments: numCheckpoints
BENCHMARK_REGISTER_F(CheckpointBench, GetCursorsToDrop)
        ->Args({1})
        ->Args({10})
        ->Args({100})
        ->Args({1000})
        ->Args({10000})
        ->Iterations(1);

static void ExtractItemsArgs(benchmark::internal::Benchmark* b) {
    for (auto items = 1; items <= 10000; items *= 10) {
        for (const auto type : {CheckpointType::Disk, CheckpointType::Memory}) {
            for (const auto state : {CHECKPOINT_OPEN, CHECKPOINT_CLOSED}) {
                b->Args({std::underlying_type<CheckpointType>::type(type),
                         state,
                         items});
            }
        }
    }
}

// Arguments: numItems
BENCHMARK_REGISTER_F(CheckpointBench, ExtractItemsToExpel)
        ->Apply(ExtractItemsArgs)
        ->Iterations(1);

// Arguments: numItems
BENCHMARK_REGISTER_F(CheckpointBench, GetLowestCursor)
        ->Args({1})
        ->Args({10})
        ->Args({100})
        ->Args({1000})
        ->Args({10000})
        ->Args({100000});
