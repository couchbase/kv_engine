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
#include "checkpoint_manager.h"
#include "engine_fixture.h"
#include "ep_bucket.h"
#include "item.h"
#include "kv_bucket.h"
#include "stored_value_factories.h"

#include "../tests/module_tests/thread_gate.h"

#include <executor/fake_executorpool.h>
#include <folly/portability/GTest.h>
#include <mock/mock_synchronous_ep_engine.h>
#include <programs/engine_testapp/mock_server.h>
#include <algorithm>
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
        if (state.thread_index == 0) {
            engine->getKVBucket()->setVBucketState(Vbid(0),
                                                   vbucket_state_active);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            ASSERT_EQ(cb::engine_errc::success,
                      engine->getKVBucket()->deleteVBucket(vbid, nullptr));
            executorPool->runNextTask(
                    AUXIO_TASK_IDX,
                    "Removing (dead) vb:0 from memory and disk");
        }
        EngineFixture::TearDown(state);
    }

    /// Flush all items in the vBucket to disk.
    size_t flushAllItems(Vbid vbid) {
        size_t itemsFlushed = 0;
        auto& ep = dynamic_cast<EPBucket&>(*engine->getKVBucket());
        EPBucket::MoreAvailable moreAvailable;
        do {
            const auto res = ep.flushVBucket(vbid);
            moreAvailable = res.moreAvailable;
            itemsFlushed += res.numFlushed;
        } while (moreAvailable == EPBucket::MoreAvailable::Yes);
        return itemsFlushed;
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
        if (state.thread_index == 0) {
            memoryTracker = BenchmarkMemoryTracker::getInstance();
            memoryTracker->reset();
        }
        VBucketBench::SetUp(state);
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
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
                "max_size=1000000000;max_checkpoints=100000000;chk_max_items=1";

        EngineFixture::SetUp(state);
        if (state.thread_index == 0) {
            engine->getKVBucket()->setVBucketState(Vbid(0),
                                                   vbucket_state_active);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            engine->getKVBucket()->deleteVBucket(vbid, nullptr);
        }
        EngineFixture::TearDown(state);
    }
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
        vb->checkpointManager->clear(*vb, 0);
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

/*
 * MB-31834: Load throughput degradation when the number of checkpoints
 * eligible for removing is high.
 * At both checkpoint-removing and CM:queueDirty we acquire the CM::queueLock.
 * If the number of checkpoints eligible for removing is high, then any slow
 * operation under lock in CheckpointRemover delays frontend operations.
 * Note that the CheckpointRemover is O(N) in the size of the
 * CM::checkpointList. The regression is caused by a change in MB-30916 where we
 * started with deallocating checkpoint memory under lock.
 *
 * This benchmark measures resource contention between a mc:worker (frontend
 * thread) executing CM::queueDirty and the ClosedUnrefCheckpointRemoverTask
 * when the number of checkpoint eligible for removing is high.
 */
BENCHMARK_DEFINE_F(CheckpointBench, QueueDirtyWithManyClosedUnrefCheckpoints)
(benchmark::State& state) {
    // Test approach:
    // - Fix the number of checkpoints to be removed and run the
    //     CheckpointRemover in a background thread.
    // - Fix the number of checkpoint to be removed at each CheckpointRemover
    //     run (must be in the order of 10^3 to catch the regression scenario).
    // - Enqueue items into the CheckpointMaanager in the frontend thread.
    //     Break when the CheckpointRemover has done. Measure (A) the number
    //     of items enqueued and (B) the runtime of the frontend thread.
    // - Output the average runtime of frontend operations (B/A), which is the
    //     measured metric for this benchmark

    ASSERT_EQ(1, state.max_iterations);

    const size_t numCheckpoints = state.range(0);
    const size_t numCkptToRemovePerIteration = state.range(1);

    auto* vb = engine->getKVBucket()->getVBucket(vbid).get();
    auto* ckptMgr = vb->checkpointManager.get();

    // Same queued_item used for both checkpointList pre-filling and
    // front-end queueDirty().
    // Note that we will generate many 1-item checkpoints even if we enqueue
    // always the same identical item. That is because we have 'chk_max_items=1'
    // in configuration, which leads to the following order of steps at every
    // call to CM::queueDirty:
    // 1) close the open checkpoint
    // 2) create a new open checkpoint
    // 3) enqueue the new mutation (note that de-duplication happens here).
    //     The new mutation will be inserted into the /new/ (empty) open
    //     checkpoint. So, there will be no de-duplication.
    queued_item qi{
            new Item(StoredDocKey(std::string("key"), CollectionID::Default),
                     vbid,
                     queue_op::mutation,
                     /*revSeq*/ 0,
                     /*bySeq*/ 0)};

    // Pre-fill CM with the defined number of checkpoints
    for (size_t i = 0; i < numCheckpoints; ++i) {
        ckptMgr->queueDirty(*vb,
                            qi,
                            GenerateBySeqno::Yes,
                            GenerateCas::Yes,
                            /*preLinkDocCtx*/ nullptr);
    }

    ThreadGate tg(2);

    // Note: numUnrefItems is also the number of removed checkpoints as
    //     we have 1 item per checkpoint.
    size_t numUnrefItems = 0;
    size_t numCkptRemoverRuns = 0;
    std::atomic<bool> bgDone{false};
    auto removeCkpt = [&tg,
                       ckptMgr,
                       vb,
                       numCkptToRemovePerIteration,
                       &numUnrefItems,
                       &numCheckpoints,
                       &numCkptRemoverRuns,
                       &bgDone]() {
        tg.threadUp();
        bool newOpenCheckpointCreated;
        while (true) {
            // Simulate the Flusher, this makes the per-iteration num of
            // checkpoints eligible for removal (last/open checkpoint excluded).
            std::vector<queued_item> items;
            ckptMgr->getItemsForPersistence(items, numCkptToRemovePerIteration);

            numUnrefItems += ckptMgr->removeClosedUnrefCheckpoints(
                    *vb, newOpenCheckpointCreated);
            numCkptRemoverRuns++;

            // Break when all but the last item (in last checkpoint) is removed
            if (numUnrefItems >= numCheckpoints - 1) {
                break;
            }
        }
        // Done, exit frontend thread
        bgDone = true;
    };

    // Note: thread started but still blocked on ThreadGate
    std::thread bgThread(removeCkpt);

    size_t itemsQueuedTotal = 0;
    size_t runtime = 0;
    while (state.KeepRunning()) {
        tg.threadUp();
        auto begin = std::chrono::steady_clock::now();
        while (!bgDone) {
            ckptMgr->queueDirty(*vb,
                                qi,
                                GenerateBySeqno::Yes,
                                GenerateCas::Yes,
                                /*preLinkDocCtx*/ nullptr);
            itemsQueuedTotal++;
        }

        runtime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now() - begin)
                          .count();
    }
    ASSERT_TRUE(itemsQueuedTotal);

    state.counters["NumCheckpointsRemoverRuns"] = numCkptRemoverRuns;
    state.counters["NumCheckpointsRemovedPerIteration"] =
            numUnrefItems / numCkptRemoverRuns;
    state.counters["ItemsEnqueued"] = itemsQueuedTotal;
    // Clang-scan-build complains about a possible division on 0.. guess
    // it doesn't know that the ASSERT_TRUE above would terminate the method
    if (itemsQueuedTotal > 0) {
        state.counters["AvgQueueDirtyRuntime"] = runtime / itemsQueuedTotal;
    }

    bgThread.join();
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

// Arguments: numCheckpoints, numCkptToRemovePerIteration
BENCHMARK_REGISTER_F(CheckpointBench, QueueDirtyWithManyClosedUnrefCheckpoints)
        ->Args({1000000, 1000})
        ->Iterations(1);
