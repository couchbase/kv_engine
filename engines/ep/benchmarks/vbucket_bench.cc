/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/*
 * Benchmarks relating to the VBucket class.
 */

#include "benchmark_memory_tracker.h"
#include "checkpoint_manager.h"
#include "engine_fixture.h"
#include "stored_value_factories.h"

#include "../tests/module_tests/thread_gate.h"

#include <mock/mock_synchronous_ep_engine.h>
#include <programs/engine_testapp/mock_server.h>

#include <gtest/gtest.h>

#include <algorithm>

class VBucketBench : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        // A number of benchmarks require more than the default 100MB bucket
        // quota - bump to ~1GB.
        varConfig = "max_size=1000000000";

        EngineFixture::SetUp(state);
        if (state.thread_index == 0) {
            engine->getKVBucket()->setVBucketState(
                    0, vbucket_state_active, false);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            engine->getKVBucket()->deleteVBucket(vbid, this);
        }
        EngineFixture::TearDown(state);
    }

    /// Flush all items in the vBucket to disk.
    size_t flushAllItems(uint16_t vbid) {
        size_t itemsFlushed = 0;
        auto& ep = dynamic_cast<EPBucket&>(*engine->getKVBucket());
        bool moreAvailable;
        do {
            size_t count;
            std::tie(moreAvailable, count) = ep.flushVBucket(vbid);
            itemsFlushed += count;
        } while (moreAvailable);
        return itemsFlushed;
    }
};

/**
 * Benchmark fixture for VBucket tests which includes a memoryTracker to
 * allow monitoring of current/peak memory usage.
 */
class MemTrackingVBucketBench : public VBucketBench {
protected:
    void SetUp(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            memoryTracker = BenchmarkMemoryTracker::getInstance(
                    *get_mock_server_api()->alloc_hooks);
            memoryTracker->reset();
        }
        VBucketBench::SetUp(state);
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            engine->getKVBucket()->deleteVBucket(vbid, this);
        }
        EngineFixture::TearDown(state);
    }

    BenchmarkMemoryTracker* memoryTracker;
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
            engine->getKVBucket()->setVBucketState(
                    0, vbucket_state_active, false);
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            engine->getKVBucket()->deleteVBucket(vbid, this);
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
    const auto itemCount = state.range(0);

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
            ASSERT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(item, cookie));
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
    const auto itemCount = state.range(0);
    int itemsFlushedTotal = 0;

    // Memory size before flushing.
    size_t baseBytes = 0;

    // Maximum memory during flushing.
    size_t peakBytes = 0;

    // Pre-size the VBucket's hashtable so a sensible size.
    engine->getKVBucket()->getVBucket(vbid)->ht.resize(itemCount);

    while (state.KeepRunning()) {
        // Add the given number of items to checkpoint manager.
        state.PauseTiming();
        std::string value(1, 'x');
        for (int i = 0; i < itemCount; ++i) {
            auto item = make_item(
                    vbid, std::string("key") + std::to_string(i), value);
            ASSERT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(item, cookie));
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
 * Measures resource contention between a mc::worker (front-end thread) adding
 * incoming mutations to the CheckpointManager (CM) and the
 * ClosedUnrefCheckpointRemoverTask (RemTask) when the number of checkpoint
 * eligible for removing is high.
 */
BENCHMARK_DEFINE_F(CheckpointBench, QueueDirtyWithManyClosedUnrefCheckpoints)
(benchmark::State& state) {
    const auto itemCount = state.range(0);
    size_t itemsQueuedTotal = 0;

    auto* vb = engine->getKVBucket()->getVBucket(vbid).get();
    auto* ckptMgr = vb->checkpointManager.get();

    // I need 3 threads running in parallel for simulating the scenarios above:
    // 1) a FrontEnd thread enqueues items into the CM
    // 2) a Flusher thread moves the persistence cursor onward. This will make
    //     closed checkpoints unreferenced and eligible for removing
    // 3) a RemTask running in a dedicated thread
    //
    // Also, I need to keep the number of checkpoint constantly high by
    // pre-filling the CheckpointManager::checkpointList. Details about this
    // in comments below.

    ThreadGate tg(3);
    std::atomic<bool> bgRun{true};

    // TODO: flusherApproxLimit should be const, but if I do that then:
    //     - if I capture it explicitly, then Clang compilation
    //         gives the "lambda capture is not required to be captured for
    //         this use" warning. That's because Clang implements the new
    //         C++14 lambda-capture specs where const and constexpr
    //         variables are implicitly captured
    //     - if I don't capture it explicitly, MSVC gives compilation error
    //         C3493 ("<variable> cannot be implicitly captured because no
    //         default capture mode has been specified")
    size_t flusherApproxLimit = 2000;
    // Just perform the flusher steps required for moving the persistence
    // cursor and creating preconditions for checkpoint removal.
    auto flush = [&tg, &bgRun, ckptMgr, flusherApproxLimit]() {
        tg.threadUp();
        while (bgRun) {
            std::vector<queued_item> items;
            // I want the Flusher to make 'flusherApproxLimit' unreferenced
            // checkpoints at every run. Given that:
            //     - in our scenario we have 1 item per checkpoint, so every
            //       checkpoint: [ckpt-start    mutation    ckpt-end]
            //     - we account also metaitems in the 'approxLimit' in
            //       CheckpointManager::getItemsForPersistence
            // then I have to pass 'approxLimit = flusherApproxLimit * 3'
            ckptMgr->getItemsForPersistence(items, flusherApproxLimit * 3);
            ckptMgr->itemsPersisted();
        }
    };

    size_t numUnrefItems = 0;
    size_t numCkptRemoverRuns = 0;
    auto removeCkpt =
            [&tg, &bgRun, ckptMgr, vb, &numUnrefItems, &numCkptRemoverRuns]() {
                tg.threadUp();
                bool newOpenCheckpointCreated;
                while (bgRun) {
                    auto removed = ckptMgr->removeClosedUnrefCheckpoints(
                            *vb, newOpenCheckpointCreated);
                    numUnrefItems += removed;
                    numCkptRemoverRuns++;
                }
            };

    // I /need/ to perform front-end operations in a situation where the
    // number of the closed-unref-checkpoints is constantly high
    // (thousands).
    // Given that all threads (FrontEnd, Flusher, CkptRem) run concurrently,
    // the CkptRem will remove a checkpoint as soon as it becomes closed
    // and unreferenced, so we will not accumulate enough checkpoint to
    // create the test scenario that I need.
    //
    // One (wrong) approach is to put a delay in the CkptRem task, so that
    // we accumulate checkpoints before the remover performs its operations.
    // Obviously the "right" sleep time is machine-dependent, so that's a
    // bad idea.
    // Another approach is to fork many FrontEnd threads, so that we will
    // add some checkpoints before the CkptRem runs. In this latter case
    // the problem is that it's almost impossible to reach the desired
    // number (thousands) of closed-unref-checkpoints (tested locally, with
    // 8 FrontEnd threads we accumulate 8 checkpoint on average, as
    // expected).
    // Also, in both cases only a small percentage of FrontEnd executions
    // will be affected by the few slow runs of CkptRem. So the impact on
    // the final measurement will be irrelevant.
    //
    // What I actually do is to pre-fill the CheckpointManager with many
    // checkpoints. The definition of 'many' is:
    //
    //     "the number of checkpoints so that the CkptRem thread will find
    //     thousands of checkpoints to remove at every run, while the
    //     FrontEnd thread is still running"
    //
    // To determine that number I have to consider that:
    // 1) We have 3 tasks (FrontEnd, Flusher, CkptRem) running concurrently
    //     on 3 threads
    // 2) Even if we run on multi-CPUs, all the 3 threads synchronize
    //     on the same CM::queueLock
    // 3) Point 2) implies that it is very likely that we end up with a
    //     kind of Round-Robin execution of the 3 threads
    // 4) The Flusher moves the persistence cursor onward. The number of
    //     movements is limited by the 'approxLimit' param.
    //     In this setup we have 1 item per checkpoint, so the 'approxLimit'
    //     param is the upper-bound for the number of checkpoints that will
    //     become unreferenced at every Flusher run.
    // 5) So, given that I want to measure the perf of the FrontEnd thread
    //     performing CM::queueDirty() 'itemCount' times, then I need to
    //     pre-fill the CheckpointManager with 'itemCount * approxLimit'
    //     checkpoints to ensure a busy CkptRem task during the entire
    //     FrontEnd perf measurement.
    //
    // One final note.
    // Our test Flusher performs:
    //     a) CM::getItemsForPersistence
    //     b) CM::itemsPersisted
    // Both the functions above acquire and release the CM::queueLock, but
    // new closed-unref-checkpoints will be ready for removing only after
    // both functions have executed.
    // The consequence is that in the worst case (at regression) it is very
    // likely to end up with the following execution interleaving:
    //
    //     1) Flusher - getItemsForCursor
    //     2) CkptRem - closed-unref-checkpoints: 0, runtime: 1 us
    //     3) FrontEnd - queueDirty - runtime: 12868 us
    //     4) Flusher - itemsPersisted
    //     5) CkptRem - closed-unref-checkpoints: 10000, runtime: 17001 us
    //     6) FrontEnd - queueDirty - runtime: 19666 us
    //
    // So, actually "only" half of the FrontEnd runs will be affected by
    // slow runtime of the CkptRem.
    // Also, that cuts the required number of checkpoints for pre-filling
    // to approximately 'itemCount * approxLimit / 2'.
    // To help in verifying that the reasoning above is correct, I add the
    // 'AvgNumCheckpointRemoved' counter to the benchmark output,
    // which should be in the order of 10^3 to consider the test valid.
    //
    // As last adjustment, I need to add a number of checkpoints to cover
    // all the iterations performed by the benchmark, so I need to multiply
    // by 'state.max_iteration'.

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
    const size_t nCheckpoints =
            itemCount * flusherApproxLimit / 2 * state.max_iterations;
    for (size_t i = 0; i < nCheckpoints; ++i) {
        ckptMgr->queueDirty(*vb,
                            qi,
                            GenerateBySeqno::Yes,
                            GenerateCas::Yes,
                            /*preLinkDocCtx*/ nullptr);
    }

    // Start background threads
    bgRun = true;
    std::thread ckptRemover(removeCkpt);
    std::thread flusher(flush);

    tg.threadUp();

    while (state.KeepRunning()) {
        // Benchmark: Add the given number of items to checkpoint manager
        for (int i = 0; i < itemCount; ++i) {
            ckptMgr->queueDirty(*vb,
                                qi,
                                GenerateBySeqno::Yes,
                                GenerateCas::Yes,
                                /*preLinkDocCtx*/ nullptr);
            ++itemsQueuedTotal;
        }
    }

    // Cleanup CheckpointManager
    ckptMgr->clear(*vb, 0);
    // Destroy background threads
    bgRun = false;
    ckptRemover.join();
    flusher.join();

    state.SetItemsProcessed(itemsQueuedTotal);
    state.counters["AvgNumCheckpointRemoved"] =
            numCkptRemoverRuns > 0 ? numUnrefItems / numCkptRemoverRuns : 0;
    state.counters["NumCheckpointRemoverRuns"] = numCkptRemoverRuns;
}

// Run with item counts from 1..10,000,000.
BENCHMARK_REGISTER_F(MemTrackingVBucketBench, QueueDirty)
        ->Args({1})
        ->Args({100})
        ->Args({10000})
        ->Args({1000000});

BENCHMARK_REGISTER_F(MemTrackingVBucketBench, FlushVBucket)
        ->RangeMultiplier(10)
        ->Range(1, 1000000);

BENCHMARK_REGISTER_F(VBucketBench, CreateDeleteStoredValue)->Threads(16);

BENCHMARK_REGISTER_F(CheckpointBench, QueueDirtyWithManyClosedUnrefCheckpoints)
        ->Args({100})
        ->Iterations(5)
        ->Repetitions(10);
