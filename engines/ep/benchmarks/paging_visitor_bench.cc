/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "engine_fixture.h"

#include "module_tests/test_helpers.h"

#include "checkpoint_manager.h"

#include <benchmark/benchmark.h>
#include <executor/fake_executorpool.h>
#include <kv_bucket.h>
#include <paging_visitor.h>
#include <platform/cb_arena_malloc.h>
#include <platform/semaphore.h>
#include <vbucket.h>

#include <folly/portability/GTest.h>

#include <random>

class PagingVisitorBench : public EngineFixture {
public:
    void SetUp(const benchmark::State& state) override {
        varConfig =
                "item_eviction_policy=full_eviction;"
                "checkpoint_memory_recovery_upper_mark=1.0";

        EngineFixture::SetUp(state);

        engine->getKVBucket()->createAndScheduleCheckpointDestroyerTasks();
        engine->getKVBucket()->createAndScheduleCheckpointRemoverTasks();

        // Run with a few different quotas
        quotaMB = state.range(0);
        engine->setMaxDataSize(quotaMB * 1024 * 1024);

        // Disable the pager as we'll run things manually
        engine->getKVBucket()->disableItemPager();

        auto* store = engine->getKVBucket();
        ASSERT_EQ(cb::engine_errc::success,
                  store->setVBucketState(vbid, vbucket_state_active));

        // Remove the persistence cursor of our vBucket so that we don't have to
        // flush during the benchmark (makes it faster).
        auto vb = engine->getVBucket(vbid);
        auto* pCursor = vb->checkpointManager->getPersistenceCursor();
        vb->checkpointManager->removeCursor(*pCursor);
    }

    void TearDown(const benchmark::State& state) override {
        ASSERT_EQ(cb::engine_errc::success,
                  engine->getKVBucket()->deleteVBucket(vbid, nullptr))
                << "Couldn't delete vb";
        executorPool->runNextTask(AUXIO_TASK_IDX,
                                  "Removing (dead) vb:0 from memory and disk");

        EngineFixture::TearDown(state);
    }

    void runAllNonIOTasks() {
        // Run everything in the NONIO queue. As the executor is
        // split into two queues and we have to poke it to move
        // things that need to run now from the future queue to
        // the ready queue it's easier to just run until we have
        // nothing else to run (i.e. CheckedExecutor throws)
        auto q = executorPool->getLpTaskQ()[NONIO_TASK_IDX];
        try {
            while (true) {
                CheckedExecutor executor(executorPool, *q);
                executor.runCurrentTask();
                executor.completeCurrentTask();
            }
        } catch (std::logic_error& e) {
        }
    }

    void recoverMemory(Vbid vbid) {
        // Creating a new checkpoint allows us to remove the old one
        auto vBucket = engine->getVBucket(vbid);
        vBucket->checkpointManager->createNewCheckpoint();

        runAllNonIOTasks();
    }

    void populateUntilFull(Vbid vbid) {
        std::string value(1024, 'x');

        // Nuke the HT in case the previous test left something in it
        auto vBucket = engine->getVBucket(vbid);
        vBucket->ht.clear();

        // It needs resizing such that we can store all of our items in it.
        // If we don't resize the HashTable then we basically turn it into a
        // bunch of linked lists which makes the benchmark SetUp take much
        // longer. quotaMB * 1024 oversizes us a fair bit (~30-40%) due to
        // other overheads in the system (including things in the order of O(n)
        // like metadata) but it works well enough.
        vBucket->ht.resize(quotaMB * 1024);
        Expects(vBucket->ht.getNumItems() == 0);

        // Uniform distribution over the range of counters values.
        // Uniform isn't likely in a real world scenario, but it's a good worst
        // case for benchmarks evicting everything as they must iterate the
        // histograms to the full extent to calculate the values at the given
        // percentile (100).
        //
        // Freq values are set between 1 and 254 so that tests can poke them
        // in certain ways. Having a min > 0 allows us to fudge the pager
        // eviction by setting the first item that the pager visits in each
        // HashBucket to a freq counter of 0, and having a max of 254 stops the
        // ItemFreqDecayer from triggering and pushing values down
        // (potentially to 0). This allows us to create stable benchmarks that
        // always evict/consider eligible for eviction the same number of items.
        std::uniform_int_distribution<uint64_t> freqDist(
                1, std::numeric_limits<uint8_t>::max() - 1);
        std::uniform_int_distribution<uint64_t> casDist(
                1, std::numeric_limits<uint64_t>::max());
        std::mt19937 freqMt;
        std::mt19937 casMt;

        for (auto i = 0;; i++) {
            auto key = std::string("key") + std::to_string(i);
            cb::engine_errc ret;
            {
                auto item = make_item(vbid, key, value);
                ret = engine->getKVBucket()->set(item, cookie);
            }

            {
                // Poke the freq counter and cas values so they're not all
                // the same
                auto vBucket = engine->getVBucket(vbid);
                auto htRet = vBucket->ht.findForWrite(makeStoredDocKey(key));
                if (htRet.storedValue) {
                    auto freq = freqDist(freqMt);
                    htRet.storedValue->setFreqCounterValue(freq);
                    htRet.storedValue->setCas(casDist(casMt));

                    // Mark clean as we are skipping persistence
                    htRet.storedValue->markClean();
                }
            }

            if (ret == cb::engine_errc::no_memory ||
                ret == cb::engine_errc::temporary_failure) {
                // Flush once we are above LWM to save time, flushing every
                // item is slow.
                recoverMemory(vbid);

                if (engine->getEpStats().getPreciseTotalMemoryUsed() >=
                    engine->getEpStats().mem_high_wat) {
                    return;
                }
            }
        }

        Expects(vBucket->ht.getSize() < vBucket->ht.getNumItems() * 2);
    }

    size_t quotaMB;
};

class LambdaEvictionVisitor : public HashTableVisitor {
public:
    using VisitFn =
            std::function<bool(const HashTable::HashBucketLock&, StoredValue&)>;

    LambdaEvictionVisitor(VBucket& vb) : vb(vb) {
    }

    LambdaEvictionVisitor(VBucket& vb, VisitFn fn)
        : vb(vb), visitFn(std::move(fn)) {
    }

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override {
        return visitFn(lh, v);
    }

    void setUpHashBucketVisit() override {
        readHandle = vb.lockCollections();
    }

    void tearDownHashBucketVisit() override {
        readHandle.unlock();
    }

    VBucket& vb;
    std::function<bool(const HashTable::HashBucketLock&, StoredValue&)> visitFn;

    Collections::VB::ReadHandle readHandle;
};

BENCHMARK_DEFINE_F(PagingVisitorBench, SingleVBucket)
(benchmark::State& state) {
    auto semaphore = std::make_shared<cb::Semaphore>();
    Configuration& cfg = engine->getConfiguration();

    while (state.KeepRunning()) {
        state.PauseTiming();

        populateUntilFull(vbid);

        auto pv = std::make_unique<PagingVisitor>(
                *engine->getKVBucket(),
                engine->getEpStats(),
                EvictionRatios{
                        1 /* active&pending */,
                        1 /* replica */}, // evict everything (but this will
                // not be run)
                semaphore,
                ITEM_PAGER,
                false,
                VBucketFilter(std::vector<Vbid>{vbid}),
                cfg.getItemEvictionAgePercentage(),
                cfg.getItemEvictionFreqCounterAgeThreshold());
        ObjectRegistry::onSwitchThread(engine.get());

        state.ResumeTiming();

        // Benchmark - measure how long it takes to run the PagingVisitor
        // over the vBucket
        pv->visitBucket(*engine->getVBucket(vbid));

        state.PauseTiming();
    }
}

BENCHMARK_DEFINE_F(PagingVisitorBench, PagerIteration)
(benchmark::State& state) {
    auto semaphore = std::make_shared<cb::Semaphore>();
    Configuration& cfg = engine->getConfiguration();

    while (state.KeepRunning()) {
        state.PauseTiming();

        populateUntilFull(vbid);

        // Poke the first item in the HashTable to have a very low freq counter
        // to "poison" the MFU histogram that the PagingVisitor creates. This
        // should prevent us from evicting almost all items with a low enough
        // eviction ratio
        auto vb = engine->getVBucket(vbid);
        LambdaEvictionVisitor ev(*vb, [](const auto& hbl, auto& v) {
            v.setFreqCounterValue(0);
            return false;
        });
        vb->ht.visit(ev);

        auto pv = std::make_unique<PagingVisitor>(
                *engine->getKVBucket(),
                engine->getEpStats(),
                EvictionRatios{
                        0.000000001 /* active&pending */,
                        1 /* replica */}, // evict everything (but this will
                // not be run)
                semaphore,
                ITEM_PAGER,
                false,
                VBucketFilter(std::vector<Vbid>{vbid}),
                cfg.getItemEvictionAgePercentage(),
                cfg.getItemEvictionFreqCounterAgeThreshold());
        ObjectRegistry::onSwitchThread(engine.get());

        state.ResumeTiming();

        // Benchmark - measure how long it takes to run the PagingVisitor
        // over the vBucket
        pv->visitBucket(*engine->getVBucket(vbid));

        state.PauseTiming();
    }
}

BENCHMARK_DEFINE_F(PagingVisitorBench, EvictAllWithoutPager)
(benchmark::State& state) {
    // Populate until we're temp failing to ensure that we can run
    // the pager
    std::vector<Vbid> vbids = {vbid};

    while (state.KeepRunning()) {
        state.PauseTiming();

        populateUntilFull(vbid);

        auto vb = engine->getVBucket(vbid);

        // Visitor will evict everything
        LambdaEvictionVisitor ev(*vb);
        ev.visitFn = [&vb, &ev](const auto& hbl, auto& v) {
            auto* sv = &v;
            vb->pageOut(ev.readHandle, hbl, sv, false /*isDropped*/);
            return true;
        };

        state.ResumeTiming();

        // Benchmark - measure how long it takes to evict everything
        vb->ht.visit(ev);

        state.PauseTiming();
    }
}

BENCHMARK_REGISTER_F(PagingVisitorBench, SingleVBucket)
        ->Threads(1)
        ->Arg(10)
        ->Arg(128)
        ->Arg(256)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(PagingVisitorBench, PagerIteration)
        ->Threads(1)
        ->Arg(10)
        ->Arg(128)
        ->Arg(256)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(PagingVisitorBench, EvictAllWithoutPager)
        ->Threads(1)
        ->Arg(10)
        ->Arg(128)
        ->Arg(256)
        ->Unit(benchmark::kMillisecond);
