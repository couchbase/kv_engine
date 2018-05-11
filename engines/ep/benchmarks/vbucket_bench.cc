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

#include <mock/mock_synchronous_ep_engine.h>

#include <gtest/gtest.h>

#include <algorithm>

class VBucketBench : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        EngineFixture::SetUp(state);
        engine->getKVBucket()->setVBucketState(0, vbucket_state_active, false);
    }

    void TearDown(const benchmark::State& state) override {
        engine->getKVBucket()->deleteVBucket(vbid, this);
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
 * Benchmark queueing items into a vBucket.
 * Items have a 10% chance of being a duplicate key of a previous item (to
 * model de-dupe).
 */
BENCHMARK_DEFINE_F(VBucketBench, QueueDirty)(benchmark::State& state) {
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

BENCHMARK_DEFINE_F(VBucketBench, FlushVBucket)(benchmark::State& state) {
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

// Run with item counts from 1..10,000,000.
BENCHMARK_REGISTER_F(VBucketBench, QueueDirty)
        ->Args({1})
        ->Args({100})
        ->Args({10000})
        ->Args({1000000});

BENCHMARK_REGISTER_F(VBucketBench, FlushVBucket)
        ->RangeMultiplier(10)
        ->Range(1, 1000000);
