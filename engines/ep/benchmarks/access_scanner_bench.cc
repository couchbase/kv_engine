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
 * Benchmarks for the AccessScanner class - measuring speed and memory overhead.
 */

#include <access_scanner.h>
#include <fakes/fake_executorpool.h>
#include <mock/mock_synchronous_ep_engine.h>
#include <programs/engine_testapp/mock_server.h>

#include "benchmark_memory_tracker.h"

#include "engine_fixture.h"
#include "item.h"
#include "kv_bucket.h"

class AccessLogBenchEngine : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        memoryTracker = BenchmarkMemoryTracker::getInstance();
        memoryTracker->reset();

        // If the access scanner is running then it will always scan
        varConfig = "alog_resident_ratio_threshold=100;";
        varConfig += "alog_max_stored_items=" +
                     std::to_string(alog_max_stored_items);
        EngineFixture::SetUp(state);
    }

    void TearDown(const benchmark::State& state) override {
        EngineFixture::TearDown(state);
        if (state.thread_index == 0) {
            memoryTracker->destroyInstance();
        }
    }

    const size_t alog_max_stored_items = 2048;

    BenchmarkMemoryTracker* memoryTracker = nullptr;
};

/*
 * Varies whether the access scanner is running or not. Also varies the
 * number of items stored in the vbucket. The purpose of this benchmark is to
 * measure the maximum memory usage of the access scanner.
 * Variables:
 *  - range(0) : Whether to run access scanner constantly (0: no, 1: yes)
 *  - range(1) : The number of items to fill the vbucket with
 *
 */
BENCHMARK_DEFINE_F(AccessLogBenchEngine, MemoryOverhead)
(benchmark::State& state) {
    ExTask task = nullptr;
    engine->getKVBucket()->setVBucketState(Vbid(0), vbucket_state_active);
    if (state.range(0) == 1) {
        state.SetLabel("AccessScanner");
        task = std::make_shared<AccessScanner>(*(engine->getKVBucket()),
                                               engine->getConfiguration(),
                                               engine->getEpStats(),
                                               1000);
        ExecutorPool::get()->schedule(task);
    } else {
        state.SetLabel("Control");
    }

    // The content of each doc, nothing too large
    std::string value(200, 'x');

    // We have a key prefix so that our keys are more realistic in length
    std::string keyPrefixPre(20, 'a');

    for (int i = 0; i < state.range(1); ++i) {
        auto item = make_item(vbid, keyPrefixPre + std::to_string(i), value);
        engine->getKVBucket()->set(item, cookie);
    }
    size_t baseMemory = memoryTracker->getCurrentAlloc();
    while (state.KeepRunning()) {
        if (state.range(0) == 1) {
            executorPool->wake(task->getId());
            executorPool->runNextTask(AUXIO_TASK_IDX, "Generating access log");
            executorPool->runNextTask(AUXIO_TASK_IDX,
                                      "Item Access Scanner on vb:0");
        }
    }
    state.counters["MaxBytesAllocatedPerItem"] =
            (memoryTracker->getMaxAlloc() - baseMemory) / alog_max_stored_items;
}

static void AccessScannerArguments(benchmark::internal::Benchmark* b) {
    std::array<int, 2> numItems{{32768, 65536}};
    for (int j : numItems) {
        b->ArgPair(0, j);
        b->ArgPair(1, j);
    }
}

BENCHMARK_REGISTER_F(AccessLogBenchEngine, MemoryOverhead)
        ->Apply(AccessScannerArguments)
        ->MinTime(0.000001);
