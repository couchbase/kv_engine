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

#include <access_scanner.h>
#include <benchmark/benchmark.h>
#include <fakes/fake_executorpool.h>
#include <mock/mock_synchronous_ep_engine.h>
#include <programs/engine_testapp/mock_server.h>
#include "benchmark_memory_tracker.h"
#include "dcp/dcpconnmap.h"

class EngineFixture : public benchmark::Fixture {
protected:
    void SetUp(const benchmark::State& state) override {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        executorPool = reinterpret_cast<SingleThreadedExecutorPool*>(
                ExecutorPool::get());
        memoryTracker = BenchmarkMemoryTracker::getInstance(
                *get_mock_server_api()->alloc_hooks);
        memoryTracker->reset();
        std::string config = "dbname=benchmarks-test;" + varConfig;

        engine.reset(new SynchronousEPEngine(config));
        ObjectRegistry::onSwitchThread(engine.get());

        engine->setKVBucket(
                engine->public_makeBucket(engine->getConfiguration()));

        engine->public_initializeEngineCallbacks();
        initialize_time_functions(get_mock_server_api()->core);
        cookie = create_mock_cookie();
    }

    void TearDown(const benchmark::State& state) override {
        executorPool->cancelAndClearAll();
        destroy_mock_cookie(cookie);
        destroy_mock_event_callbacks();
        engine->getDcpConnMap().manageConnections();
        engine.reset();
        ObjectRegistry::onSwitchThread(nullptr);
        ExecutorPool::shutdown();
        memoryTracker->destroyInstance();
    }

    Item make_item(uint16_t vbid,
                   const std::string& key,
                   const std::string& value) {
        uint8_t ext_meta[EXT_META_LEN] = {PROTOCOL_BINARY_DATATYPE_JSON};
        Item item({key, DocNamespace::DefaultCollection},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  value.c_str(),
                  value.size(),
                  ext_meta,
                  sizeof(ext_meta));
        item.setVBucketId(vbid);
        return item;
    }

    std::unique_ptr<SynchronousEPEngine> engine;
    const void* cookie = nullptr;
    const int vbid = 0;

    // Allows subclasses to add stuff to the config
    std::string varConfig;
    BenchmarkMemoryTracker* memoryTracker;
    SingleThreadedExecutorPool* executorPool;
};

class AccessLogBenchEngine : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        // If the access scanner is running then it will always scan
        varConfig = "alog_resident_ratio_threshold=100;";
        EngineFixture::SetUp(state);
    }
};

ProcessClock::time_point runNextTask(SingleThreadedExecutorPool* pool,
                                     task_type_t taskType,
                                     std::string expectedTask) {
    CheckedExecutor executor(pool, *pool->getLpTaskQ()[taskType]);
    executor.runCurrentTask(expectedTask);
    return executor.completeCurrentTask();
}

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
    engine->getKVBucket()->setVBucketState(0, vbucket_state_active, false);
    if (state.range(0) == 1) {
        state.SetLabel("AccessScanner");
        task = make_STRCPtr<AccessScanner>(
                *(engine->getKVBucket()), engine->getEpStats(), 1000);
        ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
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
            runNextTask(executorPool,
                        AUXIO_TASK_IDX,
                        "Generating access "
                        "log");
            runNextTask(executorPool,
                        AUXIO_TASK_IDX,
                        "Item Access Scanner on"
                        " vb 0");
        }
    }
    state.counters["MaxBytesAllocatedPerItem"] =
            (memoryTracker->getMaxAlloc() - baseMemory) / state.range(1);
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

static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";
int main(int argc, char** argv) {
    putenv(allow_no_stats_env);
    mock_init_alloc_hooks();
    init_mock_server(true);
    HashTable::setDefaultNumLocks(47);
    initialize_time_functions(get_mock_server_api()->core);
    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
}
