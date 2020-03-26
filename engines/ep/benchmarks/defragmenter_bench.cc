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

#include "defragmenter_visitor.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "item.h"
#include "tests/module_tests/defragmenter_test.h"
#include "tests/module_tests/test_helpers.h"

#include <benchmark/benchmark.h>
#include <engines/ep/src/defragmenter.h>
#include <folly/portability/GTest.h>
#include <valgrind/valgrind.h>

class DefragmentBench : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override {
        // The first parameter specifies the eviction mode:
        EvictionPolicy evictionPolicy;
        switch (state.range(0)) {
        case 0:
            state.SetLabel("ValueOnly");
            evictionPolicy = EvictionPolicy::Value;
            break;
        case 1:
            state.SetLabel("FullEviction");
            evictionPolicy = EvictionPolicy::Full;
            break;
        default:
            FAIL() << "Invalid input param(0) value:" << state.range(0);
        }
        vbucket.reset(
                new EPVBucket(Vbid(0),
                              vbucket_state_active,
                              globalStats,
                              checkpointConfig,
                              /*kvshard*/ nullptr,
                              /*lastSeqno*/ 1000,
                              /*lastSnapStart*/ 0,
                              /*lastSnapEnd*/ 0,
                              /*table*/ nullptr,
                              std::make_shared<DummyCB>(),
                              /*newSeqnoCb*/ nullptr,
                              [](Vbid) { return; },
                              NoopSyncWriteCompleteCb,
                              NoopSeqnoAckCb,
                              config,
                              evictionPolicy,
                              std::make_unique<Collections::VB::Manifest>()));

        populateVbucket();
    }

    void TearDown(const ::benchmark::State& state) override {
        vbucket.reset();
    }

protected:
    /* Fill the bucket with the given number of docs.
     */
    void populateVbucket() {
        // How many items to create in the VBucket. Use a large number for
        // normal runs when measuring performance (ideally we want to exceed
        // the D$ as that's how we'd expect to run in production), but a very
        // small number (enough for functional testing) when running under
        // Valgrind where there's no sense in measuring performance.
        const size_t ndocs = RUNNING_ON_VALGRIND ? 10 : 500000;

        /* Set the hashTable to a sensible size */
        vbucket->ht.resize(ndocs);

        /* Store items */
        char value[256];
        for (size_t i = 0; i < ndocs; i++) {
            std::string key = "key" + std::to_string(i);
            Item item(makeStoredDocKey(key), 0, 0, value, sizeof(value));
            ASSERT_EQ(MutationStatus::WasClean, vbucket->ht.set(item));
        }

        ASSERT_EQ(ndocs, vbucket->ht.getNumItems());
    }

    /* Measure the rate at which the defragmenter can defragment documents, using
     * the given age threshold.
     *
     * Setup a Defragmenter, then time how long it takes to visit them all
     * documents in the given vbucket, 10 passes times.
     * @return a pair of {items visited, duration}.
     */
    std::pair<size_t, std::chrono::nanoseconds> benchmarkDefragment(
            uint8_t age_threshold,
            std::chrono::milliseconds chunk_duration) {
        // Create and run visitor for the specified number of iterations, with
        // the given age.
        DefragmentVisitor visitor(DefragmenterTask::getMaxValueSize());

        visitor.setBlobAgeThreshold(age_threshold);
        visitor.setStoredValueAgeThreshold(age_threshold);
        visitor.setCurrentVBucket(*vbucket);

        // Need to run 10 passes; so we allow the deframenter to defrag at
        // least once (given the age_threshold may be up to 10).
        const size_t passes = 10;

        auto start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < passes; i++) {
            // Loop until we get to the end; this may take multiple chunks
            // depending
            // on the chunk_duration.
            HashTable::Position pos;
            while (pos != vbucket->ht.endPosition()) {
                visitor.setDeadline(std::chrono::steady_clock::now() +
                                    chunk_duration);
                pos = vbucket->ht.pauseResumeVisit(visitor, pos);
            }
        }
        auto end = std::chrono::steady_clock::now();
        auto duration = (end - start);

        return {visitor.getVisitedCount(), duration};
    }

    std::unique_ptr<VBucket> vbucket;
    EPStats globalStats;
    CheckpointConfig checkpointConfig;
    Configuration config;
};

BENCHMARK_DEFINE_F(DefragmentBench, Visit)(benchmark::State& state) {
    std::pair<size_t, std::chrono::nanoseconds> total;
    while (state.KeepRunning()) {
        auto result = benchmarkDefragment(std::numeric_limits<uint8_t>::max(),
                                          std::chrono::minutes(1));
        total.first += result.first;
        total.second += result.second;
    }
    state.counters["ItemsPerSec"] =
            total.first / std::chrono::duration<double>(total.second).count();
}

BENCHMARK_DEFINE_F(DefragmentBench, DefragAlways)(benchmark::State& state) {
    std::pair<size_t, std::chrono::nanoseconds> total;
    while (state.KeepRunning()) {
        auto result = benchmarkDefragment(0, std::chrono::minutes(1));
        total.first += result.first;
        total.second += result.second;
    }
    state.counters["ItemsPerSec"] =
            total.first / std::chrono::duration<double>(total.second).count();
}

BENCHMARK_DEFINE_F(DefragmentBench, DefragAge10)(benchmark::State& state) {
    std::pair<size_t, std::chrono::nanoseconds> total;
    while (state.KeepRunning()) {
        auto result = benchmarkDefragment(10, std::chrono::minutes(1));
        total.first += result.first;
        total.second += result.second;
    }
    state.counters["ItemsPerSec"] =
            total.first / std::chrono::duration<double>(total.second).count();
}

BENCHMARK_DEFINE_F(DefragmentBench, DefragAge10_20ms)(benchmark::State& state) {
    std::pair<size_t, std::chrono::nanoseconds> total;
    while (state.KeepRunning()) {
        auto result = benchmarkDefragment(10, std::chrono::milliseconds(20));
        total.first += result.first;
        total.second += result.second;
    }
    state.counters["ItemsPerSec"] =
            total.first / std::chrono::duration<double>(total.second).count();
}

BENCHMARK_REGISTER_F(DefragmentBench, Visit)->Range(0,1);
BENCHMARK_REGISTER_F(DefragmentBench, DefragAlways)->Range(0,1);
BENCHMARK_REGISTER_F(DefragmentBench, DefragAge10)->Range(0,1);
BENCHMARK_REGISTER_F(DefragmentBench, DefragAge10_20ms)->Range(0,1);

