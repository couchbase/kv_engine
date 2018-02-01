/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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
 * Benchmark memory accounting
 */

#include <benchmark/benchmark.h>

#include <platform/sysinfo.h>

#include "stats.h"

class TestEPStats : public EPStats {
public:
    /// update the merge threshold
    void setMemUsedMergeThreshold(size_t value) {
        memUsedMergeThreshold = value;
    }

    // Special version for the benchmark which ensures memory tracking begins
    // at zero for each KeepRunning iteration, ensuring consistent entry state.
    void memAllocatedClear(size_t sz) {
        auto& coreMemory = coreTotalMemory.get();
        coreMemory->store(0);
        auto value = coreMemory->fetch_add(sz) + sz;

        maybeUpdateEstimatedTotalMemUsed(*coreMemory, value);
    }
};

class MemoryAllocationStat : public benchmark::Fixture {
public:
    TestEPStats stats;
};

BENCHMARK_DEFINE_F(MemoryAllocationStat, AllocNRead1)(benchmark::State& state) {
    if (state.thread_index == 0) {
        stats.reset();
        stats.memoryTrackerEnabled = true;
        // memUsed merge must be 4 times higher so in theory we merge at the
        // same rate as TLS (because 4 more threads than cores).
        stats.setMemUsedMergeThreshold(10240 * 4);
    }

    while (state.KeepRunning()) {
        // range = allocations per read
        for (int i = 0; i < state.range(0); i++) {
            if (i == 0) {
                stats.memAllocatedClear(128);
            } else {
                stats.memAllocated(128);
            }
        }
        stats.getEstimatedTotalMemoryUsed();
    }
}

BENCHMARK_DEFINE_F(MemoryAllocationStat, AllocNReadM)(benchmark::State& state) {
    if (state.thread_index == 0) {
        stats.reset();
        stats.memoryTrackerEnabled = true;
        // memUsed merge must be 4 times higher so in theory we merge at the
        // same rate as TLS (because 4 more threads than cores).
        stats.setMemUsedMergeThreshold(10240 * 4);
    }

    while (state.KeepRunning()) {
        // range = allocations per read
        for (int i = 0; i < state.range(0); i++) {
            if (i == 0) {
                stats.memAllocatedClear(128);
            } else {
                stats.memAllocated(128);
            }
        }
        for (int j = 0; j < state.range(1); j++) {
            stats.getEstimatedTotalMemoryUsed();
        }
    }
}

// Test covers a range seen from a running cluster (with pillowfight load)
// The range was discovered by counting calls to memAllocated/deallocated and
// then logging how many had occurred for each read
// (getEstimatedTotalMemoryUsed)
BENCHMARK_REGISTER_F(MemoryAllocationStat, AllocNRead1)
        ->Threads(cb::get_cpu_count() * 4)
        ->RangeMultiplier(2)
        ->Range(0, 4000);

BENCHMARK_REGISTER_F(MemoryAllocationStat, AllocNReadM)
        ->Threads(cb::get_cpu_count() * 4)
        ->RangeMultiplier(2)
        ->Ranges({{0, 4000}, {128, 4000}});