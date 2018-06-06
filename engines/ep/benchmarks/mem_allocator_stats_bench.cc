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
        auto& coreMemory = coreLocal.get()->totalMemory;
        coreMemory.store(0);
        auto value = coreMemory.fetch_add(sz) + sz;

        maybeUpdateEstimatedTotalMemUsed(coreMemory, value);
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

BENCHMARK_DEFINE_F(MemoryAllocationStat, AllocNReadPreciseM)
(benchmark::State& state) {
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
            stats.getPreciseTotalMemoryUsed();
        }
    }
}

// Tests cover a rough, but realistic range seen from a running cluster (with
// pillowfight load). The range was discovered by counting calls to
// memAllocated/deallocated and then logging how many had occurred for each
// getEstimatedTotalMemoryUsed. A previous version of this file used the Range
// API and can be used if this test is being used to perform deeper analysis of
// this code.
BENCHMARK_REGISTER_F(MemoryAllocationStat, AllocNRead1)
        ->Threads(cb::get_cpu_count() * 4)
        ->Args({0})
        ->Args({200})
        ->Args({1000});

BENCHMARK_REGISTER_F(MemoryAllocationStat, AllocNReadM)
        ->Threads(cb::get_cpu_count() * 4)
        ->Args({0, 10})
        ->Args({200, 10})
        ->Args({1000, 10})
        ->Args({0, 1000})
        ->Args({200, 200})
        ->Args({1000, 10});

BENCHMARK_REGISTER_F(MemoryAllocationStat, AllocNReadPreciseM)
        ->Threads(cb::get_cpu_count() * 4)
        // This benchmark is configured to run 'alloc heavy'. The getPrecise
        // function is only used by getStats, which is infrequent relative to
        // memory alloc/dealloc
        ->Args({1000, 10})
        ->Args({100000, 10});
