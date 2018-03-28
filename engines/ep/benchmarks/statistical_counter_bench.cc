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
 * Benchmarks relating to the StatisticalCounter class.
 */

#include "statistical_counter.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <iostream>

/**
 * Define the increment factor for the statisticalCounter being used for
 * the tests. 0.012 allows an 8-bit StatisticalCounter to mimic a uint16
 * counter.
 */
static const double incFactor = 0.012;

StatisticalCounter<uint8_t> statisticalCounter(incFactor);
uint8_t counter{100}; // 100 is an arbitrary value between 0 and 255

static void BM_SaturateCounter(benchmark::State& state) {
    while (state.KeepRunning()) {
        // benchmark generateValue
        statisticalCounter.generateValue(counter);
    }
}

// Single-threaded version of the benchmark
BENCHMARK(BM_SaturateCounter)->Threads(1);
// Multi-threaded version of the benchmark
BENCHMARK(BM_SaturateCounter)->Threads(8);
