/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Benchmarks relating to the ProbabilisticCounter class.
 */

#include "probabilistic_counter.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <iostream>

/**
 * Define the increment factor for the ProbabilisticCounter being used for
 * the tests. 0.012 allows an 8-bit ProbabilisticCounter to mimic a uint16
 * counter.
 */
static const double incFactor = 0.012;

ProbabilisticCounter<uint8_t> probabilisticCounter(incFactor);
uint8_t counter{100}; // 100 is an arbitrary value between 0 and 255

static void BM_SaturateCounter(benchmark::State& state) {
    while (state.KeepRunning()) {
        // benchmark generateValue
        benchmark::DoNotOptimize(probabilisticCounter.generateValue(counter));
    }
}

// Single-threaded version of the benchmark
BENCHMARK(BM_SaturateCounter)->Threads(1);
// Multi-threaded version of the benchmark
BENCHMARK(BM_SaturateCounter)->Threads(8);
