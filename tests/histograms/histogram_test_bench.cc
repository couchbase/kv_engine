/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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
 * Benchmark tests to compare TimingHistogram against HdrHistogram
 * */

#include <benchmark/benchmark.h>
#include <daemon/timing_histogram.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <utilities/hdrhistogram.h>
#include <random>

#define LOG_NORMAL_MEAN 0
#define LOG_NORMAL_STD 2.0
#define LOG_NORMAL_SCALE_UP_MULT 35000
#define LOG_NORMAL_MIN 50000

class HdrHistogramEmpty : public HdrHistogram {
public:
    HdrHistogramEmpty() : HdrHistogram(0, 1, 1){};
};

class HdrHistogramBench : public HdrHistogram {
public:
    HdrHistogramBench() : HdrHistogram(0, 60000000, 2){};
};

template <typename T>
void HistoAddNs(T& histo, std::chrono::nanoseconds v);

template <>
void HistoAddNs(HdrHistogram& histo, std::chrono::nanoseconds v) {
    histo.addValue(v.count());
}

template <>
void HistoAddNs(HdrHistogramBench& histo, std::chrono::nanoseconds v) {
    HistoAddNs(static_cast<HdrHistogram&>(histo), v);
}
template <>
void HistoAddNs(HdrHistogramEmpty& histo, std::chrono::nanoseconds v) {
    HistoAddNs(static_cast<HdrHistogram&>(histo), v);
}

template <>
void HistoAddNs(TimingHistogram& histo, std::chrono::nanoseconds v) {
    histo.add(v);
}

template <typename T>
T LinearFillHistogram(benchmark::State& state) {
    using namespace std::chrono;
    T testHisto;
    for (int i = 0; i < state.range(0); i++) {
        HistoAddNs<T>(testHisto, static_cast<nanoseconds>(i));
    }
    return testHisto;
}

// static function to return a log normal value scaled by
// LOG_NORMAL_SCALE_UP_MULT. It creates an array of 10000 static values that
// using std::lognormal_distribution and returners them in an incrementing
// linear fashion so that they can be used in the Add benchmarks.
static uint64_t GetNextLogNormalValue() {
    static bool initialised = false;
    static std::vector<uint64_t> valuesToAdd(10000);
    static unsigned int i = 0;

    if (!initialised) {
        // create a log normal distribution and random number generator
        // so we can add random values in a log normal distribution which is a
        // better representation of a production environment
        std::random_device randomDevice;
        std::mt19937 randomNumGen(randomDevice());
        std::lognormal_distribution<long double> distribution(LOG_NORMAL_MEAN,
                                                              LOG_NORMAL_STD);
        // We have denormalize the log normal distribution with a min
        // changing from 0 to 50000ns the max should remain at inf and set
        // the mean to about 84000ns.
        // Percentile values will vary as we use a random number generator to
        // seed a X value when getting values from the distribution. However,
        // the values below should give an idea of the distribution which
        // modelled around an "ADD" op from stats.log p50:~84000ns |
        // p90:~489000ns |p99:3424000ns |p99.9:20185000ns | p99.99:41418000ns
        for (auto& currentVal : valuesToAdd) {
            auto valToAdd = static_cast<uint64_t>(
                    LOG_NORMAL_MIN + std::round(distribution(randomNumGen) *
                                                LOG_NORMAL_SCALE_UP_MULT));
            currentVal = valToAdd;
        }
        initialised = true;
    }

    if (i >= valuesToAdd.size()) {
        i = 0;
    }

    return valuesToAdd[i++];
}

template <typename T>
void HistogramConstructionDestructionHeap(benchmark::State& state) {
    using namespace std::chrono;
    nanoseconds testDuration(0);
    while (state.KeepRunning()) {
        std::unique_ptr<T> testHisto(new T());
        HistoAddNs(*testHisto.get(), testDuration);
    }
}

void HdrVariantSizeConstructionDestructionHeap(benchmark::State& state) {
    using namespace std::chrono;
    nanoseconds testDuration(0);
    while (state.KeepRunning()) {
        std::unique_ptr<HdrHistogram> testHisto(
                new HdrHistogram(0, static_cast<uint64_t>(state.range(0)), 2));
        HistoAddNs<HdrHistogram>(*testHisto.get(), testDuration);
    }
}

template <typename T>
void HistogramConstructionDestructionStack(benchmark::State& state) {
    using namespace std::chrono;
    nanoseconds testDuration(0);
    while (state.KeepRunning()) {
        T testHisto{};
        HistoAddNs<T>(testHisto, testDuration);
    }
}

void HdrVariantSizeConstructionDestructionStack(benchmark::State& state) {
    using namespace std::chrono;
    nanoseconds testDuration(0);
    while (state.KeepRunning()) {
        HdrHistogram testHisto{0, static_cast<uint64_t>(state.range(0)), 2};
        HistoAddNs<HdrHistogram>(testHisto, testDuration);
    }
}

template <typename T>
static void HistogramAdd(benchmark::State& state) {
    using namespace std::chrono;
    T testHisto;

    if (state.thread_index == 0) {
        // make sure the vector of values is set up
        GetNextLogNormalValue();
    }

    while (state.KeepRunning()) {
        HistoAddNs<T>(testHisto, nanoseconds(GetNextLogNormalValue()));
    }
}

template <typename T>
void HistogramToString(benchmark::State& state) {
    T testHisto = LinearFillHistogram<T>(state);
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(testHisto.to_string());
    }
}

template <typename T>
void HistogramReset(benchmark::State& state) {
    T testHisto{};
    while (state.KeepRunning()) {
        testHisto.reset();
    }
}

template <typename T>
void HistogramAggregation(benchmark::State& state) {
    using namespace std::chrono;
    T testHisto1 = LinearFillHistogram<T>(state);
    T testHisto2 = LinearFillHistogram<T>(state);
    while (state.KeepRunning()) {
        testHisto1 += testHisto2;
    }
}

template <typename T>
void HistogramToJson(benchmark::State& state) {
    T testHisto = LinearFillHistogram<T>(state);
    while (state.KeepRunning()) {
        auto json = testHisto.to_json();
        benchmark::DoNotOptimize(json);
    }
}

BENCHMARK_TEMPLATE(HistogramConstructionDestructionHeap, TimingHistogram);
BENCHMARK_TEMPLATE(HistogramConstructionDestructionHeap, HdrHistogramBench);
BENCHMARK_TEMPLATE(HistogramConstructionDestructionHeap, HdrHistogramEmpty);

BENCHMARK_TEMPLATE(HistogramConstructionDestructionStack, TimingHistogram);
BENCHMARK_TEMPLATE(HistogramConstructionDestructionStack, HdrHistogramBench);
BENCHMARK_TEMPLATE(HistogramConstructionDestructionStack, HdrHistogramEmpty);

BENCHMARK_TEMPLATE(HistogramAdd, TimingHistogram)
        ->Threads(4)
        ->Arg(1000)
        ->UseRealTime();
BENCHMARK_TEMPLATE(HistogramAdd, TimingHistogram)
        ->Threads(1)
        ->Arg(1000)
        ->UseRealTime();
BENCHMARK_TEMPLATE(HistogramAdd, HdrHistogramBench)
        ->Threads(4)
        ->Arg(1000)
        ->UseRealTime();
BENCHMARK_TEMPLATE(HistogramAdd, HdrHistogramBench)
        ->Threads(1)
        ->Arg(1000)
        ->UseRealTime();

BENCHMARK_TEMPLATE(HistogramToString, TimingHistogram)->Arg(10000);
BENCHMARK_TEMPLATE(HistogramToString, HdrHistogramBench)->Arg(10000);
BENCHMARK_TEMPLATE(HistogramToString, HdrHistogramEmpty)->Arg(10000);

BENCHMARK_TEMPLATE(HistogramReset, TimingHistogram);
BENCHMARK_TEMPLATE(HistogramReset, HdrHistogramBench);
BENCHMARK_TEMPLATE(HistogramReset, HdrHistogramEmpty);

BENCHMARK_TEMPLATE(HistogramAggregation, TimingHistogram)->Arg(100);
BENCHMARK_TEMPLATE(HistogramAggregation, HdrHistogramBench)->Arg(100);

BENCHMARK_MAIN();
