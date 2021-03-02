/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#include "dcp/producer.h"
#include <benchmark/benchmark.h>
#include <folly/portability/GMock.h>

class DcpProducerStreamsMapBench : public ::benchmark::Fixture {};

/**
 * Benchmark performance of find() DcpProducer SteamsMap find(), implemented
 * using folly::AtomicHashMap of estimated final size 512. Varying number of
 * elements in map.
 * Performance is very good (~single digit ns) when size() <= estimated final
 * size, espcially given zero collisions with a uint16_t key and identity hash
 * function, but degrades rapidly if size exeeeds it, due to degrading to linear
 * scan of each submap.
 */
BENCHMARK_DEFINE_F(DcpProducerStreamsMapBench, findMap)
(benchmark::State& state) {
    using StreamsMap =
            folly::AtomicHashMap<uint16_t, DcpProducer::StreamMapValue>;
    StreamsMap streams{512};
    std::vector<uint16_t> vbs;
    for (uint16_t vb = 0; vb < state.range(); vb++) {
        streams.insert({vb, {}});
        vbs.push_back(vb);
    }

    auto it = vbs.begin();
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(streams.find(*it++));
        if (it == vbs.end()) {
            it = vbs.begin();
        }
    }
    state.counters["numSubMaps"] = streams.numSubMaps();
    state.counters["capacity"] = streams.capacity();
}

/**
 * Benchmark performance of find() DcpProducer SteamsMap find(), implemented
 * using folly::AtomicHashArray of fixed size 1204.
 * Varying number of elements in map.
 */
BENCHMARK_DEFINE_F(DcpProducerStreamsMapBench, findArray)
(benchmark::State& state) {
    using StreamsMap =
            folly::AtomicHashArray<uint16_t, DcpProducer::StreamMapValue>;

    StreamsMap::Config config;
    config.maxLoadFactor = 1.0;
    auto streams = StreamsMap::create(1024, config);
    std::vector<uint16_t> vbs;
    for (uint16_t vb = 0; vb < state.range(); vb++) {
        streams->insert({vb, {}});
        vbs.push_back(vb);
    }

    auto it = vbs.begin();
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(streams->find(*it++));
        if (it == vbs.end()) {
            it = vbs.begin();
        }
    }
    state.counters["capacity"] = streams->capacity_;
}

BENCHMARK_REGISTER_F(DcpProducerStreamsMapBench, findMap)
        ->DenseRange(128, 1024, 128);

BENCHMARK_REGISTER_F(DcpProducerStreamsMapBench, findArray)
        ->DenseRange(128, 1024, 128);
