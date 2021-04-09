/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
