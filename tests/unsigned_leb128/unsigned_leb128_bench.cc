/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/protocol/unsigned_leb128.h>
#include <array>

#include <benchmark/benchmark.h>

// Create input for the test, a leb128 prefixed 'key'
static std::vector<uint8_t> makeLebPrefixedBuffer(uint32_t prefix,
                                                  size_t keylen) {
    cb::mcbp::unsigned_leb128<uint32_t> leb(prefix);
    std::vector<uint8_t> buffer;
    for (auto c : leb) {
        buffer.push_back(c);
    }
    for (size_t ii = 0; ii < keylen; ii++) {
        buffer.push_back('k');
    }
    return buffer;
}

// For the given size return the range that can be encoded in that size
std::pair<uint32_t, uint32_t> getTestRange(size_t encodedBytes) {
    switch (encodedBytes) {
    case 1:
        return {0, 127};
    case 2:
        return {128, 16383};
    case 3:
        return {16384, 2097151};
    case 4:
        return {2097152, 268435455};
    case 5:
        return {268435456, 4294967295};
    default:
        throw std::invalid_argument("getTestRange invalid argument");
    }
}

constexpr size_t numberOfInputs = 1000;

static void bench_unsigned_leb128_decode(benchmark::State& state) {
    auto range = getTestRange(state.range(0));
    std::array<std::vector<uint8_t>, numberOfInputs> buffers;
    size_t value = range.first;
    for (auto& b : buffers) {
        b = makeLebPrefixedBuffer(value, state.range(1));
        if (value == range.second) {
            value = range.first;
        } else {
            value++;
        }
    }

    auto itr = buffers.begin();
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(
                cb::mcbp::unsigned_leb128<uint32_t>::decode({*itr}));
        itr++;
        if (itr == buffers.end()) {
            itr = buffers.begin();
        }
    }
}

static void bench_unsigned_leb128_decodeCanonical(benchmark::State& state) {
    auto range = getTestRange(state.range(0));
    std::array<std::vector<uint8_t>, numberOfInputs> buffers;
    size_t value = range.first;
    for (auto& b : buffers) {
        b = makeLebPrefixedBuffer(value, state.range(1));
        if (value == range.second) {
            value = range.first;
        } else {
            value++;
        }
    }

    auto itr = buffers.begin();
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(
                cb::mcbp::unsigned_leb128<uint32_t>::decodeCanonical({*itr}));
        itr++;
        if (itr == buffers.end()) {
            itr = buffers.begin();
        }
    }
}

static void generateBenchmarkArguments(benchmark::internal::Benchmark* b) {
    // Test inputs.
    std::array<int, 5> lebSizes = {{1, 2, 3, 4, 5}};
    for (auto size : lebSizes) {
        // Second input is the 'keylen', how many bytes are in the buffer after
        // the leb128 encoded integer. For now keep the second argument fixed as
        // 0 as it doesn't affect the current decode - we may in the future want
        // to change the key len
        b->Args({size, 0});
    }
}

BENCHMARK(bench_unsigned_leb128_decode)->Apply(generateBenchmarkArguments);
BENCHMARK(bench_unsigned_leb128_decodeCanonical)
        ->Apply(generateBenchmarkArguments);

