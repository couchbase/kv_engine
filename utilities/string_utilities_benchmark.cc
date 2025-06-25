/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "utilities/string_utilities.h"
#include <benchmark/benchmark.h>
#include <platform/byte_literals.h>

std::string payload;

static void stage_and_commit(benchmark::State& state) {
    std::string_view view(payload.data(), state.range(0));
    while (state.KeepRunning()) {
        const auto decoded = base64_decode_value(base64_encode_value(view));
        if (decoded.size() != view.size()) {
            std::abort();
        }
    }
}

BENCHMARK(stage_and_commit)->RangeMultiplier(2)->Range(64, 1_MiB);

int main(int argc, char** argv) {
    payload.resize(1_MiB, 'a');

    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
    // connection.reset();
    std::exit(EXIT_SUCCESS);
}
