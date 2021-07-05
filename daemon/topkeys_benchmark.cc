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

#include <benchmark/benchmark.h>
#include <daemon/settings.h>
#include <daemon/topkeys.h>
#include <algorithm>
#include <iostream>
#include <memory>
#include <random>

/**
 * A fixture for performing topkeys updates
 */
class TopkeysBench : public benchmark::Fixture {
protected:
    TopkeysBench() {
        topkeys = std::make_unique<TopKeys>(50);
        Settings::instance().setTopkeysEnabled(true);
        for (int ii = 0; ii < 10000; ii++) {
            keys.emplace_back("topkey_test_" + std::to_string(ii));
        }
    }

    std::vector<std::string> keys;
    std::unique_ptr<TopKeys> topkeys;
};

/**
 * Benchmark the overhead of calling topkeys when the code is disabled
 */
BENCHMARK_DEFINE_F(TopkeysBench, TopkeysDisabled)(benchmark::State& state) {
    if (state.thread_index == 0) {
        Settings::instance().setTopkeysEnabled(false);
    }

    const auto* k = keys[0].data();
    const auto l = keys[0].size();

    while (state.KeepRunning()) {
        topkeys->updateKey(k, l, 10);
        ::benchmark::ClobberMemory();
    }
}

/**
 * Benchmark the code when all threads is updating the _same_ key
 */
BENCHMARK_DEFINE_F(TopkeysBench, UpdateSameKey)(benchmark::State& state) {
    if (state.thread_index == 0) {
        Settings::instance().setTopkeysEnabled(true);
    }

    const auto* k = keys[0].data();
    const auto l = keys[0].size();

    while (state.KeepRunning()) {
        topkeys->updateKey(k, l, 10);
        ::benchmark::ClobberMemory();
    }
}

/**
 * Benchmark the code when we're looping updating "random" keys.
 */
BENCHMARK_DEFINE_F(TopkeysBench, UpdateRandomKey)(benchmark::State& state) {
    if (state.thread_index == 0) {
        Settings::instance().setTopkeysEnabled(true);
    }

    std::vector<std::string> mine;
    std::copy(keys.begin(), keys.end(), std::back_inserter(mine));
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(mine.begin(), mine.end(), g);

    size_t start = 0;
    const auto size = keys.size();

    while (state.KeepRunning()) {
        const auto& element = mine[start++ % size];
        const auto* k = element.data();
        const auto l = element.size();
        topkeys->updateKey(k, l, 10);
        ::benchmark::ClobberMemory();
    }
}

BENCHMARK_REGISTER_F(TopkeysBench, TopkeysDisabled)->Threads(8)->Threads(24);
BENCHMARK_REGISTER_F(TopkeysBench, UpdateSameKey)->Threads(8)->Threads(24);
BENCHMARK_REGISTER_F(TopkeysBench, UpdateRandomKey)->Threads(8)->Threads(24);

BENCHMARK_MAIN();
