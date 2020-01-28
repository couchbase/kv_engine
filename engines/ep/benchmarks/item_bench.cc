/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
 * Benchmarks relating to the Item class.
 */

#include "item.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <random>

static void BM_CompareQueuedItemsBySeqnoAndKey(benchmark::State& state) {
    std::vector<queued_item> items;
    // Populate with N items with distinct keys
    for (size_t i = 0; i < 10000; i++) {
        auto key = std::string("key_") + std::to_string(i);
        items.emplace_back(new Item(
                DocKey(key, DocKeyEncodesCollectionId::No), {}, {}, "data", 4));
    }
    OrderItemsForDeDuplication cq;
    std::random_device rd;
    std::mt19937 g(rd());
    while (state.KeepRunning()) {
        // shuffle (while timing paused)
        state.PauseTiming();
        std::shuffle(items.begin(), items.end(), g);
        state.ResumeTiming();

        // benchmark
        std::sort(items.begin(), items.end(), cq);
    }
}
// Register the function as a benchmark
BENCHMARK(BM_CompareQueuedItemsBySeqnoAndKey);
