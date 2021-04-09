/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
