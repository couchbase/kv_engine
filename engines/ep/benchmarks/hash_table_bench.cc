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

#include "configuration.h"
#include "hash_table.h"
#include "item.h"
#include "module_tests/test_helpers.h"
#include "stats.h"
#include "stored_value_factories.h"

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

// Benchmarks inserting items into a HashTable
class HashTableBench : public benchmark::Fixture {
public:
    HashTableBench()
        : ht(stats,
             std::make_unique<StoredValueFactory>(stats),
             Configuration().getHtSize(),
             Configuration().getHtLocks()) {
    }

    void SetUp(benchmark::State& state) {
        if (state.thread_index == 0) {
            ht.resize(numItems);
        }
    }

    void TearDown(benchmark::State& state) {
        if (state.thread_index == 0) {
            ht.clear();
        }
    }

    std::vector<Item> createItems(std::string prefix) {
        std::vector<Item> items;
        items.reserve(numItems);
        const size_t itemSize = 256;
        const auto data = std::string(itemSize, 'x');
        for (size_t i = 0; i < numItems; i++) {
            auto key = makeStoredDocKey(prefix + std::to_string(i));
            items.emplace_back(key, 0, 0, data.data(), data.size());
        }

        return items;
    }

    EPStats stats;
    HashTable ht;
    const size_t numItems = 10000;
    /// Shared vector of items for tests which want to use the same
    /// data across multiple threads.
    std::vector<Item> sharedItems;
};

// Benchmark finding items in the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, Find)(benchmark::State& state) {
    // Populate the HashTable with numItems.
    if (state.thread_index == 0) {
        sharedItems = createItems("benchmark_thread_" +
                                  std::to_string(state.thread_index) + "::");
        for (auto& item : sharedItems) {
            ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
        }
    }

    // Benchmark - find them.
    size_t iteration = 0;
    while (state.KeepRunning()) {
        auto& key = sharedItems[iteration++ % numItems].getKey();
        benchmark::DoNotOptimize(ht.findForRead(key));
    }
}

// Benchmark inserting an item into the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, Insert)(benchmark::State& state) {
    size_t iteration = 0;
    auto items = createItems("benchmark_thread_" +
                             std::to_string(state.thread_index) + "::");

    while (state.KeepRunning()) {
        // To ensure consistent results; clear the HashTable every numItems
        // iterations to maintain a constant load factor.
        if (iteration == numItems) {
            state.PauseTiming();
            iteration = 0;
            ht.clear();
            state.ResumeTiming();
        }

        ASSERT_EQ(MutationStatus::WasClean, ht.set(items[iteration++]));
    }
}

// Benchmark replacing an existing item in the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, Replace)(benchmark::State& state) {
    // Populate the HashTable with numItems.
    auto items = createItems("benchmark_thread_" +
                             std::to_string(state.thread_index) + "::");
    for (auto& item : items) {
        ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
    }

    // Benchmark - update them.
    size_t iteration = 0;
    while (state.KeepRunning()) {
        ASSERT_EQ(MutationStatus::WasDirty,
                  ht.set(items[iteration++ % numItems]));
    }
}

BENCHMARK_DEFINE_F(HashTableBench, Delete)(benchmark::State& state) {
    auto items = createItems("benchmark_thread_" +
                             std::to_string(state.thread_index) + "::");

    size_t iteration = numItems;
    while (state.KeepRunning()) {
        // Re-populate the HashTable every numItems iterations
        if (iteration == numItems) {
            state.PauseTiming();
            for (auto& item : items) {
                ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
            }
            iteration = 0;
            state.ResumeTiming();
        }

        auto& key = items[iteration++ % numItems].getKey();
        {
            auto result = ht.findForWrite(key);
            ASSERT_TRUE(result.storedValue);
            ht.unlocked_del(result.lock, key);
        }
    }
}

BENCHMARK_REGISTER_F(HashTableBench, Find)->ThreadPerCpu();
BENCHMARK_REGISTER_F(HashTableBench, Insert)->ThreadPerCpu();
BENCHMARK_REGISTER_F(HashTableBench, Replace)->ThreadPerCpu();
BENCHMARK_REGISTER_F(HashTableBench, Delete)->ThreadPerCpu();
