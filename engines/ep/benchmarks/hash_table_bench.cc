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
#include <engines/ep/src/syncobject.h>
#include <folly/portability/GTest.h>
#include <spdlog/fmt/fmt.h>

// Benchmarks inserting items into a HashTable
class HashTableBench : public benchmark::Fixture {
public:
    HashTableBench()
        : ht(stats,
             std::make_unique<StoredValueFactory>(stats),
             Configuration().getHtSize(),
             Configuration().getHtLocks()) {
    }

    void SetUp(benchmark::State& state) override {
        if (state.thread_index == 0) {
            ht.resize(numItems);
        }
    }

    void TearDown(benchmark::State& state) override {
        if (state.thread_index == 0) {
            ht.clear();
        }
    }

    /**
     * Create numItems Items, giving each key the given prefix.
     * @param prefix String to prefix each key with.
     * @param pendingSyncWritesPcnt If non-zero, create SyncWrites for the given
     *   percentage. For example a value of 20 will create the 20% of numItems
     *   of Prepared SyncWrites.
     * @param collections list of collections to create items in, uniformly
     * distributed
     */
    std::vector<Item> createUniqueItems(
            const std::string& prefix,
            int pendingSyncWritesPcnt = 0,
            const std::vector<CollectionID>& collections = {
                    CollectionID::Default}) {
        std::vector<Item> items;
        items.reserve(numItems);
        // Just use a minimal item (Blob) size - we are focusing on
        // benchmarking the HashTable's methods, don't really care about
        // cost of creating Item / StoredValue objects here.
        const size_t itemSize = 1;
        const auto data = std::string(itemSize, 'x');
        for (size_t i = 0; i < numItems; i++) {
            // Use fmtlib to format key with stack-local (non-heap) buffer to
            // minimise the cost of constructing keys for Items.
            fmt::memory_buffer keyBuf;
            format_to(keyBuf, "{}{}", prefix, i);
            // Note: fmt::memory_buffer is not null-terminated, cannot use the
            // cstring-ctor
            const auto& collection = collections.at(i % collections.size());

            StoredDocKey key(to_string(keyBuf), collection);
            items.emplace_back(key, 0, 0, data.data(), data.size());

            if (pendingSyncWritesPcnt > 0) {
                if (i % (100 / pendingSyncWritesPcnt) == 0) {
                    items.back().setPendingSyncWrite({});
                }
            }
        }

        return items;
    }

    /**
     * Helper method for executing a function with all threads paused.
     *
     * Expected usage is to call this method from all threads running a
     * benchmark, specifying a function to be called once all threads have been
     * stopped.
     *
     * Function will make the first `state.threads - 1` threads block on a
     * condvar, the last thread to call it will call the given function
     * then wake up all waiting threads.
     */
    void waitForAllThreadsThenExecuteOnce(benchmark::State& state,
                                          std::function<void()> func) {
        std::unique_lock<std::mutex> lock(mutex);
        if (++waiters < state.threads) {
            // Last thread to enter - execute the given function.
            func();
            waiters = 0;
            syncObject.notify_all();
        } else {
            // Not yet the last thread - wait for the last guy to do the
            // work.
            syncObject.wait(lock, [this]() { return waiters == 0; });
        }
    }

    auto& getValFact() {
        return ht.valFact;
    }

    auto& getValueStats() {
        return ht.valueStats;
    }

    EPStats stats;
    HashTable ht;
    static const size_t numItems = 100000;
    /// Shared vector of items for tests which want to use the same
    /// data across multiple threads.
    std::vector<Item> sharedItems;
    // Shared synchronization object and mutex, needed by some benchmarks to
    // coordinate their execution phases.
    std::mutex mutex;
    SyncObject syncObject;
    int waiters = 0;
};

// Benchmark finding items in the HashTable.
// Includes extra  50% of Items are prepared SyncWrites -  an unrealistically
// high percentage in a real-world, but want to measure any performance impact
// in having such items present in the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, FindForRead)(benchmark::State& state) {
    // Populate the HashTable with numItems.
    if (state.thread_index == 0) {
        sharedItems = createUniqueItems(
                "Thread" + std::to_string(state.thread_index) + "::", 50);
        for (auto& item : sharedItems) {
            ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
        }
    }

    // Benchmark - find them.
    while (state.KeepRunning()) {
        auto& key = sharedItems[state.iterations() % numItems].getKey();
        benchmark::DoNotOptimize(ht.findForRead(key));
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark finding items (for write) in the HashTable.
// Includes extra  50% of Items are prepared SyncWrites -  an unrealistically
// high percentage in a real-world, but want to measure any performance impact
// in having such items present in the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, FindForWrite)(benchmark::State& state) {
    // Populate the HashTable with numItems.
    if (state.thread_index == 0) {
        sharedItems = createUniqueItems(
                "Thread" + std::to_string(state.thread_index) + "::", 50);
        for (auto& item : sharedItems) {
            ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
        }
    }

    // Benchmark - find them.
    while (state.KeepRunning()) {
        auto& key = sharedItems[state.iterations() % numItems].getKey();
        benchmark::DoNotOptimize(ht.findForWrite(key));
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark inserting an item into the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, Insert)(benchmark::State& state) {
    // To ensure we insert and not replace items, create a per-thread items
    // vector so each thread inserts a different set of items.
    auto items = createUniqueItems("Thread" +
                                   std::to_string(state.thread_index) + "::");

    while (state.KeepRunning()) {
        const auto index = state.iterations() % numItems;
        ASSERT_EQ(MutationStatus::WasClean, ht.set(items[index]));

        // Once a thread gets to the end of it's items; pause timing and let
        // the *last* thread clear them all - this is to avoid measuring any
        // of the ht.clear() cost indirectly when other threads are trying to
        // insert.
        // Note: state.iterations() starts at 0; hence checking for
        // state.iterations() % numItems (aka 'index') is zero to represent we
        // wrapped.
        if (index == 0) {
            state.PauseTiming();
            waitForAllThreadsThenExecuteOnce(state, [this]() { ht.clear(); });
            state.ResumeTiming();
        }
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark replacing an existing item in the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, Replace)(benchmark::State& state) {
    // Populate the HashTable with numItems.
    auto items = createUniqueItems("Thread" +
                                   std::to_string(state.thread_index) + "::");
    for (auto& item : items) {
        ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
    }

    // Benchmark - update them.
    while (state.KeepRunning()) {
        ASSERT_EQ(MutationStatus::WasDirty,
                  ht.set(items[state.iterations() % numItems]));
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(HashTableBench, Delete)(benchmark::State& state) {
    auto items = createUniqueItems("Thread" +
                                   std::to_string(state.thread_index) + "::");

    while (state.KeepRunning()) {
        const auto index = state.iterations() % numItems;

        // Populate the HashTable every numItems iterations.
        //
        // Once a thread deletes all of it's items; pause timing and let
        // the *last* thread re-populate the HashTable (so we can continue to
        // delete)
        // - this is to avoid measuring any of the re-populate cost while
        // other threads are trying to delete.
        if (index == 1) {
            state.PauseTiming();
            waitForAllThreadsThenExecuteOnce(state, [this, &items]() {
                // re-populate HashTable.
                for (auto& item : items) {
                    ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
                }
            });
            state.ResumeTiming();
        }

        auto& key = items[index].getKey();
        {
            auto result = ht.findForWrite(key);
            ASSERT_TRUE(result.storedValue);
            ht.unlocked_del(result.lock, result.storedValue);
        }
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark inserting an item into the HashTable.
BENCHMARK_DEFINE_F(HashTableBench, MultiCollectionInsert)
(benchmark::State& state) {
    // To ensure we insert and not replace items, create a per-thread items
    // vector so each thread inserts a different set of items.

    const size_t numCollections = state.range(0);

    std::vector<CollectionID> collections;

    CollectionIDType counter = CollectionID::Default;
    while (collections.size() < numCollections) {
        if (!CollectionID::isReserved(counter)) {
            collections.emplace_back(counter);
            stats.trackCollectionStats(counter);
        }
        ++counter;
    }

    auto items = createUniqueItems(
            "Thread" + std::to_string(state.thread_index) + "::",
            0,
            collections);

    while (state.KeepRunning()) {
        const auto index = state.iterations() % numItems;
        ASSERT_EQ(MutationStatus::WasClean, ht.set(items[index]));

        // Once a thread gets to the end of it's items; pause timing and let
        // the *last* thread clear them all - this is to avoid measuring any
        // of the ht.clear() cost indirectly when other threads are trying to
        // insert.
        // Note: state.iterations() starts at 0; hence checking for
        // state.iterations() % numItems (aka 'index') is zero to represent we
        // wrapped.
        if (index == 0) {
            state.PauseTiming();
            waitForAllThreadsThenExecuteOnce(state, [this]() { ht.clear(); });
            state.ResumeTiming();
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(HashTableBench, HTStatsEpilogue)(benchmark::State& state) {
    // To ensure we insert and not replace items, create a per-thread items
    // vector so each thread inserts a different set of items.

    const size_t numCollections = state.range(0);

    std::vector<CollectionID> collections;

    CollectionIDType counter = CollectionID::Default;
    while (collections.size() < numCollections) {
        if (!CollectionID::isReserved(counter)) {
            collections.emplace_back(counter);
            stats.trackCollectionStats(counter);
        }
        ++counter;
    }

    auto items = createUniqueItems(
            "Thread" + std::to_string(state.thread_index) + "::",
            0,
            collections);

    auto& valFact = getValFact();
    auto& valueStats = getValueStats();

    std::vector<StoredValue::UniquePtr> values;
    values.reserve(items.size());

    for (const auto& item : items) {
        values.emplace_back((*valFact)(item, nullptr));
    }

    while (state.KeepRunning()) {
        const auto index = state.iterations() % numItems;

        auto empty = valueStats.prologue(nullptr);
        valueStats.epilogue(empty, values[index].get().get());

        // Once a thread gets to the end of it's items; pause timing and let
        // the *last* thread clear them all - this is to avoid measuring any
        // of the ht.clear() cost indirectly when other threads are trying to
        // insert.
        // Note: state.iterations() starts at 0; hence checking for
        // state.iterations() % numItems (aka 'index') is zero to represent we
        // wrapped.
        if (index == 0) {
            state.PauseTiming();
            waitForAllThreadsThenExecuteOnce(state, [this]() { ht.clear(); });
            state.ResumeTiming();
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(HashTableBench, Clear)(benchmark::State& state) {
    // Generate numItems to add to (and then clear from) HashTable.
    sharedItems = createUniqueItems("Key");

    // Benchmark - measure how long it takes to clear the HashTable.
    while (state.KeepRunning()) {
        state.PauseTiming();
        for (auto& item : sharedItems) {
            ASSERT_EQ(MutationStatus::WasClean, ht.set(item));
        }
        state.ResumeTiming();

        ht.clear();
    }
}

BENCHMARK_REGISTER_F(HashTableBench, FindForRead)
        ->ThreadPerCpu()
        ->Iterations(HashTableBench::numItems);
BENCHMARK_REGISTER_F(HashTableBench, FindForWrite)
        ->ThreadPerCpu()
        ->Iterations(HashTableBench::numItems);
BENCHMARK_REGISTER_F(HashTableBench, Insert)
        ->ThreadPerCpu()
        ->Iterations(HashTableBench::numItems);
BENCHMARK_REGISTER_F(HashTableBench, Replace)
        ->ThreadPerCpu()
        ->Iterations(HashTableBench::numItems);
BENCHMARK_REGISTER_F(HashTableBench, Delete)
        ->ThreadPerCpu()
        ->Iterations(HashTableBench::numItems);

BENCHMARK_REGISTER_F(HashTableBench, MultiCollectionInsert)
        ->ThreadPerCpu()
        ->Iterations(HashTableBench::numItems)
        ->Range(1, 1000);

BENCHMARK_REGISTER_F(HashTableBench, HTStatsEpilogue)
        ->ThreadPerCpu()
        ->Iterations(HashTableBench::numItems)
        ->Range(1, 1000);

BENCHMARK_REGISTER_F(HashTableBench, Clear)->Iterations(100);
