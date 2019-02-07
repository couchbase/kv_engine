/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
 * Benchmarks relating to the CheckpointIterator class.
 */

#include "atomic.h"
#include "checkpoint_iterator.h"

#include <benchmark/benchmark.h>
#include <list>

typedef std::unique_ptr<int> TestItem;
typedef std::list<TestItem> ListContainer;
typedef CheckpointIterator<ListContainer> ListContainerIterator;

ListContainerIterator listContainerBegin(ListContainer& c) {
    return ListContainerIterator(c, ListContainerIterator::Position::begin);
}

/**
 * Benchmark ton check the performance of doing an iterator compare.
 * The reason being that when using a std::reference_wrapper (see
 * checkpointer_iterator.h) it is important to check for equality using the
 * address of the containers as opposed to doing a "deep" compare which
 * would involve comparing each element, which if there are many elements
 * will be costly.
 */
static void BM_CheckpointIteratorCompare(benchmark::State& state) {
    ListContainer c;
    // Populate with 1000 items
    for (size_t ii = 0; ii < 1000; ++ii) {
        c.push_back(std::make_unique<int>(ii));
    }

    // Create two iterators for the container
    auto cursor = listContainerBegin(c);
    auto cursor2 = listContainerBegin(c);

    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(cursor == cursor2);
    }
}

// Register the function as a benchmark
BENCHMARK(BM_CheckpointIteratorCompare);
