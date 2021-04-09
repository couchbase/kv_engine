/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
