/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "probabilistic_counter.h"

#include <mutex>
#include <random>

static std::uniform_real_distribution<> dis{0.0, 1.0};

// Generate and return a random double value
double ProbabilisticCounterImpl::generateRandom() {
/*
 * Use a thread_local random number generator.  Based on the
 * following: https://stackoverflow.com/a/21238187
 */
#if __APPLE__
    /*
     * Due to Apple's clang disabling the thread_local keyword
     * support we have to have the code below.
     * @todo Fixed in XCode 8 (MacOS 10.11.5 / 10.12 or later).
     */
    static __thread bool seeded = false;
    static __thread std::minstd_rand::result_type generatorState = 0;
    if (!seeded) {
        seeded = true;
        generatorState = std::random_device()();
    }
    std::minstd_rand gen(generatorState);
    // Move the generator state forward
    generatorState = gen();
#else
    static thread_local std::minstd_rand gen{std::random_device()()};
#endif
    return dis(gen);
}
