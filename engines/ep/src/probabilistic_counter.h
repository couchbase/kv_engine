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

#pragma once

#include <mutex>
#include <random>

/**
 * Provides thread-safe counter functionality so as the counter increases it
 * becomes increasingly more difficult to increment.  This enables a high
 * granularity counter to be implemented using only a small number of bits.
 *
 * It is based on the logarithmic counter described at
 * http://antirez.com/news/109
 *
 * uint8_t LFULogIncr(uint8_t counter) {
 *     if (counter == 255) return 255;
 *     double r = (double)rand()/RAND_MAX;
 *     double baseval = counter - LFU_INIT_VAL;
 *     if (baseval < 0) baseval = 0;
 *     double p = 1.0/(baseval*server.lfu_log_factor+1);
 *     if (r < p) counter++;
 *     return counter;
 * }
 *
 * The actual counter is not held within the class as it is typically the bits
 * stored in a taggedPtr.
 *
 * Through experimentation it has been determined that you need a incFactor of:
 * - approx 800 to mimic a u32int counter (max value of 4,294,967,295)
 * - approx 0.012 to mimic a u16int counter (max value of 65,535)
 *
 * For example to replace a u16int counter with a probabilistic counter that
 * only requires 8-bits of storage, you would need to construct a
 * ProbabilisticCounter as follows:
 * ProbabilisticCounter<uint8_t> probabilisticCounter(2.0);
 *
 * It would be used as follows:
 *
 * uint8_t counter{0};
 * counter = probabilisticCounter.generateValue(counter);
 *
 * The generateValue can be called approximately 65,000 times before the counter
 * becomes saturated at 255.
 *
 */
template <class T>
class ProbabilisticCounter {
public:
    explicit ProbabilisticCounter(double incFac = 0.0) : incFactor(incFac) {
    }

    /**
     * Attempts generate a new incremented value for a given uint16_t.  The
     * increment functionality is probabilistic, with it becoming increasingly
     * more difficult to increment as the value gets higher.
     * The function is thread-safe.
     * @param counter  The current counter value to generate increment for
     * @returns new incremented value.
     */
    T generateValue(T counter) {
        if (isSaturated(counter)) {
            return counter;
        }
        double rand = generateRandom();

        // A power function is used to avoid incrementing the counter too
        // aggressively when the input value is low.
        auto divisor =
                (counter == 0) ? 1.0 : (counter * counter * incFactor + 1);
        double prob = 1.0 / divisor;
        if (rand < prob) {
            counter++;
        }
        return counter;
    }

    /**
     * Returns true if the counter passed in is saturated.  For a LogCounter
     * templated on u8int_t this would be at a value of 255.
     * @param counter  The counter to check to see if saturated.
     * @returns bool indicating if the passed in counter is saturated or not.
     */
    bool isSaturated(uint16_t counter) const {
        return (counter == std::numeric_limits<T>::max());
    }

private:
    // Generate and return a random double value
    double generateRandom() {
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

    std::uniform_real_distribution<> dis{0.0, 1.0};
    double incFactor;
};
