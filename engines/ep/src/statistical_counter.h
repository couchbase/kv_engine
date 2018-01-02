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

#pragma once

#include <random>

/**
 * Provides counter functionality so as the counter increases it becomes
 * increasingly more difficult to increment.  This enables a high granularity
 * counter to be implemented using only a small number of bits.
 *
 * It is based on the statistical counter described at
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
 * - approx 14000 to mimic a u32int counter (max value of 4,294,967,295)
 * - approx 2 to mimic a u16int counter (max value of 65,535)
 *
 * For example to replace a u16int counter with a statistical counter that
 * only requires 8-bits of storage, you would need to construct a
 * StatisticalCounter as follows:
 * StatisticalCounter<uint8_t> statisticalCounter(2.0);
 *
 * It would be used as follows:
 *
 * uint16_t counter{0}; // Currently we are using uint16_t even though only
 *                      // 8-bits of storage are actually used.
 * counter = statisticalCounter.generateCounterValue(counter);
 *
 * The generateCounterValue can be called approximately 65,000 times before
 * the counter becomes saturated at 255.
 *
 */
template <class T>
class StatisticalCounter {
public:
    StatisticalCounter(double incFac = 0.0) : incFactor(incFac) {
    }

    /**
     * Attempts generate a new incremented value for a given uint16_t.  The
     * increment functionality is probabilistic, with it becoming increasingly
     * more difficult to increment as the value gets higher.
     * @param counter  The current counter value to generate increment for
     * @returns new incremented value.
     */
    T generateCounterValue(T counter) {
        if (isSaturated(counter)) {
            return counter;
        }
        double rand = dis(gen);
        auto divisor = (counter == 0) ? 1.0 : (counter * incFactor);
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
    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_real_distribution<> dis{0.0, 1.0};
    double incFactor;
};
