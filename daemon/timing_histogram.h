/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include <platform/platform.h>
#include <array>
#include <relaxed_atomic.h>
#include <string>

/** Records timings of some event, accumulating them in a histogram.
 *
 * Histogram has buckets of approximately exponentially increasing sizes:
 *
 *     - Less than or equal to 1 microsecond (µs)
 *     - [10-19], [20-29], ..., [900-999] µs
 *     - 1, 2, ..., 49 ms
 *     - 500, 1000, 1500, ... 4500 ms
 *     - [5-9], [10-19], [20-39], [40-79], [80-inf] seconds.
 *
 */
class TimingHistogram {
public:
    TimingHistogram(void);
    TimingHistogram(const TimingHistogram &other);
    TimingHistogram& operator=(const TimingHistogram &other);
    TimingHistogram& operator+=(const TimingHistogram& other);

    void reset(void);
    void add(const hrtime_t nsec);
    std::string to_string(void);
    uint32_t get_ns();
    uint32_t get_usec(const uint8_t index);
    uint32_t get_msec(const uint8_t index);
    uint32_t get_halfsec(const uint8_t index);
    uint32_t get_wayout(const uint8_t index);
    uint32_t get_total();

    uint32_t aggregate_wayout();

private:
    // This helper is used for operator overloads. It is supplied a
    // class such as std::plus, std::minus; with the first argument (of
    // the class's function) being the current value and the second being
    // the new value.
    template <template <typename ...Args> class F>
    static void arith_op(TimingHistogram& dst, const TimingHistogram& src);

    /* We collect timings for <=1 us */
    Couchbase::RelaxedAtomic<uint32_t> ns;
    /* We collect timings per 10usec */
    std::array<Couchbase::RelaxedAtomic<uint32_t>, 100> usec;
    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    std::array<Couchbase::RelaxedAtomic<uint32_t>, 50> msec;
    std::array<Couchbase::RelaxedAtomic<uint32_t>, 10> halfsec;
    // wayout use the following buckets:
    // [5-9], [10-19], [20-39], [40-79], [80-inf].
    std::array<Couchbase::RelaxedAtomic<uint32_t>, 5> wayout;
    Couchbase::RelaxedAtomic<uint64_t> total;
};
