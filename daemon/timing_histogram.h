/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <relaxed_atomic.h>
#include <array>
#include <chrono>
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
    TimingHistogram();
    TimingHistogram(const TimingHistogram &other);
    TimingHistogram& operator=(const TimingHistogram &other);
    TimingHistogram& operator+=(const TimingHistogram& other);

    void reset();
    void add(std::chrono::nanoseconds nsec);
    std::string to_string();
    uint64_t get_total();


private:
    uint32_t get_ns();
    uint32_t get_msec(uint8_t index);
    uint32_t get_wayout(uint8_t index);
    uint32_t aggregate_wayout();

    // This helper is used for operator overloads. It is supplied a
    // class such as std::plus, std::minus; with the first argument (of
    // the class's function) being the current value and the second being
    // the new value.
    template <template <typename ...Args> class F>
    static void arith_op(TimingHistogram& dst, const TimingHistogram& src);

    /* We collect timings for <=1 us */
    cb::RelaxedAtomic<uint32_t> ns;
    /* We collect timings per 10usec */
    std::array<cb::RelaxedAtomic<uint32_t>, 100> usec;
    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    std::array<cb::RelaxedAtomic<uint32_t>, 50> msec;
    std::array<cb::RelaxedAtomic<uint32_t>, 10> halfsec;
    // wayout use the following buckets:
    // [5-9], [10-19], [20-39], [40-79], [80-inf].
    std::array<cb::RelaxedAtomic<uint32_t>, 5> wayout;
    cb::RelaxedAtomic<uint64_t> total;
};
