/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <chrono>
#include <cstdint>
#include <platform/platform.h>
#include <platform/ring_buffer.h>

#include "relaxed_atomic.h"

namespace cb {
namespace sampling {

static const size_t INTERVAL_SERIES_SIZE = 10;

/**
 * This class represents an interval in time. It contains the
 * *total* number of ops performed in this interval and the total amount of
 * time the ops took. The actual span of time this interval covers is determined
 * by the IntervalSeries which contains it.
 */
struct Interval {
    Couchbase::RelaxedAtomic<uint64_t> count = {(0)};
    Couchbase::RelaxedAtomic<uint64_t> duration_ns = {(0)};
    Interval& operator+=(const Interval& other);

    // Reset the counters to 0
    void reset();
};

/**
 * An IntervalSeries is simply a RingBuffer of multiple Interval objects.
 * It covers a span of `INTERVAL_SERIES_SIZE` seconds.
 */
class IntervalSeries {
public:
    /**
     * Get the count of all the operations and the total duration time for
     * the entire series.
     * @return the combined interval coverting `resolution * intervals.size()`
     * seconds.
     */
    Interval getAggregate() const;

    void sample(const Interval& stats);

    void reset();

private:
    cb::RingBuffer<Interval, INTERVAL_SERIES_SIZE> intervals;
};


}
}
