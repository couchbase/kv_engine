/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <platform/ring_buffer.h>

#include "relaxed_atomic.h"

namespace cb::sampling {

static const size_t INTERVAL_SERIES_SIZE = 10;

/**
 * This class represents an interval in time. It contains the
 * *total* number of ops performed in this interval and the total amount of
 * time the ops took. The actual span of time this interval covers is determined
 * by the IntervalSeries which contains it.
 */
struct Interval {
    cb::RelaxedAtomic<uint64_t> count = {(0)};
    cb::RelaxedAtomic<uint64_t> duration_ns = {(0)};
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

} // namespace cb::sampling
