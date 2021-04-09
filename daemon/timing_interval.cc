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

#include "timing_interval.h"

using namespace cb::sampling;

Interval& Interval::operator+=(const Interval& other) {
    count += other.count;
    duration_ns += other.duration_ns;
    return *this;
}

void Interval::reset() {
    count = 0;
    duration_ns = 0;
}

void IntervalSeries::reset() {
    intervals.reset();
}

Interval IntervalSeries::getAggregate() const {
    Interval ret;
    for (auto& i : intervals) {
        ret += i;
    }
    return ret;
}

void IntervalSeries::sample(const Interval& stats) {
    intervals.push_back(stats);
}
