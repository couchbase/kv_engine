/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
