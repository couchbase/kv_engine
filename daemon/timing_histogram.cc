/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "timing_histogram.h"

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <gsl/gsl>
#include <string>

TimingHistogram::TimingHistogram() {
    reset();
}

TimingHistogram::TimingHistogram(const TimingHistogram &other) {
    *this = other;
}

template <template <typename ...Args> class F>
void TimingHistogram::arith_op(TimingHistogram& a, const TimingHistogram& b) {
    a.ns = F<uint32_t>()(a.ns, b.ns);

    size_t idx;
    size_t len = a.usec.size();
    for (idx = 0; idx < len; ++idx) {
        a.usec[idx] = F<uint32_t>()(a.usec[idx], b.usec[idx]);
    }

    len = a.msec.size();
    for (idx = 0; idx < len; ++idx) {
        a.msec[idx] = F<uint32_t>()(a.msec[idx], b.msec[idx]);
    }

    len = a.halfsec.size();
    for (idx = 0; idx < len; ++idx) {
        a.halfsec[idx] = F<uint32_t>()(a.halfsec[idx], b.halfsec[idx]);
    }

    len = a.wayout.size();
    for (idx = 0; idx < len; ++idx) {
        a.wayout[idx] = F<uint32_t>()(a.wayout[idx], b.wayout[idx]);
    }
    a.total = F<uint64_t>()(a.total, b.total);
}

template <typename T>
struct identity {
    T operator() (const T& a, const T& b) {
        return b;
    }
};

/**
 * This isn't completely accurate, but it's only called whenever we're
 * grabbing the stats. We don't want to create a lock in order to make
 * sure that "total" is in 100% sync with all of the samples.. We
 * don't care <em>THAT</em> much for being accurate..
 */
TimingHistogram& TimingHistogram::operator=(const TimingHistogram& other) {
    TimingHistogram::arith_op<identity>(*this, other);
    return *this;
}

/**
 * As per operator=, this isn't completely accurate/consistent, but it's only
 * called whenever we're grabbing the stats.
 */
TimingHistogram& TimingHistogram::operator+=(const TimingHistogram& other) {
    TimingHistogram::arith_op<std::plus>(*this, other);
    return *this;
}

void TimingHistogram::reset() {
    ns = 0;
    for (auto& us : usec) {
        us.reset();
    }
    for (auto& ms : msec) {
        ms.reset();
    }
    for (auto& hs : halfsec) {
        hs.reset();
    }
    for (auto& wo: wayout) {
        wo.reset();
    }
    total.reset();
}

void TimingHistogram::add(std::chrono::nanoseconds nsec) {
    using namespace std::chrono;
    using halfseconds = duration<long long, std::ratio<1, 2>>;

    auto us = duration_cast<microseconds>(nsec);
    auto ms = duration_cast<milliseconds>(us);
    auto hs = duration_cast<halfseconds>(ms);

    if (us.count() == 0) {
        ns++;
    } else if (us.count() < 1000) {
        usec[us.count() / 10]++;
    } else if (ms.count() < 50) {
        msec[ms.count()]++;
    } else if (hs.count() < 10) {
        halfsec[hs.count()]++;
    } else {
        // [5-9], [10-19], [20-39], [40-79], [80-inf].
        auto sec = duration_cast<seconds>(hs);
        if (sec.count() < 10) {
            wayout[0]++;
        } else if (sec.count() < 20) {
            wayout[1]++;
        } else if (sec.count() < 40) {
            wayout[2]++;
        } else if (sec.count() < 80) {
            wayout[3]++;
        } else {
            wayout[4]++;
        }
    }
    total++;
}

std::string TimingHistogram::to_string() {
    nlohmann::json json;

    json["ns"] = get_ns();

    std::vector<uint32_t> array;
    for (auto &us : usec) {
        array.push_back(us);
    }
    json["us"] = array;

    array.clear();
    size_t len = msec.size();
    // element 0 isn't used
    for (size_t ii = 1; ii < len; ii++) {
        array.push_back(get_msec(gsl::narrow<uint8_t>(ii)));
    }
    json["ms"] = array;

    array.clear();

    for (auto &hs : halfsec) {
        array.push_back(hs);
    }
    json["500ms"] = array;

    json["5s-9s"] = get_wayout(0);
    json["10s-19s"] = get_wayout(1);
    json["20s-39s"] = get_wayout(2);
    json["40s-79s"] = get_wayout(3);
    json["80s-inf"] = get_wayout(4);

    // for backwards compatibility, add the old wayouts
    json["wayout"] = aggregate_wayout();

    return json.dump();
}

/* get functions of Timings class */

uint32_t TimingHistogram::get_ns() {
    return ns;
}

uint32_t TimingHistogram::get_msec(uint8_t index) {
    return msec[index];
}

uint32_t TimingHistogram::get_wayout(uint8_t index) {
    return wayout[index];
}

uint32_t TimingHistogram::aggregate_wayout() {
    uint32_t ret = 0;
    for (auto &wo : wayout) {
        ret += wo;
    }
    return ret;
}

uint64_t TimingHistogram::get_total() {
    return total;
}
