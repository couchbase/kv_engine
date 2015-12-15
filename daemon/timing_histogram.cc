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

#include <platform/platform.h>
#include <atomic>
#include <sstream>
#include <string>
#include <cJSON.h>
#include <cJSON_utils.h>

TimingHistogram::TimingHistogram() {
    reset();
}

TimingHistogram::TimingHistogram(const TimingHistogram &other) {
    *this = other;
}

template <typename T>
static void copy(T&dst, const T&src) {
    dst.store(src.load(std::memory_order_relaxed),
              std::memory_order_relaxed);
}

/**
 * This isn't completely accurate, but it's only called whenever we're
 * grabbing the stats. We don't want to create a lock in order to make
 * sure that "total" is in 100% sync with all of the samples.. We
 * don't care <em>THAT</em> much for being accurate..
 */
TimingHistogram& TimingHistogram::operator=(const TimingHistogram&other) {
    copy(ns, other.ns);

    size_t idx;
    size_t len = usec.size();
    for (idx = 0; idx < len; ++idx) {
        copy(usec[idx], other.usec[idx]);
    }

    len = msec.size();
    for (idx = 0; idx < len; ++idx) {
        copy(msec[idx], other.msec[idx]);
    }

    len = halfsec.size();
    for (idx = 0; idx < len; ++idx) {
        copy(halfsec[idx], other.halfsec[idx]);
    }

    len = wayout.size();
    for (idx = 0; idx < len; ++idx) {
        copy(wayout[idx], other.wayout[idx]);
    }

    copy(total, other.total);

    return *this;
}

void TimingHistogram::reset(void) {
    ns.store(0, std::memory_order_relaxed);
    for(auto& us: usec) {
        us.store(0, std::memory_order_relaxed);
    }
    for(auto& ms: msec) {
        ms.store(0, std::memory_order_relaxed);
    }
    for(auto& hs: halfsec) {
        hs.store(0, std::memory_order_relaxed);
    }
    for (auto& wo: wayout) {
        wo.store(0, std::memory_order_relaxed);
    }
    total.store(0, std::memory_order_relaxed);
}

void TimingHistogram::add(const hrtime_t nsec) {
    hrtime_t us = nsec / 1000;
    hrtime_t ms = us / 1000;
    hrtime_t hs = ms / 500;

    if (us == 0) {
        ns.fetch_add(1, std::memory_order_relaxed);
    } else if (us < 1000) {
        usec[us / 10].fetch_add(1, std::memory_order_relaxed);
    } else if (ms < 50) {
        msec[ms].fetch_add(1, std::memory_order_relaxed);
    } else if (hs < 10) {
        halfsec[hs].fetch_add(1, std::memory_order_relaxed);
    } else {
        // [5-9], [10-19], [20-39], [40-79], [80-inf].
        hrtime_t sec = hs / 2;
        if (sec < 10) {
            wayout[0].fetch_add(1, std::memory_order_relaxed);
        } else if (sec < 20) {
            wayout[1].fetch_add(1, std::memory_order_relaxed);
        } else if (sec < 40) {
            wayout[2].fetch_add(1, std::memory_order_relaxed);
        } else if (sec < 80) {
            wayout[3].fetch_add(1, std::memory_order_relaxed);
        } else {
            wayout[4].fetch_add(1, std::memory_order_relaxed);
        }
    }
    total.fetch_add(1, std::memory_order_relaxed);
}

std::string TimingHistogram::to_string(void) {
    unique_cJSON_ptr json(cJSON_CreateObject());
    cJSON* root = json.get();

    if (root == nullptr) {
        throw std::bad_alloc();
    }

    cJSON_AddNumberToObject(root, "ns", get_ns());

    cJSON *array = cJSON_CreateArray();
    for (auto &us : usec) {
        cJSON *obj = cJSON_CreateNumber(us.load(std::memory_order_relaxed));
        cJSON_AddItemToArray(array, obj);
    }
    cJSON_AddItemToObject(root, "us", array);

    array = cJSON_CreateArray();
    size_t len = msec.size();
    // element 0 isn't used
    for (size_t ii = 1; ii < len; ii++) {
        cJSON *obj = cJSON_CreateNumber(get_msec(ii));
        cJSON_AddItemToArray(array, obj);
    }
    cJSON_AddItemToObject(root, "ms", array);

    array = cJSON_CreateArray();
    for (auto &hs : halfsec) {
        cJSON *obj = cJSON_CreateNumber(hs.load(std::memory_order_relaxed));
        cJSON_AddItemToArray(array, obj);
    }
    cJSON_AddItemToObject(root, "500ms", array);

    cJSON_AddNumberToObject(root, "5s-9s", get_wayout(0));
    cJSON_AddNumberToObject(root, "10s-19s", get_wayout(1));
    cJSON_AddNumberToObject(root, "20s-39s", get_wayout(2));
    cJSON_AddNumberToObject(root, "40s-79s", get_wayout(3));
    cJSON_AddNumberToObject(root, "80s-inf", get_wayout(4));

    // for backwards compatibility, add the old wayouts
    cJSON_AddNumberToObject(root, "wayout", aggregate_wayout());
    char *ptr = cJSON_PrintUnformatted(root);
    std::string ret(ptr);
    cJSON_Free(ptr);

    return ret;
}

/* get functions of Timings class */

uint32_t TimingHistogram::get_ns() {
    return ns.load(std::memory_order_relaxed);
}

uint32_t TimingHistogram::get_usec(const uint8_t index) {
    return usec[index].load(std::memory_order_relaxed);
}

uint32_t TimingHistogram::get_msec(const uint8_t index) {
    return msec[index].load(std::memory_order_relaxed);
}

uint32_t TimingHistogram::get_halfsec(const uint8_t index) {
    return halfsec[index].load(std::memory_order_relaxed);
}

uint32_t TimingHistogram::get_wayout(const uint8_t index) {
    return wayout[index].load(std::memory_order_relaxed);
}

uint32_t TimingHistogram::aggregate_wayout() {
    uint32_t ret = 0;
    for (auto &wo : wayout) {
        ret += wo.load(std::memory_order_relaxed);
    }
    return ret;
}

uint32_t TimingHistogram::get_total() {
    return total.load(std::memory_order_relaxed);
}
