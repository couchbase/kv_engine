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

#include "command_timings.h"
#include <platform/platform.h>
#include <atomic>
#include <sstream>
#include <string>

CommandTimings::CommandTimings() {
    reset();
}

void CommandTimings::reset(void) {
    ns.store(0);
    for(auto& us: usec) {
        us.store(0);
    }
    for(auto& ms: msec) {
        ms.store(0);
    }
    for(auto& hs: halfsec) {
        hs.store(0);
    }
    wayout.store(0);
    total.store(0);
}

void CommandTimings::collect(const hrtime_t nsec) {
    hrtime_t us = nsec / 1000;
    hrtime_t ms = us / 1000;
    hrtime_t hs = ms / 500;

    if (us == 0) {
        ns++;
    } else if (us < 1000) {
        usec[us / 10]++;
    } else if (ms < 50) {
        msec[ms]++;
    } else if (hs < 10) {
        halfsec[hs]++;
    } else {
        wayout++;
    }
    total++;
}

std::string CommandTimings::to_string(void) {
    std::stringstream ss;

    ss << "{\"ns\":" << get_ns() << ",\"us\":[";
    for (int ii = 0; ii < 99; ++ii) {
        ss << get_usec(ii) << ",";
    }
    ss << get_usec(99) << "],\"ms\":[";
    for (int ii = 1; ii < 49; ++ii) {
        ss << get_msec(ii) << ",";
    }
    ss << get_msec(49) << "],\"500ms\":[";
    for (int ii = 0; ii < 9; ++ii) {
        ss << get_halfsec(ii) << ",";
    }
    ss << get_halfsec(9) << "],\"wayout\":"
    << get_wayout() << "}";
    std::string str = ss.str();

    return ss.str();
}

/* get functions of Timings class */

uint32_t CommandTimings::get_ns() {
    return ns.load(std::memory_order_relaxed);
}

uint32_t CommandTimings::get_usec(const uint8_t index) {
    return usec[index].load(std::memory_order_relaxed);
}

uint32_t CommandTimings::get_msec(const uint8_t index) {
    return msec[index].load(std::memory_order_relaxed);
}

uint32_t CommandTimings::get_halfsec(const uint8_t index) {
    return halfsec[index].load(std::memory_order_relaxed);
}

uint32_t CommandTimings::get_wayout() {
    return wayout.load(std::memory_order_relaxed);
}

uint32_t CommandTimings::get_total() {
    return total.load(std::memory_order_relaxed);
}
