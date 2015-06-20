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
#include <atomic>
#include <string>

class CommandTimings {
public:
    CommandTimings(void);
    CommandTimings(const CommandTimings &other);
    CommandTimings& operator=(const CommandTimings &other);

    void reset(void);
    void collect(const hrtime_t nsec);
    std::string to_string(void);
    uint32_t get_ns();
    uint32_t get_usec(const uint8_t index);
    uint32_t get_msec(const uint8_t index);
    uint32_t get_halfsec(const uint8_t index);
    uint32_t get_wayout();
    uint32_t get_total();

private:
    /* We collect timings for <=1 us */
    std::atomic<uint32_t> ns;
    /* We collect timings per 10usec */
    std::array<std::atomic<uint32_t>, 100> usec;
    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    std::array<std::atomic<uint32_t>, 50> msec;
    std::array<std::atomic<uint32_t>, 10> halfsec;
    std::atomic<uint32_t> wayout;
    std::atomic<uint64_t> total;
};
