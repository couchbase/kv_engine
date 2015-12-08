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
#include <string>
#include <cstdint>
#include "timing_histogram.h"

#define MAX_NUM_OPCODES 0x100

class Timings {
public:
    Timings(void);
    void reset(void);
    void collect(const uint8_t opcode, const hrtime_t nsec);
    std::string generate(const uint8_t opcode);
    uint64_t get_aggregated_mutation_stats();
    uint64_t get_aggregated_retrival_stats();

private:
    std::array<TimingHistogram, MAX_NUM_OPCODES> timings;
};
