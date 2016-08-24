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
#include "timings.h"
#include <memcached/protocol_binary.h>
#include <platform/platform.h>
#include "timing_histogram.h"

Timings::Timings() {
    reset();
}

Timings& Timings::operator=(const Timings& other) {
    timings = other.timings;
    interval_latency_lookups = other.interval_latency_lookups;
    interval_latency_mutations = other.interval_latency_mutations;
    return *this;
}

void Timings::reset(void) {
    for (int ii = 0; ii < MAX_NUM_OPCODES; ++ii) {
        timings[ii].reset();
    }

    {
        std::lock_guard<std::mutex> lg(lock);
        interval_latency_lookups.reset();
        interval_latency_mutations.reset();
    }
}

void Timings::collect(const uint8_t opcode, const hrtime_t nsec) {
    timings[opcode].add(nsec);
    auto& interval = interval_counters[opcode];
    interval.count++;
    interval.duration_ns += nsec;
}

std::string Timings::generate(const uint8_t opcode) {
    return timings[opcode].to_string();
}

static const uint8_t timings_mutations[] = {
    PROTOCOL_BINARY_CMD_ADD,
    PROTOCOL_BINARY_CMD_ADDQ,
    PROTOCOL_BINARY_CMD_APPEND,
    PROTOCOL_BINARY_CMD_APPENDQ,
    PROTOCOL_BINARY_CMD_DECREMENT,
    PROTOCOL_BINARY_CMD_DECREMENTQ,
    PROTOCOL_BINARY_CMD_DELETE,
    PROTOCOL_BINARY_CMD_DELETEQ,
    PROTOCOL_BINARY_CMD_GAT,
    PROTOCOL_BINARY_CMD_GATQ,
    PROTOCOL_BINARY_CMD_INCREMENT,
    PROTOCOL_BINARY_CMD_INCREMENTQ,
    PROTOCOL_BINARY_CMD_PREPEND,
    PROTOCOL_BINARY_CMD_PREPENDQ,
    PROTOCOL_BINARY_CMD_REPLACE,
    PROTOCOL_BINARY_CMD_REPLACEQ,
    PROTOCOL_BINARY_CMD_SET,
    PROTOCOL_BINARY_CMD_SETQ,
    PROTOCOL_BINARY_CMD_TOUCH,
    PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
    PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
    PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
    PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
    PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
    PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
    PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
    PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
    PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
    PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
    PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION};

static const uint8_t timings_retrievals[] = {
    PROTOCOL_BINARY_CMD_GAT,
    PROTOCOL_BINARY_CMD_GATQ,
    PROTOCOL_BINARY_CMD_GET,
    PROTOCOL_BINARY_CMD_GETK,
    PROTOCOL_BINARY_CMD_GETKQ,
    PROTOCOL_BINARY_CMD_GETQ,
    PROTOCOL_BINARY_CMD_GET_LOCKED,
    PROTOCOL_BINARY_CMD_GET_RANDOM_KEY,
    PROTOCOL_BINARY_CMD_GET_REPLICA,
    PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
    PROTOCOL_BINARY_CMD_SUBDOC_GET,
    PROTOCOL_BINARY_CMD_SUBDOC_EXISTS};


uint64_t Timings::get_aggregated_mutation_stats() {

    uint64_t ret = 0;
    for (auto cmd : timings_mutations) {
        ret += timings[cmd].get_total();
    }
    return ret;
}

uint64_t Timings::get_aggregated_retrival_stats() {

    uint64_t ret = 0;
    for (auto cmd : timings_retrievals) {
        ret += timings[cmd].get_total();
    }
    return ret;
}

cb::sampling::Interval Timings::get_interval_mutation_latency() {
    std::lock_guard<std::mutex> lg(lock);
    return interval_latency_mutations.getAggregate();
}

cb::sampling::Interval Timings::get_interval_lookup_latency() {
    std::lock_guard<std::mutex> lg(lock);
    return interval_latency_lookups.getAggregate();
}

void Timings::sample(std::chrono::seconds sample_interval) {
    cb::sampling::Interval interval_lookup, interval_mutation;

    for (auto op : timings_mutations) {
        interval_mutation += interval_counters[op];
        interval_counters[op].reset();
    }

    for (auto op : timings_retrievals) {
        interval_lookup += interval_counters[op];
        interval_counters[op].reset();
    }

    {
        std::lock_guard<std::mutex> lg(lock);
        interval_latency_lookups.sample(interval_lookup);
        interval_latency_mutations.sample(interval_mutation);
    }
}
