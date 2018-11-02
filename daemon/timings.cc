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

void Timings::reset() {
    for (auto& t : timings) {
        t.reset();
    }

    {
        std::lock_guard<std::mutex> lg(lock);
        interval_latency_lookups.reset();
        interval_latency_mutations.reset();
    }
}

void Timings::collect(cb::mcbp::ClientOpcode opcode,
                      std::chrono::nanoseconds nsec) {
    timings[std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode)].add(
            nsec);
    auto& interval = interval_counters
            [std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode)];
    interval.count++;
    interval.duration_ns += nsec.count();
}

std::string Timings::generate(cb::mcbp::ClientOpcode opcode) {
    return timings[std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode)]
            .to_string();
}

static const cb::mcbp::ClientOpcode timings_mutations[] = {
        cb::mcbp::ClientOpcode::Add,
        cb::mcbp::ClientOpcode::Addq,
        cb::mcbp::ClientOpcode::Append,
        cb::mcbp::ClientOpcode::Appendq,
        cb::mcbp::ClientOpcode::Decrement,
        cb::mcbp::ClientOpcode::Decrementq,
        cb::mcbp::ClientOpcode::Delete,
        cb::mcbp::ClientOpcode::Deleteq,
        cb::mcbp::ClientOpcode::Gat,
        cb::mcbp::ClientOpcode::Gatq,
        cb::mcbp::ClientOpcode::Increment,
        cb::mcbp::ClientOpcode::Incrementq,
        cb::mcbp::ClientOpcode::Prepend,
        cb::mcbp::ClientOpcode::Prependq,
        cb::mcbp::ClientOpcode::Replace,
        cb::mcbp::ClientOpcode::Replaceq,
        cb::mcbp::ClientOpcode::Set,
        cb::mcbp::ClientOpcode::Setq,
        cb::mcbp::ClientOpcode::Touch,
        cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
        cb::mcbp::ClientOpcode::SubdocArrayInsert,
        cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
        cb::mcbp::ClientOpcode::SubdocArrayPushLast,
        cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
        cb::mcbp::ClientOpcode::SubdocCounter,
        cb::mcbp::ClientOpcode::SubdocDelete,
        cb::mcbp::ClientOpcode::SubdocDictUpsert,
        cb::mcbp::ClientOpcode::SubdocReplace,
        cb::mcbp::ClientOpcode::SubdocDictAdd,
        cb::mcbp::ClientOpcode::SubdocMultiMutation};

static const cb::mcbp::ClientOpcode timings_retrievals[] = {
        cb::mcbp::ClientOpcode::Gat,
        cb::mcbp::ClientOpcode::Gatq,
        cb::mcbp::ClientOpcode::Get,
        cb::mcbp::ClientOpcode::Getk,
        cb::mcbp::ClientOpcode::Getkq,
        cb::mcbp::ClientOpcode::Getq,
        cb::mcbp::ClientOpcode::GetLocked,
        cb::mcbp::ClientOpcode::GetRandomKey,
        cb::mcbp::ClientOpcode::GetReplica,
        cb::mcbp::ClientOpcode::SubdocMultiLookup,
        cb::mcbp::ClientOpcode::SubdocGet,
        cb::mcbp::ClientOpcode::SubdocExists};

uint64_t Timings::get_aggregated_mutation_stats() {

    uint64_t ret = 0;
    for (auto cmd : timings_mutations) {
        ret += timings[std::underlying_type<cb::mcbp::ClientOpcode>::type(cmd)]
                       .get_total();
    }
    return ret;
}

uint64_t Timings::get_aggregated_retrival_stats() {

    uint64_t ret = 0;
    for (auto cmd : timings_retrievals) {
        ret += timings[std::underlying_type<cb::mcbp::ClientOpcode>::type(cmd)]
                       .get_total();
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
        interval_mutation += interval_counters
                [std::underlying_type<cb::mcbp::ClientOpcode>::type(op)];
        interval_counters[std::underlying_type<cb::mcbp::ClientOpcode>::type(
                                  op)]
                .reset();
    }

    for (auto op : timings_retrievals) {
        interval_lookup += interval_counters
                [std::underlying_type<cb::mcbp::ClientOpcode>::type(op)];
        interval_counters[std::underlying_type<cb::mcbp::ClientOpcode>::type(
                                  op)]
                .reset();
    }

    {
        std::lock_guard<std::mutex> lg(lock);
        interval_latency_lookups.sample(interval_lookup);
        interval_latency_mutations.sample(interval_mutation);
    }
}
