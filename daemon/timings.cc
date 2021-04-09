/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "timings.h"

Timings::Timings() {
    reset();
}

Timings::~Timings() {
    std::lock_guard<std::mutex> lg(histogram_mutex);
    for (auto& t : timings) {
        delete t;
    }
}

void Timings::reset() {
    {
        std::lock_guard<std::mutex> lg(histogram_mutex);
        for (auto& t : timings) {
            if (t) {
                t.load()->reset();
            }
        }
    }

    {
        std::lock_guard<std::mutex> lg(lock);
        interval_latency_lookups.reset();
        interval_latency_mutations.reset();
    }
}

void Timings::collect(cb::mcbp::ClientOpcode opcode,
                      std::chrono::nanoseconds nsec) {
    using namespace std::chrono;
    get_or_create_timing_histogram(
            std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode))
            .add(duration_cast<microseconds>(nsec));
    auto& interval =
            interval_counters
                    .get()[std::underlying_type<cb::mcbp::ClientOpcode>::type(
                            opcode)];
    interval.count++;
    interval.duration_ns += nsec.count();
}

std::string Timings::generate(cb::mcbp::ClientOpcode opcode) {
    auto* histoPtr =
            timings[std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode)]
                    .load();
    if (histoPtr) {
        return histoPtr->to_string();
    }
    return std::string("{}");
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

uint64_t Timings::get_aggregated_mutation_stats() const {
    uint64_t ret = 0;
    for (auto cmd : timings_mutations) {
        auto* histoPtr =
                timings[std::underlying_type<cb::mcbp::ClientOpcode>::type(cmd)]
                        .load();
        if (histoPtr) {
            ret += histoPtr->getValueCount();
        }
    }
    return ret;
}

uint64_t Timings::get_aggregated_retrieval_stats() const {
    uint64_t ret = 0;
    for (auto cmd : timings_retrievals) {
        auto* histoPtr =
                timings[std::underlying_type<cb::mcbp::ClientOpcode>::type(cmd)]
                        .load();
        if (histoPtr) {
            ret += histoPtr->getValueCount();
        }
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

Hdr1sfMicroSecHistogram& Timings::get_or_create_timing_histogram(
        uint8_t opcode) {
    if (!timings[opcode]) {
        std::lock_guard<std::mutex> allocLock(histogram_mutex);
        if (!timings[opcode]) {
            timings[opcode] = new Hdr1sfMicroSecHistogram();
        }
    }
    return *(timings[opcode].load());
}

Hdr1sfMicroSecHistogram* Timings::get_timing_histogram(uint8_t opcode) const {
    return timings[opcode].load();
}

void Timings::sample(std::chrono::seconds sample_interval) {
    cb::sampling::Interval interval_lookup, interval_mutation;

    for (auto op : timings_mutations) {
        for (auto intervals : interval_counters) {
            interval_mutation += intervals
                    [std::underlying_type<cb::mcbp::ClientOpcode>::type(op)];
            intervals[std::underlying_type<cb::mcbp::ClientOpcode>::type(op)]
                    .reset();
        }
    }

    for (auto op : timings_retrievals) {
        for (auto intervals : interval_counters) {
            interval_mutation += intervals
                    [std::underlying_type<cb::mcbp::ClientOpcode>::type(op)];
            intervals[std::underlying_type<cb::mcbp::ClientOpcode>::type(op)]
                    .reset();
        }
    }

    {
        std::lock_guard<std::mutex> lg(lock);
        interval_latency_lookups.sample(interval_lookup);
        interval_latency_mutations.sample(interval_mutation);
    }
}
