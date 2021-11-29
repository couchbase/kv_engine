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
#include "stat_timings.h"

#include <statistics/definitions.h>
#include <statistics/labelled_collector.h>
#include <utilities/hdrhistogram.h>

StatTimings::~StatTimings() {
    std::lock_guard<std::mutex> lg(histogramMutex);
    for (auto& t : noArgsTimings) {
        delete t.load();
    }
    for (auto& t : withArgsTimings) {
        delete t.load();
    }
}

void StatTimings::reset() {
    for (auto& t : noArgsTimings) {
        if (auto* ptr = t.load()) {
            ptr->reset();
        }
    }
    for (auto& t : withArgsTimings) {
        if (auto* ptr = t.load()) {
            ptr->reset();
        }
    }
}

void StatTimings::record(StatGroupId statGroup,
                         bool withArguments,
                         std::chrono::nanoseconds duration) {
    using namespace std::chrono;
    getOrCreateHistogram(statGroup, withArguments)
            .add(duration_cast<microseconds>(duration));
}

void StatTimings::addStats(const BucketStatCollector& collector) {
    using namespace cb::stats;
    StatsGroupManager::getInstance().iterate(
            [&collector, this](const StatGroup& sg) {
                // The "All" group is requested by providing an empty key,
                // replace this "all" for clarity when reading the output.

                using namespace std::string_view_literals;
                auto key = sg.id == StatGroupId::All ? "all"sv : sg.key;
                auto labelled = collector.withLabel("stat_key", key);

                if (auto* histPtr = getHistogram(sg.id, false /* no args */)) {
                    // histogram for stat group with no arguments provided
                    labelled.withLabel("arg_suffix", "")
                            .addStat(Key::stat_timings, *histPtr);
                }
                if (auto* histPtr = getHistogram(sg.id, true /* with args */)) {
                    // histogram for stat group with an argument provided.
                    // For all existing stat groups, this filters the results
                    // down to a single vbucket/scope/collection. To reflect
                    // this, suffix the stat key with ".single"
                    labelled.withLabel("arg_suffix", ".single")
                            .addStat(Key::stat_timings, *histPtr);
                }
            });
}

size_t StatTimings::getMemFootPrint() const {
    // include the size of StatTimings itself
    size_t footprint = sizeof(this);

    // add up the footprint of any histograms which have been allocated
    for (auto& t : noArgsTimings) {
        if (auto* ptr = t.load()) {
            footprint += ptr->getMemFootPrint();
        }
    }
    for (auto& t : withArgsTimings) {
        if (auto* ptr = t.load()) {
            footprint += ptr->getMemFootPrint();
        }
    }

    return footprint;
}

Hdr1sfMicroSecHistogram& StatTimings::getOrCreateHistogram(
        StatGroupId statGroup, bool withArguments) {
    auto& histograms = withArguments ? withArgsTimings : noArgsTimings;

    if (!histograms[size_t(statGroup)]) {
        std::lock_guard<std::mutex> allocLock(histogramMutex);
        if (!histograms[size_t(statGroup)]) {
            histograms[size_t(statGroup)] = new Hdr1sfMicroSecHistogram();
        }
    }
    return *(histograms[size_t(statGroup)].load());
}

Hdr1sfMicroSecHistogram* StatTimings::getHistogram(StatGroupId statGroup,
                                                   bool withArguments) {
    auto& histograms = withArguments ? withArgsTimings : noArgsTimings;
    return histograms[size_t(statGroup)].load();
}