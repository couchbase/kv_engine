/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/stat_group.h>
#include <array>
#include <atomic>
#include <chrono>
#include <mutex>

// forward decls
class Hdr1sfMicroSecHistogram;
class BucketStatCollector;

/**
 * Records timings for each stat group requested with ClientOpcode::Stat.
 *
 * The full duration of each stat operation is recorded in a histogram
 * determined by the key.
 *
 * Stat keys with no arguments (e.g., "timings") have a histogram each, while
 * keys with args (e.g., "collections-details foobar") are are simplified
 * to avoid creating a histogram per vbucket/collection/scope etc.
 */
class StatTimings {
public:
    StatTimings() = default;

    StatTimings(const StatTimings&) = delete;
    StatTimings(StatTimings&&) = delete;
    StatTimings& operator=(const StatTimings&) = delete;
    StatTimings& operator=(StatTimings&&) = delete;
    ~StatTimings();

    /**
     * Reset all histograms.
     */
    void reset();

    /**
     * Record the time taken to generate values for a particular stat group.
     * @param statGroup the stat group which was requested
     * @param withArguments whether the request provided an argument
     * @param duration time taken to generate a response for the request.
     */
    void record(StatGroupId statGroup,
                bool withArguments,
                std::chrono::nanoseconds duration);

    /**
     * Collect stats from all initialised histograms.
     */
    void addStats(const BucketStatCollector& collector);

    size_t getMemFootPrint() const;

protected:
    Hdr1sfMicroSecHistogram& getOrCreateHistogram(StatGroupId statGroup,
                                                  bool withArguments);

    Hdr1sfMicroSecHistogram* getHistogram(StatGroupId statGroup,
                                          bool withArguments);

    using TimingHistArray = std::array<std::atomic<Hdr1sfMicroSecHistogram*>,
                                       size_t(StatGroupId::enum_max)>;

    // Map of stat key to histogram; used to record time taken generating
    // stats in response to a given stats key (e.g., vbucket-details).
    // Each histogram is allocated on first use.
    TimingHistArray noArgsTimings{{nullptr}};

    // Histograms tracking duration of requests with an argument e.g.,
    //  "collections-details _default"
    // In most cases, for stat groups with an optional argument, if an argument
    // is provided stats are collected for one vbucket/scope/collection, rather
    // than all. Record this separately as it will probably have a different
    // distribution.
    TimingHistArray withArgsTimings{{nullptr}};

    // mutex guarding histogram creation
    std::mutex histogramMutex;
};
