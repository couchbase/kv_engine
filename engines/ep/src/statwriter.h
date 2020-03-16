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

#include "hdrhistogram.h"

#include <memcached/engine_common.h>
#include <platform/histogram.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ostr.h>

#include <atomic>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

using namespace std::string_view_literals;

class EventuallyPersistentEngine;

/**
 * Data for a single histogram bucket to be added as a stat.
 */
struct HistogramBucket {
    // All currently used histograms have bucket bounds which are
    // (convertible to) uint64_t so this is used here. If histograms
    // are added for other underlying types, this may need extending.
    uint64_t lowerBound;
    uint64_t upperBound;
    uint64_t count;
};

/**
 * Data for a whole histogram for use when adding a stat.
 *
 * The add_casted_stat has overloads which will convert a Histogram
 * or HdrHistogram to this structure.
 *
 * This type exists to provide a canonical structure for histogram data.
 * All currently used Histogram stats can be converted to this format.
 * This means future alternative stat sinks (e.g., prometheus) don't need
 * to be aware of what type of histogram is used internally.
 */
struct HistogramData {
    uint64_t mean = 0;
    uint64_t sampleCount = 0;
    uint64_t sampleSum = 0;
    std::vector<HistogramBucket> buckets;
};

/**
 * Helper method to get a Histogram bucket lower bound as a uint64_t.
 *
 * Histogram can be instantiated with types not immediately
 * convertible to uint64_t (e.g., a std::chrono::duration);
 * this helper avoids duplicating code to handle different
 * instantiations.
 *
 * @param bucket the histogram bucket to from which to extract info
 * @return the lower bound of the given bucket
 */
template <typename HistValueType>
uint64_t getBucketMin(const HistValueType& bucket) {
    return bucket->start();
}

inline uint64_t getBucketMin(const MicrosecondHistogram::value_type& bucket) {
    return bucket->start().count();
}

/**
 * Helper method to get a Histogram bucket upper bound as a uint64_t.
 *
 * @param bin the histogram bucket to from which to extract info
 * @return the upper bound of the given bucket
 */
template <typename HistValueType>
uint64_t getBucketMax(const HistValueType& bucket) {
    return bucket->end();
}

inline uint64_t getBucketMax(const MicrosecondHistogram::value_type& bucket) {
    return bucket->end().count();
}

template <typename T>
void add_casted_stat(std::string_view k,
                     const T& v,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    fmt::memory_buffer buf;
    fmt::format_to(buf, "{}", v);
    add_stat(k, {buf.data(), buf.size()}, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const bool v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_stat(k, v ? "true"sv : "false"sv, cookie);
}

template <typename T>
void add_casted_stat(std::string_view k,
                     const std::atomic<T>& v,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    add_casted_stat(k, v.load(), add_stat, cookie);
}

template <typename T>
void add_casted_stat(std::string_view k,
                     const cb::RelaxedAtomic<T>& v,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    add_casted_stat(k, v.load(), add_stat, cookie);
}

/**
 * Call add_stat for each bucket of a HistogramData.
 */
inline void add_casted_stat(std::string_view k,
                            const HistogramData& hist,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    fmt::memory_buffer buf;
    format_to(buf, "{}_mean", k);
    add_casted_stat({buf.data(), buf.size()}, hist.mean, add_stat, cookie);

    for (const auto& bucket : hist.buckets) {
        buf.resize(0);
        format_to(buf, "{}_{},{}", k, bucket.lowerBound, bucket.upperBound);
        add_casted_stat(
                {buf.data(), buf.size()}, bucket.count, add_stat, cookie);
    }
}

inline void add_casted_histo_stat(std::string_view k,
                                  const HdrHistogram& v,
                                  const AddStatFn& add_stat,
                                  const void* cookie) {
    if (v.getValueCount() > 0) {
        HistogramData histData;
        histData.mean = std::round(v.getMean());
        histData.sampleCount = v.getValueCount();

        HdrHistogram::Iterator iter{v.getHistogramsIterator()};
        while (auto result = v.getNextBucketLowHighAndCount(iter)) {
            auto [lower, upper, count] = *result;

            histData.buckets.push_back({lower, upper, count});
            // HdrHistogram doesn't track the sum of all added values but
            // prometheus requires that value. For now just approximate it
            // from bucket counts.
            auto avgBucketValue = (lower + upper) / 2;
            histData.sampleSum += avgBucketValue * count;
        }
        add_casted_stat(k, histData, add_stat, cookie);
    }
}

inline void add_casted_stat(std::string_view k,
                            const HdrHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const Hdr1sfMicroSecHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const Hdr2sfMicroSecHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const Hdr1sfInt32Histogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const HdrUint8Histogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat(k, v, add_stat, cookie);
}

template <typename T, template <class> class Limits>
void add_casted_stat(std::string_view k,
                     const Histogram<T, Limits>& hist,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    HistogramData histData{};

    histData.sampleCount = hist.total();
    histData.buckets.reserve(hist.size());

    for (const auto& bin : hist) {
        auto lower = getBucketMin(bin);
        auto upper = getBucketMax(bin);
        auto count = bin->count();
        histData.buckets.push_back({lower, upper, count});

        // TODO: Histogram doesn't track the sum of all added values but
        //  prometheus requires that value. For now just approximate it from
        //  bucket counts.
        auto avgBucketValue = (lower + upper) / 2;
        histData.sampleSum += avgBucketValue * count;
    }

    if (histData.sampleCount != 0) {
        histData.mean =
                std::round(double(histData.sampleSum) / histData.sampleCount);
    }

    add_casted_stat(k, histData, add_stat, cookie);
}

template <typename P, typename T>
void add_prefixed_stat(P prefix,
                       std::string_view name,
                       const T& val,
                       const AddStatFn& add_stat,
                       const void* cookie) {
    fmt::memory_buffer buf;
    format_to(buf, "{}:{}", prefix, name);
    add_casted_stat({buf.data(), buf.size()}, val, add_stat, cookie);
}
