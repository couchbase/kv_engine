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
#include <type_traits>

class EventuallyPersistentEngine;

/**
 * Data for a single histogram bucket to be added as a stat.
 */
struct HistogramBucket {
    // All currently used histograms have bucket bounds which are
    // (convertible to) uint64_t so this is used here. If histograms
    // are added for other underlying types, this may need extending.
    uint64_t lowerBound = 0;
    uint64_t upperBound = 0;
    uint64_t count = 0;
};

/**
 * Data for a whole histogram for use when adding a stat.
 *
 * StatCollector has overloads which will convert a Histogram
 * or HdrHistogram to this type, and call addStat with the result.
 *
 * This type exists to provide a canonical structure for histogram data.
 * All currently used Histogram stats can be converted to this format.
 * This means future alternative stat sinks (e.g., prometheus) don't need
 * to be aware of what type of histogram is used internally.
 */
struct HistogramData {
    // TODO: the mean _is_ derivable from the count and sum,
    //  but an accurate sum is not yet tracked. However, HdrHistogram
    //  _can_, for now, provide a more accurate mean.
    //  Once the sum is tracked, the mean can be removed from here.
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

/**
 * Interface implemented by statistics collection backends.
 *
 * Allows stats to be added in a key-value manner, to be formatted
 * and exposed in a backend-specific manner.
 *
 * Users may call the collector addStat with a stat key (e.g., get_cmd)
 * and a value for the backend to format appropriately.
 */
class StatCollector {
public:
    /**
     * Add a textual stat to the collector.
     *
     * Try to use other type specific overloads where possible.
     */
    virtual void addStat(std::string_view k, std::string_view v) = 0;
    /**
     * Add a boolean stat to the collector.
     */
    virtual void addStat(std::string_view k, bool v) = 0;

    /**
     * Add a numeric stat to the collector.
     *
     * Overloaded for signed, unsigned, and floating-point numbers.
     * Converting all numbers to any one of these types would either
     * cause narrowing, loss of precision, so backends are responsible
     * for handling each appropriately.
     */
    virtual void addStat(std::string_view k, int64_t v) = 0;
    virtual void addStat(std::string_view k, uint64_t v) = 0;
    virtual void addStat(std::string_view k, double v) = 0;

    /**
     * Add a histogram stat to the collector.
     *
     * HistogramData is an intermediate type to which multiple
     * histogram types are converted.
     */
    virtual void addStat(std::string_view k, const HistogramData& hist) = 0;

    /**
     * Add a textual stat. This overload is present to avoid conversion
     * to bool; overload resolution selects the bool overload rather than the
     * string_view overload.
     *
     * TODO: MB-40259 - replace this with a more general solution.
     */
    void addStat(std::string_view k, const char* v) {
        addStat(k, std::string_view(v));
    };

    /**
     * Overload with other signed/unsigned/float types.
     *
     * Avoids ambiguous calls when a numeric type is not explicitly
     * handled and may be converted to more than one of int64_t,
     * uint64_t,and double.
     */
    template <class T, class = std::enable_if_t<std::is_arithmetic_v<T>>>
    void addStat(std::string_view k, T v) {
        /* Converts the value to uint64_t/int64_t/double
         * based on if it is a signed/unsigned type.
         */
        if constexpr (std::is_unsigned_v<T>) {
            addStat(k, uint64_t(v));
        } else if constexpr (std::is_signed_v<T>) {
            addStat(k, int64_t(v));
        } else {
            addStat(k, double(v));
        }
    }

    /**
     * Converts a HdrHistogram instance to HistogramData,
     * and adds the result to the collector.
     *
     * Used to adapt histogram types to a single common type
     * for backends to support.
     */
    void addStat(std::string_view k, const HdrHistogram& v) {
        if (v.getValueCount() > 0) {
            HistogramData histData;
            histData.mean = std::round(v.getMean());
            histData.sampleCount = v.getValueCount();

            HdrHistogram::Iterator iter{v.getHistogramsIterator()};
            while (auto result = v.getNextBucketLowHighAndCount(iter)) {
                auto [lower, upper, count] = *result;

                histData.buckets.push_back({lower, upper, count});

                // TODO: HdrHistogram doesn't track the sum of all added values
                //  but prometheus requires that value. For now just approximate
                //  it from bucket counts.
                auto avgBucketValue = (lower + upper) / 2;
                histData.sampleSum += avgBucketValue * count;
            }
            addStat(k, histData);
        }
    }

    /**
     * Converts a Histogram<T, Limits> instance to HistogramData,
     * and adds the result to the collector.
     *
     * Used to adapt histogram types to a single common type
     * for backends to support.
     */
    template <typename T, template <class> class Limits>
    void addStat(std::string_view k, const Histogram<T, Limits>& hist) {
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
            histData.mean = std::round(double(histData.sampleSum) /
                                       histData.sampleCount);
        }
        addStat(k, histData);
    }

    /**
     * Convenience method for types with a method
     *  T load() const;
     *
     *  which returns an arithmetic type. Used to "unwrap" std::atomic,
     *  RelaxedAtomic, Monotonic, and NonNegativeCounter instances.
     *
     *  Avoids relying on implicit conversions for these types, so _other_
     *  types are not implicitly converted unintentionally.
     *
     */
    template <typename T>
    auto addStat(std::string_view k, const T& v)
            -> std::enable_if_t<std::is_arithmetic_v<decltype(v.load())>,
                                void> {
        addStat(k, v.load());
    }
};

/**
 * StatCollector implementation for exposing stats via CMD_STAT.
 *
 * Formats all stats to text and immediately calls the provided
 * addStatFn.
 */
class CBStatCollector : public StatCollector {
public:
    /**
     * Construct a collector which calls the provided addStatFn
     * for each added stat.
     * @param addStatFn callback called for each stat
     * @param cookie passed to addStatFn for each call
     */
    CBStatCollector(const AddStatFn& addStatFn, const void* cookie)
        : addStatFn(addStatFn), cookie(cookie) {
    }
    // Allow usage of the "helper" methods defined in the base type.
    // They would otherwise be shadowed
    using StatCollector::addStat;

    void addStat(std::string_view k, std::string_view v) override;
    void addStat(std::string_view k, bool v) override;
    void addStat(std::string_view k, int64_t v) override;
    void addStat(std::string_view k, uint64_t v) override;
    void addStat(std::string_view k, double v) override;
    void addStat(std::string_view k, const HistogramData& hist) override;

private:
    const AddStatFn& addStatFn;
    const void* cookie;
};

// Convenience method which maintain the existing add_casted_stat interface
// but calls out to CBStatCollector.
template <typename T>
void add_casted_stat(std::string_view k,
                     T&& v,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    CBStatCollector(add_stat, cookie).addStat(k, std::forward<T>(v));
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
