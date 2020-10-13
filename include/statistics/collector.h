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

#include "definitions.h"

#include <platform/histogram.h>

#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>

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

class CollectionID;
class HdrHistogram;
class LabelledStatCollector;
class ScopeID;

class BucketStatCollector;
/**
 * Interface implemented by stats backends.
 *
 * Allows stats to be added in a key-value manner. Keys may also have a metric
 * family name and labels, but not all backends need support these.
 *
 * Users may call addStat with a key and value to be formatted
 * appropriately by the backend.
 *
 * Implementations which do not support labels should use the cbstatsKey.
 * These keys should be unique per-bucket.
 *
 * Stats are often organised in related blocks, for example all stats for a
 * particular bucket. Rather than repeating the bucket label for every stat,
 * a collector can be created which adds the bucket label to every added
 * stat.
 *
 *  StatCollector collector;
 *  collector.addStat("uptime", 12345); // global, no labels
 *  {
 *      // every stat added through `labelled` will have a bucket label
 *      auto labelled = collector.withLabels({{"bucket", "bucketName"}});
 *      labelled.addStat("mem_used", 999);
 *      labelled.addStat("disk_size", 123, {{"scope", "0x0"}});
 *  }
 *
 * Would lead to CBStats generating:
 *
 * uptime: 12345
 * mem_used: 999
 * 0x0:disk_size: 123
 *
 * Note: cbstats collects stats for a single bucket, so does not need to
 * distinguish between multiple.
 *
 * In contrast, the Prometheus backend may generate:
 *
 * uptime 12345
 * mem_used{bucket="bucketName"} 1
 * disk_size{bucket="bucketName", scope="0x0"} 123
 *
 */
class StatCollector {
public:
    using Labels = std::unordered_map<std::string_view, std::string_view>;

    /*
     * Create a collector tracking stats for a specific bucket. The new
     * collector wraps this instance, and labels all added stats with the
     * bucket name.
     */
    [[nodiscard]] BucketStatCollector forBucket(std::string_view bucket) const;

    /**
     * Add a textual stat to the collector.
     *
     * Try to use other type specific overloads where possible.
     */
    virtual void addStat(const cb::stats::StatDef& k,
                         std::string_view v,
                         const Labels& labels) const = 0;
    /**
     * Add a boolean stat to the collector.
     */
    virtual void addStat(const cb::stats::StatDef& k,
                         bool v,
                         const Labels& labels) const = 0;

    /**
     * Add a numeric stat to the collector.
     *
     * Overloaded for signed, unsigned, and floating-point numbers.
     * Converting all numbers to any one of these types would either
     * cause narrowing, loss of precision, so backends are responsible
     * for handling each appropriately.
     */
    virtual void addStat(const cb::stats::StatDef& k,
                         int64_t v,
                         const Labels& labels) const = 0;
    virtual void addStat(const cb::stats::StatDef& k,
                         uint64_t v,
                         const Labels& labels) const = 0;
    virtual void addStat(const cb::stats::StatDef& k,
                         double v,
                         const Labels& labels) const = 0;

    /**
     * Add a histogram stat to the collector.
     *
     * HistogramData is an intermediate type to which multiple
     * histogram types are converted.
     */
    virtual void addStat(const cb::stats::StatDef& k,
                         const HistogramData& hist,
                         const Labels& labels) const = 0;

    /**
     * Add a HdrHistogram stat to the collector.
     *
     */
    virtual void addStat(const cb::stats::StatDef& k,
                         const HdrHistogram& v,
                         const Labels& labels) const = 0;

    /**
     * Add a textual stat. This overload is present to avoid conversion
     * to bool; overload resolution selects the bool overload rather than the
     * string_view overload.
     *
     * TODO: MB-40259 - replace this with a more general solution.
     */
    void addStat(const cb::stats::StatDef& k,
                 const char* v,
                 const Labels& labels) const {
        addStat(k, std::string_view(v), labels);
    };

    /**
     * Overload with other signed/unsigned/float types.
     *
     * Avoids ambiguous calls when a numeric type is not explicitly
     * handled and may be converted to more than one of int64_t,
     * uint64_t,and double.
     */
    template <class T, class = std::enable_if_t<std::is_arithmetic_v<T>>>
    void addStat(const cb::stats::StatDef& k, T v, const Labels& labels) const {
        /* Converts the value to uint64_t/int64_t/double
         * based on if it is a signed/unsigned type.
         */

        static_assert(std::is_floating_point_v<T> || std::is_unsigned_v<T> ||
                              std::is_signed_v<T>,
                      "addStat called with unexpected type which is"
                      "arithmetic but not signed, unsigned or floating point.");

        // check floating point before is_signed
        // as floating point types may also be signed.
        if constexpr (std::is_floating_point_v<T>) {
            addStat(k, double(v), labels);
        } else if constexpr (std::is_unsigned_v<T>) {
            addStat(k, uint64_t(v), labels);
        } else if constexpr (std::is_signed_v<T>) {
            addStat(k, int64_t(v), labels);
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
    void addStat(const cb::stats::StatDef& k,
                 const Histogram<T, Limits>& hist,
                 const Labels& labels) const {
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
        addStat(k, histData, labels);
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
    auto addStat(const cb::stats::StatDef& k,
                 const T& v,
                 const Labels& labels) const
            -> std::enable_if_t<std::is_arithmetic_v<decltype(v.load())>,
                                void> {
        addStat(k, v.load(), labels);
    }

    /**
     * Look up the given stat key enum in the static StatDefs array.
     */
    static const cb::stats::StatDef& lookup(cb::stats::Key key);

    /**
     * Look up the stat definition (see stats.def.h) for the provided
     * key, then call addStat with that StatDef.
     *
     * Used to lookup the unit and labels associated with the stat.
     */
    template <typename T>
    void addStat(cb::stats::Key k, T&& v, const Labels& labels) const {
        addStat(lookup(k), std::forward<T>(v), labels);
    }

    /**
     * Overload for addStat calls with no specified labels.
     * Avoids default args on the other addStat overloads, as they are not
     * recommended for virtual methods.
     */
    template <typename Key, typename Value>
    void addStat(Key&& k, Value&& v) const {
        addStat(std::forward<Key>(k),
                std::forward<Value>(v),
                {/* no labels */});
    }

    virtual bool includeAggregateMetrics() const = 0;

    /**
     * Get the cookie from the underlying collector. If the collector is not
     * (a wrapper of) CBStatCollector, this will throw.
     *
     * MB-39505: This is a temporary workaround while migrating code
     * requiring privilege checks, and will be removed.
     */
    virtual const void* getCookie() const = 0;

    virtual ~StatCollector() = default;
};
