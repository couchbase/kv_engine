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
 * Helper method to get a Histogram bucket lower bound as a uint64_t.
 *
 * Histogram can be instantiated with types not immediately
 * convertible to uint64_t (e.g., a std::chrono::duration);
 * this helper avoids duplicating code to handle different
 * instantiations.
 */
template <typename T, template <class> class Limits>
uint64_t getBucketMin(const typename Histogram<T, Limits>::value_type& bin) {
    return bin->start();
}

inline uint64_t getBucketMin(const MicrosecondHistogram::value_type& bin) {
    return bin->start().count();
}

/**
 * Helper method to get a Histogram bucket upper bound as a uint64_t.
 */
template <typename T, template <class> class Limits>
uint64_t getBucketMax(const typename Histogram<T, Limits>::value_type& bin) {
    return bin->end();
}

inline uint64_t getBucketMax(const MicrosecondHistogram::value_type& bin) {
    return bin->end().count();
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
 * Convert a histogram into add stat calls for each bucket and for the mean.
 */
template <typename T>
inline void add_casted_histo_stat(std::string_view k,
                                  const T& v,
                                  const AddStatFn& add_stat,
                                  const void* cookie) {
    if (v.getValueCount() > 0) {
        fmt::memory_buffer buf;
        format_to(buf, "{}_mean", k);
        add_casted_stat({buf.data(), buf.size()},
                        std::round(v.getMean()),
                        add_stat,
                        cookie);

        HdrHistogram::Iterator iter{v.getHistogramsIterator()};
        while (auto result = v.getNextBucketLowHighAndCount(iter)) {
            auto [lower, upper, count] = *result;
            if (count > 0) {
                buf.resize(0);
                format_to(buf, "{}_{},{}", k, lower, upper);
                add_casted_stat(
                        {buf.data(), buf.size()}, count, add_stat, cookie);
            }
        }
    }
}

inline void add_casted_stat(std::string_view k,
                            const HdrHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<HdrHistogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const Hdr1sfMicroSecHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<Hdr1sfMicroSecHistogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const Hdr2sfMicroSecHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<Hdr2sfMicroSecHistogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const Hdr1sfInt32Histogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<Hdr1sfInt32Histogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(std::string_view k,
                            const HdrUint8Histogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<HdrUint8Histogram>(k, v, add_stat, cookie);
}

template <typename T, template <class> class Limits>
void add_casted_stat(std::string_view k,
                     const Histogram<T, Limits>& hist,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    fmt::memory_buffer buf;
    for (const auto& bin : hist) {
        auto lower = getBucketMin(bin);
        auto upper = getBucketMax(bin);
        auto count = bin->count();
        buf.resize(0);
        format_to(buf, "{}_{},{}", k, lower, upper);
        add_casted_stat({buf.data(), buf.size()}, count, add_stat, cookie);
    }
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
