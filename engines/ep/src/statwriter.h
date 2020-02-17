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

#include <boost/optional.hpp>
#include <memcached/engine_common.h>
#include <platform/histogram.h>
#include <atomic>
#include <sstream>
#include <string>
#include <string_view>

using namespace std::string_view_literals;

class EventuallyPersistentEngine;

template <typename T>
void add_casted_stat(std::string_view k,
                     const T& v,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    std::stringstream vals;
    vals << v;
    add_stat(k, vals.str(), cookie);
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
inline void add_casted_histo_stat(const char* k,
                                  const T& v,
                                  const AddStatFn& add_stat,
                                  const void* cookie) {
    if (v.getValueCount() > 0) {
        std::string meanKey(k);
        meanKey += "_mean";
        add_casted_stat(
                meanKey.c_str(), std::round(v.getMean()), add_stat, cookie);

        HdrHistogram::Iterator iter{v.getHistogramsIterator()};
        while (auto result = v.getNextBucketLowHighAndCount(iter)) {
            if (std::get<2>(*result) > 0) {
                std::string newKey(k);
                newKey += "_" + std::to_string(std::get<0>(*result)) + "," +
                          std::to_string(std::get<1>(*result));
                add_casted_stat(
                        newKey.c_str(), std::get<2>(*result), add_stat, cookie);
            }
        }
    }
}

inline void add_casted_stat(const char* k,
                            const HdrHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<HdrHistogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(const char* k,
                            const Hdr1sfMicroSecHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<Hdr1sfMicroSecHistogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(const char* k,
                            const Hdr2sfMicroSecHistogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<Hdr2sfMicroSecHistogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(const char* k,
                            const Hdr1sfInt32Histogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<Hdr1sfInt32Histogram>(k, v, add_stat, cookie);
}

inline void add_casted_stat(const char* k,
                            const HdrUint8Histogram& v,
                            const AddStatFn& add_stat,
                            const void* cookie) {
    add_casted_histo_stat<HdrUint8Histogram>(k, v, add_stat, cookie);
}

/// @cond DETAILS
/**
 * Convert a histogram into a bunch of calls to add stats.
 */
template <typename T, template <class> class Limits>
struct histo_stat_adder {
    histo_stat_adder(const char* k, const AddStatFn& a, const void* c)
        : prefix(k), add_stat(a), cookie(c) {
    }

    void operator()(const std::unique_ptr<HistogramBin<T, Limits>>& b) {
        std::stringstream ss;
        ss << prefix << "_" << b->start() << "," << b->end();
        add_casted_stat(ss.str().c_str(), b->count(), add_stat, cookie);
    }
    const char *prefix;
    AddStatFn add_stat;
    const void *cookie;
};

// Specialization for UnsignedMicroseconds - needs count() calling to get
// a raw integer which can be printed.
template <>
inline void histo_stat_adder<UnsignedMicroseconds, cb::duration_limits>::
operator()(const MicrosecondHistogram::value_type& b) {
    std::stringstream ss;
    ss << prefix << "_" << b->start().count() << "," << b->end().count();

    add_casted_stat(ss.str().c_str(), b->count(), add_stat, cookie);
}
/// @endcond

template <typename T, template <class> class Limits>
void add_casted_stat(const char* k,
                     const Histogram<T, Limits>& v,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    histo_stat_adder<T, Limits> a(k, add_stat, cookie);
    std::for_each(v.begin(), v.end(), a);
}

template <typename P, typename T>
void add_prefixed_stat(P prefix,
                       const char* nm,
                       T val,
                       const AddStatFn& add_stat,
                       const void* cookie) {
    std::stringstream name;
    name << prefix << ":" << nm;

    add_casted_stat(name.str().c_str(), val, add_stat, cookie);
}

template <typename P, typename T, template <class> class Limits>
void add_prefixed_stat(P prefix,
                       const char* nm,
                       Histogram<T, Limits>& val,
                       const AddStatFn& add_stat,
                       const void* cookie) {
    std::stringstream name;
    name << prefix << ":" << nm;

    add_casted_stat(name.str().c_str(), val, add_stat, cookie);
}
