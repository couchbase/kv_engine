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

#include "config.h"

#include "hdrhistogram.h"
#include "objectregistry.h"

#include <memcached/engine_common.h>
#include <platform/histogram.h>
#include <platform/sized_buffer.h>

#include <atomic>
#include <cstring>
#include <sstream>

class EventuallyPersistentEngine;

inline void add_casted_stat(const char *k, const char *v,
                            ADD_STAT add_stat, const void *cookie) {
    EventuallyPersistentEngine *e = ObjectRegistry::onSwitchThread(NULL, true);
    add_stat(k, static_cast<uint16_t>(strlen(k)),
             v, static_cast<uint32_t>(strlen(v)), cookie);
    ObjectRegistry::onSwitchThread(e);
}

template <typename T>
void add_casted_stat(const char *k, const T &v,
                            ADD_STAT add_stat, const void *cookie) {
    std::stringstream vals;
    vals << v;
    add_casted_stat(k, vals.str().c_str(), add_stat, cookie);
}

inline void add_casted_stat(const char *k, const bool v,
                            ADD_STAT add_stat, const void *cookie) {
    add_casted_stat(k, v ? "true" : "false", add_stat, cookie);
}

template <typename T>
void add_casted_stat(const char *k, const std::atomic<T> &v,
                            ADD_STAT add_stat, const void *cookie) {
    add_casted_stat(k, v.load(), add_stat, cookie);
}

template <>
inline void add_casted_stat(const char* k,
                     const cb::const_char_buffer& v,
                     ADD_STAT add_stat,
                     const void* cookie) {
    EventuallyPersistentEngine* e = ObjectRegistry::onSwitchThread(NULL, true);
    add_stat(k, static_cast<uint16_t>(strlen(k)), v.data(), v.size(), cookie);
    ObjectRegistry::onSwitchThread(e);
}

template <>
inline void add_casted_stat(const char* k,
                            const HdrHistogram& v,
                            ADD_STAT add_stat,
                            const void* cookie) {
    HdrHistogram::Iterator iter{v.makeLinearIterator(1)};
    while (auto result = v.getNextValueAndCount(iter)) {
        if (result->second > 0) {
            std::stringstream ss;
            ss << k << "_" << result->first << "," << result->first;
            add_casted_stat(ss.str().c_str(), result->second, add_stat, cookie);
        }
    }
}

/// @cond DETAILS
/**
 * Convert a histogram into a bunch of calls to add stats.
 */
template <typename T, template <class> class Limits>
struct histo_stat_adder {
    histo_stat_adder(const char *k, ADD_STAT a, const void *c)
        : prefix(k), add_stat(a), cookie(c) {}

    void operator()(const std::unique_ptr<HistogramBin<T, Limits>>& b) {
        std::stringstream ss;
        ss << prefix << "_" << b->start() << "," << b->end();
        add_casted_stat(ss.str().c_str(), b->count(), add_stat, cookie);
    }
    const char *prefix;
    ADD_STAT add_stat;
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
                     ADD_STAT add_stat,
                     const void* cookie) {
    histo_stat_adder<T, Limits> a(k, add_stat, cookie);
    std::for_each(v.begin(), v.end(), a);
}

template <typename P, typename T>
void add_prefixed_stat(P prefix, const char *nm, T val,
                  ADD_STAT add_stat, const void *cookie) {
    std::stringstream name;
    name << prefix << ":" << nm;

    add_casted_stat(name.str().c_str(), val, add_stat, cookie);
}

template <typename P, typename T, template <class> class Limits>
void add_prefixed_stat(P prefix,
                       const char* nm,
                       Histogram<T, Limits>& val,
                       ADD_STAT add_stat,
                       const void* cookie) {
    std::stringstream name;
    name << prefix << ":" << nm;

    add_casted_stat(name.str().c_str(), val, add_stat, cookie);
}
