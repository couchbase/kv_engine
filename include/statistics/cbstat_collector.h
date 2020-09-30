/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "collector.h"

#include <memcached/engine_common.h>
#include <spdlog/fmt/fmt.h>

#include <string>
#include <string_view>

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

    void addStat(const cb::stats::StatDef& k,
                 std::string_view v,
                 const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 bool v,
                 const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 int64_t v,
                 const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 uint64_t v,
                 const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 double v,
                 const Labels& labels) override;
    void addStat(const cb::stats::StatDef& k,
                 const HistogramData& hist,
                 const Labels& labels) override;

    /**
     * Get the wrapped cookie and addStatFn. Useful while code is
     * being transitioned to the StatCollector interface.
     */
    std::pair<const void*, const AddStatFn&> getCookieAndAddFn() {
        return {cookie, addStatFn};
    }

private:
    /**
     * Formats a CBStats-suitable stat key by adding a
     *  scopeID:
     * or
     *  scopeID:collectionID:
     *
     *  prefix to the unique key. E.g.,
     *
     *  0x0:disk_size
     *  0x0:0x8:disk_size
     *
     * The scopeID and collectionID values are looked up from the provided
     * @p labels map, with keys "scope" and "collection" respectively.
     * @param uniqueKey base key to add prefix to
     * @param labels labels to
     * @return
     */
    static std::string addCollectionsPrefix(std::string_view uniqueKey,
                                            const Labels& labels);

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
