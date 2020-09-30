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

struct ServerApi;
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
    CBStatCollector(AddStatFn addStatFn,
                    const void* cookie,
                    ServerApi* serverApi)
        : addStatFn(std::move(addStatFn)),
          cookie(cookie),
          serverApi(serverApi) {
    }

    // Allow usage of the "helper" methods defined in the base type.
    // They would otherwise be shadowed
    using StatCollector::addStat;

    void addStat(const cb::stats::StatDef& k,
                 std::string_view v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 bool v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 int64_t v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 uint64_t v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 double v,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 const HistogramData& hist,
                 const Labels& labels) const override;
    void addStat(const cb::stats::StatDef& k,
                 const HdrHistogram& hist,
                 const Labels& labels) const override;

    cb::engine_errc testPrivilegeForStat(
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const override;

    /**
     * Get the wrapped cookie and addStatFn. Useful while code is
     * being transitioned to the StatCollector interface.
     */
    std::pair<const void*, const AddStatFn&> getCookieAndAddFn() const {
        return {cookie, addStatFn};
    }

    /**
     * Turn off formatting of stat keys for this collector.
     * Used to avoid "legacy" stats added via add_casted_stat being used
     * as a format string. These keys may include user data (e.g., dcp
     * connection names) which may contain invalid collection specifiers.
     *
     * Once all stats are added to a collector directly, this will no
     * longer be necessary, as all user data can be incorporated into
     * the stat key from labels (provided to format as a _value_, rather than
     * within the format string).
     *
     */
    void disableStatKeyFormatting() {
        shouldFormatStatKeys = false;
    }

    bool includeAggregateMetrics() const override {
        return true;
    }

private:
    /**
     * Formats a CBStats-suitable stat key. This replaces any named format
     * specifiers (e.g., {connection_type} ) in the key, and prepends a
     * scope/collection prefix if necessary -
     *
     *  scopeID:
     * or
     *  scopeID:collectionID:
     *
     * E.g.,
     *
     *  0x0:disk_size
     *  0x0:0x8:disk_size
     *
     * based on if the stat was added through a scope/collection collector.
     *
     * The scopeID and collectionID values are looked up from the provided
     * @p labels map, with keys "scope" and "collection" respectively.
     * @param key base key to format
     * @param labels labels to use as named format args (see fmt::arg)
     * @return formatted key
     * @throws std::runtime_error if the format fails, possibly due to a
     *         required named fmt argument not being in @p labels
     */
    std::string formatKey(std::string_view key, const Labels& labels) const;

    // Whether this stat collector should attempt to replace fmt specifiers
    // in the provided stat keys. May be disabled for stats with unsanitized
    // user data in the key.
    bool shouldFormatStatKeys = true;

    const AddStatFn addStatFn;
    const void* cookie;

    ServerApi* serverApi;
};

// Convenience method which maintain the existing add_casted_stat interface
// but calls out to CBStatCollector.
template <typename T>
void add_casted_stat(std::string_view k,
                     T&& v,
                     const AddStatFn& add_stat,
                     const void* cookie) {
    // the collector is used _immediately_ for addStat and then destroyed,
    // so testPrivilegeForStat is never called, so the server api can be null
    CBStatCollector collector(add_stat, cookie, nullptr);
    collector.disableStatKeyFormatting();
    collector.addStat(k, std::forward<T>(v));
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
