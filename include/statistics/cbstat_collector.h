/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collector.h"

#include <fmt/format.h>
#include <memcached/engine_common.h>
#include <string>
#include <string_view>

class CookieIface;

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
    CBStatCollector(AddStatFn addStatFn, const CookieIface* cookie)
        : addStatFn(std::move(addStatFn)), cookie(cookie) {
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
                 float v,
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
    std::pair<const CookieIface*, const AddStatFn&> getCookieAndAddFn() const {
        return {cookie, addStatFn};
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

    const AddStatFn addStatFn;
    const CookieIface* cookie;
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
    CBStatCollector collector(add_stat,
                              static_cast<const CookieIface*>(cookie));
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
