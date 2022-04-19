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

#include <statistics/cbstat_collector.h>

#include <fmt/args.h>
#include <hdrhistogram/hdrhistogram.h>
#include <logger/logger.h>
#include <memcached/cookie_iface.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/rbac/privileges.h>
#include <memcached/server_cookie_iface.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ostr.h>

#include <string_view>

using namespace std::string_view_literals;

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              std::string_view v,
                              const Labels& labels) const {
    if (!k.isCBStat()) {
        return;
    }
    // CBStats has no concept of labels, but needs to distinguish some stats
    // through prefixes
    // TODO: scope and collection prefixing was added before general formatted
    // stat support. It should be removed, and scope/col stats should declare
    // they require formatting. For now, if a scope_id is present the prefix
    // must be added.
    if (k.needsFormatting() || labels.count("scope_id")) {
        addStatFn(formatKey(k.cbstatsKey, labels), v, cookie);
    } else {
        addStatFn(k.cbstatsKey, v, cookie);
    }
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              bool v,
                              const Labels& labels) const {
    addStat(k, v ? "true"sv : "false"sv, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              int64_t v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              uint64_t v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              float v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              double v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              const HistogramData& hist,
                              const Labels& labels) const {
    auto key = k.needsFormatting() ? formatKey(k.cbstatsKey, labels)
                                   : std::string(k.cbstatsKey);
    fmt::memory_buffer buf;
    format_to(std::back_inserter(buf), "{}_mean", key);
    addStat(cb::stats::StatDef({buf.data(), buf.size()}), hist.mean, labels);

    for (const auto& bucket : hist.buckets) {
        buf.resize(0);
        format_to(std::back_inserter(buf),
                  "{}_{},{}",
                  key,
                  bucket.lowerBound,
                  bucket.upperBound);
        addStat(cb::stats::StatDef({buf.data(), buf.size()}),
                bucket.count,
                labels);
    }
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              const HdrHistogram& v,
                              const Labels& labels) const {
    // cbstats handles HdrHistograms in the same manner as Histogram,
    // so convert to the common HistogramData type and call addStat again.
    if (v.getValueCount() > 0) {
        HistogramData histData;
        histData.mean = std::round(v.getMean());
        histData.sampleCount = v.getValueCount();

        for (const auto& bucket : v) {
            histData.buckets.push_back(
                    {bucket.lower_bound, bucket.upper_bound, bucket.count});

            // TODO: HdrHistogram doesn't track the sum of all added values. but
            //  For now just approximate it from bucket counts.
            auto avgBucketValue = (bucket.lower_bound + bucket.upper_bound) / 2;
            histData.sampleSum += avgBucketValue * bucket.count;
        }
        addStat(k, histData, labels);
    }
}

cb::engine_errc CBStatCollector::testPrivilegeForStat(
        std::optional<ScopeID> sid, std::optional<CollectionID> cid) const {
    try {
        switch (cookie->testPrivilege(
                              cb::rbac::Privilege::SimpleStats, sid, cid)
                        .getStatus()) {
        case cb::rbac::PrivilegeAccess::Status::Ok:
            return cb::engine_errc::success;
        case cb::rbac::PrivilegeAccess::Status::Fail:
            return cb::engine_errc::no_access;
        case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
            return cid ? cb::engine_errc::unknown_collection
                       : cb::engine_errc::unknown_scope;
        }
    } catch (const std::exception& e) {
        LOG_ERROR(
                "CBStatCollector::testPrivilegeForStat: received exception"
                "while checking privilege for sid:{}: cid:{} {}",
                sid ? sid->to_string() : "no-scope",
                cid ? cid->to_string() : "no-collection",
                e.what());
    }
    return cb::engine_errc::failed;
}

std::string CBStatCollector::formatKey(std::string_view key,
                                       const Labels& labels) const {
    fmt::memory_buffer buf;

    try {
        // if this stat was added through a scope or collection collector,
        // prepend the appropriate prefix
        if (labels.count("scope_id")) {
            fmt::format_to(
                    std::back_inserter(buf), "{}:", labels.at("scope_id"));
            if (labels.count("collection_id")) {
                fmt::format_to(std::back_inserter(buf),
                               "{}:",
                               labels.at("collection_id"));
            }
        }
        // now format the key itself, it may contain replacement specifiers
        // that can only be replaced with the appropriate value at runtime
        fmt::dynamic_format_arg_store<fmt::format_context> store;
        for (const auto& label : labels) {
            store.push_back(fmt::arg(label.first, label.second));
        }
        fmt::vformat_to(std::back_inserter(buf), key, store);

        return fmt::to_string(buf);

    } catch (const fmt::format_error& e) {
        throw std::runtime_error(
                "CBStatCollector::formatKey: Failed to format stat: \"" +
                std::string(key) + "\" : " + e.what());
    }
}
