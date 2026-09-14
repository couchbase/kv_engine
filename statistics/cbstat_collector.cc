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

#include <cblogger/logger.h>
#include <fmt/args.h>
#include <hdrhistogram/hdrhistogram.h>
#include <memcached/cookie_iface.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/rbac/privileges.h>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ostr.h>

#include <string_view>

namespace {
/**
 * Serialize a HistogramData to the same JSON shape HdrHistogram::to_json()
 * produces (see platform/hdrhistogram/hdrhistogram.cc), so both histogram
 * types are indistinguishable to a client - and so that mcstat and other
 * non-cbstats clients can render any histogram stat with the same generic
 * JSON-histogram handling regardless of which C++ histogram type backs it
 * server-side.
 */
std::string histogramDataToJson(const HistogramData& hist) {
    uint64_t bucketedTotal = 0;
    for (const auto& bucket : hist.buckets) {
        bucketedTotal += bucket.count;
    }

    nlohmann::json data = nlohmann::json::array();
    uint64_t cumulative = 0;
    for (const auto& bucket : hist.buckets) {
        cumulative += bucket.count;
        const double percentile =
                bucketedTotal ? (100.0 * static_cast<double>(cumulative) /
                                 static_cast<double>(bucketedTotal))
                              : 0.0;
        data.push_back({bucket.upperBound, bucket.count, percentile});
    }

    nlohmann::json root;
    root["total"] = bucketedTotal;
    root["bucketsLow"] =
            hist.buckets.empty() ? 0 : hist.buckets.front().lowerBound;
    root["data"] = std::move(data);
    // HdrHistogram::to_json() doesn't include the mean - TimingHistogram-
    // Printer instead re-derives an average from bucket midpoints. That
    // approximation is exact for HistogramData too *except* for histograms
    // such as ArrayHistogram, whose buckets represent a single exact value
    // (e.g. bucket {i, i+1} means "value exactly i"), where the midpoint
    // (i+0.5) is systematically 0.5 higher than the true mean. Include the
    // already-tracked mean explicitly so clients can use it instead.
    root["mean"] = hist.mean;
    root["overflowed"] = hist.sampleCount - bucketedTotal;
    root["overflowed_sum"] = 0;
    root["max_trackable"] = hist.maxTrackableValue;
    return root.dump();
}
} // namespace

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
    if (k.needsFormatting() || labels.contains("scope_id")) {
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
    fmt::format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              uint64_t v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              float v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              double v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              const HistogramData& hist,
                              const Labels& labels) const {
    if (!useOldStyleHistograms) {
        // Report as a single JSON document, the same format used for
        // HdrHistogram-backed stats (see the addStat(HdrHistogram) overload
        // below) - lets any non-cbstats-compat client (e.g. mcstat) render
        // every histogram stat the same way, regardless of which C++
        // histogram type backs it server-side.
        addStat(k, histogramDataToJson(hist), labels);
        return;
    }

    auto key = k.needsFormatting() ? formatKey(k.cbstatsKey, labels)
                                   : std::string(k.cbstatsKey);
    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "{}_mean", key);
    addStat(cb::stats::StatDef({buf.data(), buf.size()}), hist.mean, labels);

    uint64_t cumulativeCount = 0;
    for (const auto& bucket : hist.buckets) {
        buf.resize(0);
        fmt::format_to(std::back_inserter(buf),
                       "{}_{},{}",
                       key,
                       bucket.lowerBound,
                       bucket.upperBound);
        addStat(cb::stats::StatDef({buf.data(), buf.size()}),
                bucket.count,
                labels);
        cumulativeCount += bucket.count;
    }

    // If cumulative bucket counts don't add up to the total sample count, then
    // those are overflow samples which are not tracked by the main histogram.
    // Report via _overflowed and _max_tracked keys so cbstats et al.
    // can render.
    const auto overflowed = hist.sampleCount - cumulativeCount;
    if (overflowed) {
        buf.resize(0);
        fmt::format_to(std::back_inserter(buf), "{}_overflowed", key);
        addStat(cb::stats::StatDef({buf.data(), buf.size()}),
                overflowed,
                labels);
        buf.resize(0);
        fmt::format_to(std::back_inserter(buf), "{}_maxTrackable", key);
        addStat(cb::stats::StatDef({buf.data(), buf.size()}),
                hist.maxTrackableValue,
                labels);
    }
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              const HdrHistogram& v,
                              const Labels& labels) const {
    if (useOldStyleHistograms) {
        // cbstats handles HdrHistograms in the same manner as Histogram,
        // so convert to the common HistogramData type and call addStat again.
        if (v.getValueCount() > 0) {
            HistogramData histData;
            histData.mean = std::round(v.getMean());
            histData.sampleCount = v.getValueCount() + v.getOverflowCount();
            histData.maxTrackableValue = v.getMaxTrackableValue();

            for (const auto& bucket : v) {
                histData.buckets.push_back(
                        {bucket.lower_bound, bucket.upper_bound, bucket.count});

                // TODO: HdrHistogram doesn't track the sum of all added values.
                // but
                //  For now just approximate it from bucket counts.
                auto avgBucketValue =
                        (bucket.lower_bound + bucket.upper_bound) / 2;
                histData.sampleSum += avgBucketValue * bucket.count;
            }
            addStat(k, histData, labels);
        }
    } else {
        addStat(k, v.to_string(), labels);
    }
}

cb::engine_errc CBStatCollector::testPrivilegeForStat(
        std::optional<cb::rbac::Privilege> additionalPriv,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    if (additionalPriv) {
        const auto ret = doTestPrivilege(*additionalPriv, sid, cid);
        if (ret != cb::engine_errc::success) {
            return ret;
        }
    }

    return doTestPrivilege(cb::rbac::Privilege::SimpleStats, sid, cid);
}

cb::engine_errc CBStatCollector::doTestPrivilege(
        cb::rbac::Privilege privilege,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    try {
        const auto access = cookie.testPrivilege(privilege, sid, cid);
        return access.getEngineErrorCode({}, cid);
    } catch (const std::exception& e) {
        LOG_ERROR_CTX(
                "CBStatCollector::doTestPrivilege: received exception"
                "while checking privilege",
                {"privilege", privilege},
                {"sid", sid ? sid->to_string() : "no-scope"},
                {"cid", cid ? cid->to_string() : "no-collection"},
                {"error", e.what()});
    }
    return cb::engine_errc::failed;
}

bool CBStatCollector::allowPrivilegedStats() const {
    try {
        return cookie.testPrivilege(cb::rbac::Privilege::Stats, {}, {})
                .success();
    } catch (const std::exception& e) {
        LOG_ERROR(
                "CBStatCollector::allowPrivilegedStats: received exception"
                "while checking privilege: {}",
                e.what());
        return false;
    }
}

std::string CBStatCollector::formatKey(std::string_view key,
                                       const Labels& labels) const {
    fmt::memory_buffer buf;

    try {
        // if this stat was added through a scope or collection collector,
        // prepend the appropriate prefix
        if (labels.contains("scope_id")) {
            fmt::format_to(
                    std::back_inserter(buf), "{}:", labels.at("scope_id"));
            if (labels.contains("collection_id")) {
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
