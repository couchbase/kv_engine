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

#include <statistics/cbstat_collector.h>

#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ostr.h>

#include <string_view>

using namespace std::string_view_literals;

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              std::string_view v,
                              const Labels& labels) const {
    // CBStats has no concept of labels, but needs to distinguish some stats
    // through prefixes

    addStatFn(formatKey(k.cbstatsKey, labels), v, cookie);
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
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              uint64_t v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              double v,
                              const Labels& labels) const {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              const HistogramData& hist,
                              const Labels& labels) const {
    auto key = formatKey(k.cbstatsKey, labels);
    fmt::memory_buffer buf;
    format_to(buf, "{}_mean", key);
    addStat(cb::stats::StatDef({buf.data(), buf.size()}), hist.mean, labels);

    for (const auto& bucket : hist.buckets) {
        buf.resize(0);
        format_to(buf, "{}_{},{}", key, bucket.lowerBound, bucket.upperBound);
        addStat(cb::stats::StatDef({buf.data(), buf.size()}),
                bucket.count,
                labels);
    }
}

/**
 * Format a string containing `fmt` replacement specifiers using key-value pairs
 * from a map as named arguments
 *
 * e.g.,
 *
 * formatFromMap(buf, "{connection_type}:items_remaining", {{"connection_type",
 * "replica}});
 * ->
 * "replica:items_remaining"
 *
 * Note - this is only required as the currently used version of fmt does not
 * support dynamic args. Once a version of fmt with
 *     fmt::dynamic_format_arg_store
 * support is available, this can be simplified.
 * Additionally, fmt::format_args is _not_ safe to construct with
 * fmt::make_format_args in a similar manner, due to lifetime issues as each
 * fmt::arg(...) needs to outlive any usages of the format_args object
 * (again, resolved by dynamic_format_arg_store)
 *
 */

auto formatFromMap(fmt::memory_buffer& buf,
                   std::string_view formatStr,
                   const StatCollector::Labels& labels) {
    auto itr = labels.cbegin();

    // lambda to create a fmt named arg from the next key-value pair
    // from the iterator.
    auto getArg = [&itr]() {
        const auto& [key, value] = *itr++;
        return fmt::arg(key, value);
    };

    // note, the order of evaluation of the args is unspecified, but isn't
    // important anyway. This can be made more succinct, but this is
    // straightforward to read and understand.

    switch (labels.size()) {
    case 0:
        return fmt::format_to(buf, formatStr);
    case 1:
        return fmt::format_to(buf, formatStr, getArg());
    case 2:
        return fmt::format_to(buf, formatStr, getArg(), getArg());
    case 3:
        return fmt::format_to(buf, formatStr, getArg(), getArg(), getArg());
    case 4:
        return fmt::format_to(
                buf, formatStr, getArg(), getArg(), getArg(), getArg());
    case 5:
        return fmt::format_to(buf,
                              formatStr,
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg());
    case 6:
        return fmt::format_to(buf,
                              formatStr,
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg());
    case 7:
        return fmt::format_to(buf,
                              formatStr,
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg());
    case 8:
        return fmt::format_to(buf,
                              formatStr,
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg(),
                              getArg());
    }

    throw std::runtime_error("makeFormatArgs too many labels provided");
}

std::string CBStatCollector::formatKey(std::string_view key,
                                       const Labels& labels) const {
    fmt::memory_buffer buf;

    try {
        // if this stat was added through a scope or collection collector,
        // prepend the appropriate prefix
        if (labels.count("scope")) {
            fmt::format_to(buf, "{}:", labels.at("scope"));
            if (labels.count("collection")) {
                fmt::format_to(buf, "{}:", labels.at("collection"));
            }
        }
        // now format the key itself, it may contain replacement specifiers
        // that can only be replaced with the appropriate value at runtime
        formatFromMap(buf, key, labels);

        return {buf.data(), buf.size()};

    } catch (const fmt::format_error& e) {
        throw std::runtime_error(
                "CBStatCollector::formatKey: Failed to format stat: \"" +
                std::string(key) + "\" : " + e.what());
    }
}
