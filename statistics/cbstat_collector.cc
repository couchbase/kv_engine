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
                              const Labels& labels) {
    // CBStats has no concept of labels, but needs to distinguish scope and
    // collection stats through a prefix.
    addStatFn(addCollectionsPrefix(k.uniqueKey, labels), v, cookie);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              bool v,
                              const Labels& labels) {
    addStat(k, v ? "true"sv : "false"sv, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              int64_t v,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              uint64_t v,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              double v,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(const cb::stats::StatDef& k,
                              const HistogramData& hist,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}_mean", k.uniqueKey);
    addStat(cb::stats::StatDef({buf.data(), buf.size()}), hist.mean, labels);

    for (const auto& bucket : hist.buckets) {
        buf.resize(0);
        format_to(buf,
                  "{}_{},{}",
                  k.uniqueKey,
                  bucket.lowerBound,
                  bucket.upperBound);
        addStat(cb::stats::StatDef({buf.data(), buf.size()}),
                bucket.count,
                labels);
    }
}

std::string CBStatCollector::addCollectionsPrefix(std::string_view uniqueKey,
                                                  const Labels& labels) {
    fmt::memory_buffer buf;

    if (labels.count("scope")) {
        fmt::format_to(buf, "{}:", labels.at("scope"));
        if (labels.count("collection")) {
            fmt::format_to(buf, "{}:", labels.at("collection"));
        }
    }

    fmt::format_to(buf, "{}", uniqueKey);

    return {buf.data(), buf.size()};
}