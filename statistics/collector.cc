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

#include "statistics/collector.h"

#include "statistics/labelled_collector.h"
#include <spdlog/fmt/fmt.h>
#include <utilities/hdrhistogram.h>

#include <string_view>

using namespace std::string_view_literals;

LabelledStatCollector StatCollector::withLabels(const Labels& labels) {
    return {*this, labels};
}

void StatCollector::addStat(std::string_view k,
                            const HdrHistogram& v,
                            const Labels& labels) {
    if (v.getValueCount() > 0) {
        HistogramData histData;
        histData.mean = std::round(v.getMean());
        histData.sampleCount = v.getValueCount();

        HdrHistogram::Iterator iter{v.getHistogramsIterator()};
        while (auto result = v.getNextBucketLowHighAndCount(iter)) {
            auto [lower, upper, count] = *result;

            histData.buckets.push_back({lower, upper, count});

            // TODO: HdrHistogram doesn't track the sum of all added values but
            //  prometheus requires that value. For now just approximate it
            //  from bucket counts.
            auto avgBucketValue = (lower + upper) / 2;
            histData.sampleSum += avgBucketValue * count;
        }
        addStat(k, histData, labels);
    }
}

void CBStatCollector::addStat(std::string_view k,
                              std::string_view v,
                              const Labels&) {
    // CBStats has no concept of labels, and so they are unused
    addStatFn(k, v, cookie);
}

void CBStatCollector::addStat(std::string_view k,
                              bool v,
                              const Labels& labels) {
    addStat(k, v ? "true"sv : "false"sv, labels);
}

void CBStatCollector::addStat(std::string_view k,
                              int64_t v,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(std::string_view k,
                              uint64_t v,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(std::string_view k,
                              double v,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}", v);
    addStat(k, {buf.data(), buf.size()}, labels);
}

void CBStatCollector::addStat(std::string_view k,
                              const HistogramData& hist,
                              const Labels& labels) {
    fmt::memory_buffer buf;
    format_to(buf, "{}_mean", k);
    addStat({buf.data(), buf.size()}, hist.mean, labels);

    for (const auto& bucket : hist.buckets) {
        buf.resize(0);
        format_to(buf, "{}_{},{}", k, bucket.lowerBound, bucket.upperBound);
        addStat({buf.data(), buf.size()}, bucket.count, labels);
    }
}