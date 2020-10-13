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

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <logger/logger.h>
#include <prometheus/metric_family.h>

#include <unordered_map>

class PrometheusStatTest : public ::testing::Test {
public:
    void SetUp() override {
        Test::SetUp();
        cb::logger::createConsoleLogger();
    }

    using StatMap = std::unordered_map<std::string, prometheus::MetricFamily>;

    // Convenience holder for metrics for the high and low cardinality
    // prometheus endpoints
    struct EndpointMetrics {
        StatMap high;
        StatMap low;
    };
    EndpointMetrics getMetrics() const;
};
