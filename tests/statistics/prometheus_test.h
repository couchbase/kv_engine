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

#include "collector_test.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <logger/logger.h>
#include <prometheus/metric_family.h>

#include <unordered_map>

class PrometheusStatTest : public CollectorTest {
public:
    using StatMap = std::unordered_map<std::string, prometheus::MetricFamily>;

    // Convenience holder for metrics for the high and low cardinality
    // prometheus endpoints
    struct EndpointMetrics {
        StatMap high;
        StatMap low;
        StatMap metering;
    };
    EndpointMetrics getMetrics() const;
};
