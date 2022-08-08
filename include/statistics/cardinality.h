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

namespace cb::prometheus {
/**
 * Indicates which group of stats should be collected for a given
 * request
 *  * low cardinality: per-bucket or global instance stats
 *  * high cardinality: per-collection/per-scope stats
 *  * all: include metrics from both low and high cardinality groups (e.g.,
 *         for code shared with cbstats)
 */
enum class MetricGroup { Low, High, Metering, All };
} // namespace cb::prometheus