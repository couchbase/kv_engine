/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "thread_stats.h"
#include "buckets.h"
#include "connection.h"
#include "front_end_thread.h"

HighResolutionThreadStats& get_high_resolution_thread_stats(Connection& c) {
    auto& independent_stats = c.getBucket().high_resolution_stats;
    return independent_stats.at(c.getThread().index);
}

void reset_high_resolution_thread_stats(
        std::vector<HighResolutionThreadStats>& thread_stats) {
    for (auto& ii : thread_stats) {
        ii.reset();
    }
}
