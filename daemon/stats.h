/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <executor/globaltask.h>
#include <hdrhistogram/hdrhistogram.h>
#include <memcached/engine_error.h>
#include <relaxed_atomic.h>

namespace cb::prometheus {
// forward declaration
enum class MetricGroup;
} // namespace cb::prometheus

/**
 * Global stats.
 */
struct GlobalStatistics {
    /** The current number of connections to the server */
    cb::RelaxedAtomic<unsigned int> curr_conns;

    size_t getCurrConnections() const {
        return curr_conns.load();
    }

    /// The number of system connections
    cb::RelaxedAtomic<unsigned int> system_conns;

    size_t getSystemConnections() const {
        return system_conns.load();
    }

    size_t getUserConnections() const {
        return getCurrConnections() - getSystemConnections();
    }

    /** The total number of connections to the server since start (or reset) */
    cb::RelaxedAtomic<unsigned int> total_conns;

    /** The current number of allocated connection objects */
    cb::RelaxedAtomic<unsigned int> conn_structs;

    /** The number of times I reject a client */
    cb::RelaxedAtomic<uint64_t> rejected_conns;

    /// Number of connections currently closing
    cb::RelaxedAtomic<uint64_t> curr_conn_closing;

    /** The number of auth commands sent */
    cb::RelaxedAtomic<uint64_t> auth_cmds;
    /** The number of authentication errors */
    cb::RelaxedAtomic<uint64_t> auth_errors;

    // ! Histograms of various task wait times, one per Task.
    std::array<Hdr1sfMicroSecHistogram, static_cast<int>(TaskId::TASK_COUNT)>
            taskSchedulingHistogram;

    // ! Histograms of various task run times, one per Task.
    std::array<Hdr1sfMicroSecHistogram, static_cast<int>(TaskId::TASK_COUNT)>
            taskRuntimeHistogram;
};

extern GlobalStatistics global_statistics;

class BucketStatCollector;
class StatCollector;
class PrometheusStatCollector;
class Bucket;
cb::engine_errc server_stats(const StatCollector& collector,
                             const Bucket& bucket);

void server_clock_stats(const StatCollector& collector);

class Timings;
/**
 * Add per-opcode timings stats to the given collector.
 */
void server_bucket_timing_stats(const BucketStatCollector& collector,
                                const Timings& timings);

// Add timing stats related to external authentication provider.
void external_auth_timing_stats(const BucketStatCollector& collector);

/**
 * Add stats needed for Prometheus to the given collector.
 *
 * The groups of stats which will be added is dependent on @p cardinality.
 * The cardinality indicates if the incoming request was to the high
 * or low cardinality endpoint.
 */
cb::engine_errc server_prometheus_stats(
        const PrometheusStatCollector& collector,
        cb::prometheus::MetricGroup metricGroup);

/**
 * Add all "low cardinality" group stats to the provided collector.
 *
 * These stats should remain (comparatively) few in number, and should
 * scale at most per-bucket (no per-vbucket, no per-collection).
 */
cb::engine_errc server_prometheus_stats_low(
        const PrometheusStatCollector& collector);

/**
 * Add all "high cardinality" group stats to the provided collector.
 *
 * These stats are expected to be numerous, and may scale e.g., per-collection.
 * It should be expected that these stats will be scraped less frequently
 * (than the "low cardinality" group) by Prometheus, but no exact interval
 * should be assumed.
 */
cb::engine_errc server_prometheus_stats_high(
        const PrometheusStatCollector& collector);

/**
 * Add metering/throttling specific metrics to the provided collector.
 *
 * These metrics are relevant to "serverless" deployments, and will not be
 * exposed at all otherwise.
 */
cb::engine_errc server_prometheus_metering(
        const PrometheusStatCollector& collector);

std::string getStatsResetTime();

class Cookie;
void stats_reset(Cookie& cookie);
