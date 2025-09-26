/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "stats.h"

#include "buckets.h"
#include "external_auth_manager_thread.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "network_interface_manager.h"
#include "sdk_connection_manager.h"
#include "settings.h"
#include <fmt/chrono.h>
#include <folly/Chrono.h>
#include <logger/logger.h>
#include <platform/cb_arena_malloc.h>
#include <platform/timeutils.h>
#include <serverless/config.h>
#include <sigar/sigar.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus.h>
#include <statistics/prometheus_collector.h>
#include <utilities/fusion_support.h>
#include <utilities/string_utilities.h>
#include <string_view>

// add global stats
static void server_global_stats(const StatCollector& collector) {
    rel_time_t now = mc_time_get_current_time();

    using namespace cb::stats;
    collector.addStat(Key::uptime, now);
    collector.addStat(Key::stat_reset, getStatsResetTime());
    collector.addStat(Key::time, mc_time_convert_to_abs_time(now));

    collector.addStat(Key::version, get_server_version());
    collector.addStat(Key::memcached_version, MEMCACHED_VERSION);

    if (collector.allowPrivilegedStats()) {
        collector.addStat(Key::daemon_connections,
                          networkInterfaceManager
                                  ? networkInterfaceManager
                                            ->getNumberOfDaemonConnections()
                                  : 0);
        auto curr = stats.curr_conns.load();
        auto sys = stats.system_conns.load();
        collector.addStat(Key::curr_connections, curr);
        collector.addStat(Key::system_connections, sys);
        collector.addStat(Key::user_connections, curr > sys ? curr - sys : 0);
        collector.addStat(Key::rejected_connections, stats.rejected_conns);
        collector.addStat(Key::max_user_connections,
                          Settings::instance().getMaxUserConnections());
        collector.addStat(Key::max_system_connections,
                          Settings::instance().getSystemConnections());
        collector.addStat(Key::total_connections, stats.total_conns);
        collector.addStat(Key::connection_structures, stats.conn_structs);
        collector.addStat(Key::curr_connections_closing,
                          stats.curr_conn_closing);
        if (isFusionSupportEnabled()) {
            collector.addStat(Key::fusion_migration_rate_limit,
                              magma::Magma::GetFusionMigrationRateLimit());
            collector.addStat(Key::fusion_sync_rate_limit,
                              magma::Magma::GetFusionSyncRateLimit());
        }

        auto sdks = SdkConnectionManager::instance().getConnectedSdks();
        for (const auto& [key, value] : sdks) {
            collector.withLabels({{"sdk", key}})
                    .addStat(Key::current_external_client_connections, value);
        }

        std::unordered_map<std::string, size_t> allocStats;
        cb::ArenaMalloc::getGlobalStats(allocStats);
        auto allocated = allocStats.find("allocated");
        if (allocated != allocStats.end()) {
            collector.addStat(Key::daemon_memory_allocated, allocated->second);
        }
        auto resident = allocStats.find("resident");
        if (resident != allocStats.end()) {
            collector.addStat(Key::daemon_memory_resident, resident->second);
        }
        collector.addStat(Key::auth_cmds, stats.auth_cmds);
        collector.addStat(Key::auth_errors, stats.auth_errors);
        externalAuthManager->addStats(collector);
    }
}

/// Add global stats related to clocks and time.
void server_clock_stats(const StatCollector& collector) {
    using namespace cb::stats;

    const auto fineClockOverhead =
            cb::estimateClockOverhead<std::chrono::steady_clock>();
    collector.addStat(Key::clock_fine_overhead_ns,
                      fineClockOverhead.overhead.count());
    const auto fineClockResolution =
            cb::estimateClockResolution<std::chrono::steady_clock>();
    collector.addStat(Key::clock_fine_resolution_ns,
                      fineClockResolution.count());

    const auto coarseClockOverhead =
            cb::estimateClockOverhead<folly::chrono::coarse_steady_clock>();
    collector.addStat(Key::clock_coarse_overhead_ns,
                      coarseClockOverhead.overhead.count());
    const auto coarseClockResolution =
            cb::estimateClockResolution<folly::chrono::coarse_steady_clock>();
    collector.addStat(Key::clock_coarse_resolution_ns,
                      coarseClockResolution.count());

    // Note that measurementPeriod is the same for fine and coarse - it's the
    // period of the clock we use to _measure_ the given clock with - and hence
    // just report it once.
    collector.addStat(Key::clock_measurement_period_ns,
                      coarseClockOverhead.measurementPeriod.count());
}

/// add stats aggregated over all buckets
static void server_agg_stats(const StatCollector& collector) {
    using namespace cb::stats;
    auto& timings = BucketManager::instance().aggregatedTimings;
    uint64_t total_mutations = timings.get_aggregated_mutation_stats();
    uint64_t total_retrievals = timings.get_aggregated_retrieval_stats();
    uint64_t total_ops = total_retrievals + total_mutations;
    collector.addStat(Key::cmd_total_sets, total_mutations);
    collector.addStat(Key::cmd_total_gets, total_retrievals);
    collector.addStat(Key::cmd_total_ops, total_ops);

    collector.addStat(Key::threads, Settings::instance().getNumWorkerThreads());

    auto lookup_latency = timings.get_interval_lookup_latency();
    collector.addStat(Key::cmd_lookup_10s_count, lookup_latency.count);
    collector.addStat(Key::cmd_lookup_10s_duration_us,
                      lookup_latency.duration_ns / 1000);

    auto mutation_latency = timings.get_interval_mutation_latency();
    collector.addStat(Key::cmd_mutation_10s_count, mutation_latency.count);
    collector.addStat(Key::cmd_mutation_10s_duration_us,
                      mutation_latency.duration_ns / 1000);
}

/// add stats related to a single bucket
static void server_bucket_stats(const BucketStatCollector& collector,
                                const Bucket& bucket) {
    struct thread_stats thread_stats;
    thread_stats.aggregate(bucket.stats);

    using namespace cb::stats;
    collector.addStat(Key::cmd_get, thread_stats.cmd_get);
    collector.addStat(Key::cmd_set, thread_stats.cmd_set);
    collector.addStat(Key::cmd_flush, thread_stats.cmd_flush);

    collector.addStat(Key::cmd_subdoc_lookup, thread_stats.cmd_subdoc_lookup);
    collector.addStat(Key::cmd_subdoc_mutation,
                      thread_stats.cmd_subdoc_mutation);
    collector.addStat(Key::subdoc_offload_count,
                      thread_stats.subdoc_offload_count);

    collector.addStat(Key::bytes_subdoc_lookup_total,
                      thread_stats.bytes_subdoc_lookup_total);
    collector.addStat(Key::bytes_subdoc_lookup_extracted,
                      thread_stats.bytes_subdoc_lookup_extracted);
    collector.addStat(Key::bytes_subdoc_mutation_total,
                      thread_stats.bytes_subdoc_mutation_total);
    collector.addStat(Key::bytes_subdoc_mutation_inserted,
                      thread_stats.bytes_subdoc_mutation_inserted);

    collector.addStat(Key::stat_timings_mem_usage,
                      bucket.statTimings.getMemFootPrint());

    // bucket specific totals
    auto& current_bucket_timings = bucket.timings;
    uint64_t mutations = current_bucket_timings.get_aggregated_mutation_stats();
    uint64_t lookups = current_bucket_timings.get_aggregated_retrieval_stats();
    collector.addStat(Key::cmd_mutation, mutations);
    collector.addStat(Key::cmd_lookup, lookups);

    collector.addStat(Key::get_hits, thread_stats.get_hits);
    collector.addStat(Key::get_misses, thread_stats.get_misses);
    collector.addStat(Key::delete_misses, thread_stats.delete_misses);
    collector.addStat(Key::delete_hits, thread_stats.delete_hits);
    collector.addStat(Key::incr_misses, thread_stats.incr_misses);
    collector.addStat(Key::incr_hits, thread_stats.incr_hits);
    collector.addStat(Key::decr_misses, thread_stats.decr_misses);
    collector.addStat(Key::decr_hits, thread_stats.decr_hits);
    collector.addStat(Key::cas_misses, thread_stats.cas_misses);
    collector.addStat(Key::cas_hits, thread_stats.cas_hits);
    collector.addStat(Key::cas_badval, thread_stats.cas_badval);
    collector.addStat(Key::bytes_read, thread_stats.bytes_read);
    collector.addStat(Key::bytes_written, thread_stats.bytes_written);
    collector.addStat(Key::conn_yields, thread_stats.conn_yields);
    collector.addStat(Key::conn_timeslice_yields,
                      thread_stats.conn_timeslice_yields);
    collector.addStat(Key::subdoc_update_races,
                      thread_stats.subdoc_update_races);

    collector.addStat(Key::cmd_lock, thread_stats.cmd_lock);
    collector.addStat(Key::lock_errors, thread_stats.lock_errors);

    auto& respCounters = bucket.responseCounters;
    // Ignore success responses by starting from begin + 1
    uint64_t total_resp_errors = std::accumulate(
            std::begin(respCounters) + 1, std::end(respCounters), uint64_t(0));
    collector.addStat(Key::total_resp_errors, total_resp_errors);

    collector.addStat(Key::clients, bucket.clients);
    collector.addStat(Key::items_in_transit, bucket.items_in_transit);
}

/**
 * Add timing stats related to a single bucket.
 *
 * Adds per-opcode timing histograms to the provided collector.
 * Only opcodes which have actually been used will be included in the
 * collector.
 *
 */
void server_bucket_timing_stats(const BucketStatCollector& collector,
                                const Timings& timings) {
    using namespace cb::mcbp;

    for (uint8_t opcode = 0; opcode < uint8_t(ClientOpcode::Invalid);
         opcode++) {
        if (!is_supported_opcode(ClientOpcode(opcode))) {
            continue;
        }
        auto* histPtr = timings.get_timing_histogram(opcode);
        // The histogram is created when the op is first seen.
        // If the histogram has not been created, add an empty histogram
        // to the stat collector.

        if (histPtr && histPtr->getValueCount() > 0) {
            collector.withLabels({{"opcode", to_string(ClientOpcode(opcode))}})
                    .addStat(cb::stats::Key::cmd_duration, *histPtr);
        }
    }
}

// Add timing stats related to external authentication provider.
void external_auth_timing_stats(const BucketStatCollector& collector) {
    using namespace cb::mcbp;
    if (externalAuthManager->authorizationResponseTimes.getValueCount() > 0) {
        collector.addStat(cb::stats::Key::auth_external_authorization_duration,
                          externalAuthManager->authorizationResponseTimes);
    }
    if (externalAuthManager->authenticationResponseTimes.getValueCount() > 0) {
        collector.addStat(cb::stats::Key::auth_external_authentication_duration,
                          externalAuthManager->authenticationResponseTimes);
    }
}

/// add global, aggregated and bucket specific stats
cb::engine_errc server_stats(const StatCollector& collector,
                             const Bucket& bucket) {
    try {
        server_global_stats(collector);
        if (collector.allowPrivilegedStats()) {
            server_agg_stats(collector);
        }
        auto bucketC = collector.forBucket(bucket.name);
        server_bucket_stats(bucketC, bucket);
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
    return cb::engine_errc::success;
}

cb::engine_errc server_prometheus_stats(
        const PrometheusStatCollector& collector,
        cb::prometheus::MetricGroup metricGroup) {
    // prefix all "normal" KV metrics with a short string indicating the
    // service of origin - "kv_".
    // Only metering metrics are exposed without this, for consistency with
    // other services.
    auto kvCollector =
            collector.withPrefix(std::string(cb::prometheus::kvPrefix));
    try {
        using cb::prometheus::MetricGroup;
        // do global stats
        if (metricGroup == MetricGroup::Low) {
            server_global_stats(kvCollector);
            stats_audit(kvCollector);
            if (cb::serverless::isEnabled()) {
                // include all metering metrics, with the "kv_" prefix
                server_prometheus_metering(kvCollector);
                // MB-56934: Temporarily add all metering metrics
                // again, _without_ the "kv_" prefix.
                // This is to allow a transitional period, until consumers
                // have moved to the prefixed version.
                server_prometheus_metering(collector);
            }
        } else {
            try {
                auto instance = sigar::SigarIface::New();
                instance->iterate_threads([&kvCollector](auto tid,
                                                         auto name,
                                                         auto user,
                                                         auto system) {
                    auto thread_pool = get_thread_pool_name(name);
                    kvCollector.addStat(cb::stats::Key::thread_cpu_usage,
                                        user,
                                        {{"tid", std::to_string(tid)},
                                         {"thread_name", name},
                                         {"thread_pool", thread_pool},
                                         {"domain", "user"}});
                    kvCollector.addStat(cb::stats::Key::thread_cpu_usage,
                                        system,
                                        {{"tid", std::to_string(tid)},
                                         {"thread_name", name},
                                         {"thread_pool", thread_pool},
                                         {"domain", "system"}});
                });
            } catch (const std::exception& e) {
                LOG_WARNING_CTX("sigar::iterate_threads", {"error", e.what()});
            }
        }
        BucketManager::instance().forEach([&kvCollector,
                                           metricGroup](Bucket& bucket) {
            if (bucket.type == BucketType::ClusterConfigOnly) {
                // skip config-only buckets but keep the no-bucket with stats
                // not associated with a bucket
                return true;
            }
            auto bucketC = kvCollector.forBucket(bucket.name);

            if (bucket.type == BucketType::NoBucket) {
                // collect only opcode timings and continue checking buckets
                if (metricGroup != MetricGroup::Low) {
                    server_bucket_timing_stats(bucketC, bucket.timings);
                    external_auth_timing_stats(bucketC);
                }
                return true;
            }

            // do engine stats
            const auto err = bucket.getEngine().get_prometheus_stats(
                    bucketC, metricGroup);
            if (err != cb::engine_errc::success) {
                LOG_WARNING_CTX(
                        "server_prometheus_stats(): Failed to get prometheus "
                        "stats",
                        {"bucket", bucket.name},
                        {"error", err});
            }

            if (metricGroup == MetricGroup::Low) {
                // do memcached per-bucket stats
                server_bucket_stats(bucketC, bucket);
            } else {
                // do memcached timings stats
                server_bucket_timing_stats(bucketC, bucket.timings);
            }

            // continue checking buckets
            return true;
        });

    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
    return cb::engine_errc::success;
}

cb::engine_errc server_prometheus_stats_low(
        const PrometheusStatCollector& collector) {
    return server_prometheus_stats(collector, cb::prometheus::MetricGroup::Low);
}

cb::engine_errc server_prometheus_stats_high(
        const PrometheusStatCollector& collector) {
    return server_prometheus_stats(collector,
                                   cb::prometheus::MetricGroup::High);
}

cb::engine_errc server_prometheus_metering(
        const PrometheusStatCollector& collector) {
    try {
        using namespace cb::stats;
        using namespace std::chrono;
        // add the memcached start time (epoch) as the most recent time
        // the metering metrics were zeroed by service restart.
        // Note: The counters can still be reset by bucket recreation,
        // without this timestamp advancing.
        // For now, this is service-restart for consistency with the
        // metering spec and other impls.
        collector.addStat(Key::boot_timestamp,
                          double(mc_time_convert_to_abs_time(0)));
        // add per bucket metering metrics
        BucketManager::instance().forEach([&collector](Bucket& bucket) {
            if (bucket.type == BucketType::NoBucket ||
                bucket.type == BucketType::ClusterConfigOnly) {
                // skip the initial bucket with aggregated stats and config-only
                // buckets
                return true;
            }
            auto bucketC = collector.forBucket(bucket.name);

            bucket.addMeteringMetrics(bucketC);

            // continue checking buckets
            return true;
        });

    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
    return cb::engine_errc::success;
}
