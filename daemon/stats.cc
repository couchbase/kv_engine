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

#include "stats.h"

#include "buckets.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "server_socket.h"
#include "settings.h"

#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus.h>

#include <string_view>

// add global stats
static void server_global_stats(const StatCollector& collector) {
    rel_time_t now = mc_time_get_current_time();

    using namespace cb::stats;
    collector.addStat(Key::uptime, now);
    collector.addStat(Key::stat_reset, (const char*)reset_stats_time);
    collector.addStat(Key::time, mc_time_convert_to_abs_time(now));
    collector.addStat(Key::version, get_server_version());
    collector.addStat(Key::memcached_version, MEMCACHED_VERSION);

    collector.addStat(Key::daemon_connections, ServerSocket::getNumInstances());
    collector.addStat(Key::curr_connections,
                      stats.curr_conns.load(std::memory_order_relaxed));
    collector.addStat(Key::system_connections,
                      stats.system_conns.load(std::memory_order_relaxed));
    collector.addStat(Key::total_connections, stats.total_conns);
    collector.addStat(Key::connection_structures, stats.conn_structs);
}

/// add stats aggregated over all buckets
static void server_agg_stats(const StatCollector& collector) {
    using namespace cb::stats;
    // index 0 contains the aggregated timings for all buckets
    auto& timings = all_buckets[0].timings;
    uint64_t total_mutations = timings.get_aggregated_mutation_stats();
    uint64_t total_retrievals = timings.get_aggregated_retrieval_stats();
    uint64_t total_ops = total_retrievals + total_mutations;
    collector.addStat(Key::cmd_total_sets, total_mutations);
    collector.addStat(Key::cmd_total_gets, total_retrievals);
    collector.addStat(Key::cmd_total_ops, total_ops);

    collector.addStat(Key::rejected_conns, stats.rejected_conns);
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

    collector.addStat(Key::bytes_subdoc_lookup_total,
                      thread_stats.bytes_subdoc_lookup_total);
    collector.addStat(Key::bytes_subdoc_lookup_extracted,
                      thread_stats.bytes_subdoc_lookup_extracted);
    collector.addStat(Key::bytes_subdoc_mutation_total,
                      thread_stats.bytes_subdoc_mutation_total);
    collector.addStat(Key::bytes_subdoc_mutation_inserted,
                      thread_stats.bytes_subdoc_mutation_inserted);

    // bucket specific totals
    auto& current_bucket_timings = bucket.timings;
    uint64_t mutations = current_bucket_timings.get_aggregated_mutation_stats();
    uint64_t lookups = current_bucket_timings.get_aggregated_retrieval_stats();
    collector.addStat(Key::cmd_mutation, mutations);
    collector.addStat(Key::cmd_lookup, lookups);

    collector.addStat(Key::auth_cmds, thread_stats.auth_cmds);
    collector.addStat(Key::auth_errors, thread_stats.auth_errors);
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
    collector.addStat(Key::iovused_high_watermark,
                      thread_stats.iovused_high_watermark);
    collector.addStat(Key::msgused_high_watermark,
                      thread_stats.msgused_high_watermark);

    collector.addStat(Key::cmd_lock, thread_stats.cmd_lock);
    collector.addStat(Key::lock_errors, thread_stats.lock_errors);

    auto& respCounters = bucket.responseCounters;
    // Ignore success responses by starting from begin + 1
    uint64_t total_resp_errors = std::accumulate(
            std::begin(respCounters) + 1, std::end(respCounters), 0);
    collector.addStat(Key::total_resp_errors, total_resp_errors);
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

        const auto& histogram = histPtr ? *histPtr : Hdr1sfMicroSecHistogram();
        collector.withLabels({{"opcode", to_string(ClientOpcode(opcode))}})
                .addStat(cb::stats::Key::cmd_duration, histogram);
    }
}

/// add global, aggregated and bucket specific stats
cb::engine_errc server_stats(const StatCollector& collector,
                             const Bucket& bucket) {
    std::lock_guard<std::mutex> guard(stats_mutex);
    try {
        server_global_stats(collector);
        server_agg_stats(collector);
        auto bucketC = collector.forBucket(bucket.name);
        server_bucket_stats(bucketC, bucket);
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
    return cb::engine_errc::success;
}

cb::engine_errc server_prometheus_stats(
        const StatCollector& collector,
        cb::prometheus::Cardinality cardinality) {
    std::lock_guard<std::mutex> guard(stats_mutex);
    try {
        // do global stats
        if (cardinality == cb::prometheus::Cardinality::Low) {
            server_global_stats(collector);
            stats_audit(collector);
        }
        bucketsForEach(
                [&collector, cardinality](Bucket& bucket, void*) {
                    if (std::string_view(bucket.name).empty()) {
                        // skip the initial bucket with aggregated stats
                        return true;
                    }
                    auto bucketC = collector.forBucket(bucket.name);

                    // do engine stats
                    bucket.getEngine().get_prometheus_stats(bucketC,
                                                            cardinality);

                    if (cardinality == cb::prometheus::Cardinality::Low) {
                        // do memcached per-bucket stats
                        server_bucket_stats(bucketC, bucket);
                    } else {
                        // do memcached timings stats
                        server_bucket_timing_stats(bucketC, bucket.timings);
                    }

                    // continue checking buckets
                    return true;
                },
                nullptr);

    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
    return cb::engine_errc::success;
}