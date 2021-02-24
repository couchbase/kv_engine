/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <relaxed_atomic.h>

#include <memcached/engine_error.h>

#include <cstdint>
#include <mutex>
#include <vector>

namespace cb::prometheus {
// forward declaration
enum class Cardinality;
} // namespace cb::prometheus

/**
 * Stats stored per-thread.
 */
struct thread_stats {
    thread_stats() {
        reset();
    }

    void reset() {
        cmd_get = 0;
        get_hits = 0;
        get_misses = 0;
        cmd_set = 0;
        delete_hits = 0;
        cas_hits = 0;
        cas_badval = 0;
        delete_misses = 0;
        incr_misses = 0;
        decr_misses = 0;
        incr_hits = 0;
        decr_hits = 0;
        cas_misses = 0;
        bytes_written = 0;
        bytes_read = 0;
        cmd_flush = 0;
        conn_yields = 0;
        auth_cmds = 0;
        auth_errors = 0;
        cmd_subdoc_lookup = 0;
        cmd_subdoc_mutation = 0;
        cmd_lock = 0;
        lock_errors = 0;

        bytes_subdoc_lookup_total = 0;
        bytes_subdoc_lookup_extracted = 0;
        bytes_subdoc_mutation_total = 0;
        bytes_subdoc_mutation_inserted = 0;

        iovused_high_watermark = 0;
        msgused_high_watermark = 0;
    }

    thread_stats & operator += (const thread_stats &other) {
        cmd_get += other.cmd_get;
        get_misses += other.get_misses;
        cmd_set += other.cmd_set;
        get_hits += other.get_hits;
        delete_hits += other.delete_hits;
        cas_hits += other.cas_hits;
        cas_badval += other.cas_badval;
        delete_misses += other.delete_misses;
        decr_misses += other.decr_misses;
        incr_misses += other.incr_misses;
        decr_hits += other.decr_hits;
        incr_hits += other.incr_hits;
        cas_misses += other.cas_misses;
        bytes_read += other.bytes_read;
        bytes_written += other.bytes_written;
        cmd_flush += other.cmd_flush;
        conn_yields += other.conn_yields;
        auth_cmds += other.auth_cmds;
        auth_errors += other.auth_errors;
        cmd_subdoc_lookup += other.cmd_subdoc_lookup;
        cmd_subdoc_mutation += other.cmd_subdoc_mutation;

        cmd_lock += other.cmd_lock;
        lock_errors += other.lock_errors;

        bytes_subdoc_lookup_total += other.bytes_subdoc_lookup_total;
        bytes_subdoc_lookup_extracted += other.bytes_subdoc_lookup_extracted;
        bytes_subdoc_mutation_total += other.bytes_subdoc_mutation_total;
        bytes_subdoc_mutation_inserted += other.bytes_subdoc_mutation_inserted;

        iovused_high_watermark.setIfGreater(other.iovused_high_watermark);
        msgused_high_watermark.setIfGreater(other.msgused_high_watermark);

        return *this;
    }

    void aggregate(const std::vector<thread_stats>& thread_stats) {
        for (auto& ii : thread_stats) {
            *this += ii;
        }
    }

    cb::RelaxedAtomic<uint64_t> cmd_get;
    cb::RelaxedAtomic<uint64_t> get_hits;
    cb::RelaxedAtomic<uint64_t> get_misses;
    cb::RelaxedAtomic<uint64_t> cmd_set;
    cb::RelaxedAtomic<uint64_t> delete_hits;
    cb::RelaxedAtomic<uint64_t> cas_hits;
    cb::RelaxedAtomic<uint64_t> cas_badval;
    cb::RelaxedAtomic<uint64_t> delete_misses;
    cb::RelaxedAtomic<uint64_t> incr_misses;
    cb::RelaxedAtomic<uint64_t> decr_misses;
    cb::RelaxedAtomic<uint64_t> incr_hits;
    cb::RelaxedAtomic<uint64_t> decr_hits;
    cb::RelaxedAtomic<uint64_t> cas_misses;
    cb::RelaxedAtomic<uint64_t> bytes_read;
    cb::RelaxedAtomic<uint64_t> bytes_written;
    cb::RelaxedAtomic<uint64_t> cmd_flush;
    cb::RelaxedAtomic<uint64_t>
            conn_yields; /* # of yields for connections (-R option)*/
    cb::RelaxedAtomic<uint64_t> auth_cmds;
    cb::RelaxedAtomic<uint64_t> auth_errors;
    /* # of subdoc lookup commands (GET/EXISTS/MULTI_LOOKUP) */
    cb::RelaxedAtomic<uint64_t> cmd_subdoc_lookup;
    /* # of subdoc mutation commands */
    cb::RelaxedAtomic<uint64_t> cmd_subdoc_mutation;

    /** # of lock commands */
    cb::RelaxedAtomic<uint64_t> cmd_lock;

    /** # of times an operation failed due to accessing a locked item */
    cb::RelaxedAtomic<uint64_t> lock_errors;

    /* # of bytes in the complete document which subdoc lookups searched
       within. Compare with 'bytes_subdoc_lookup_extracted' */
    cb::RelaxedAtomic<uint64_t> bytes_subdoc_lookup_total;
    /* # of bytes extracted during a subdoc lookup operation and sent back to
      the client. */
    cb::RelaxedAtomic<uint64_t> bytes_subdoc_lookup_extracted;

    /* # of bytes in the complete document which subdoc mutations updated.
       Compare with 'bytes_subdoc_mutation_inserted' */
    cb::RelaxedAtomic<uint64_t> bytes_subdoc_mutation_total;
    /* # of bytes inserted during a subdoc mutation operation (which were
       received from the client). */
    cb::RelaxedAtomic<uint64_t> bytes_subdoc_mutation_inserted;

    /* Highest value iovsize has got to */
    cb::RelaxedAtomic<int> iovused_high_watermark;
    /* High value Connection->msgused has got to */
    cb::RelaxedAtomic<int> msgused_high_watermark;
};

/**
 * Global stats.
 */
struct stats {
    /** The current number of connections to the server */
    std::atomic<unsigned int> curr_conns;

    size_t getCurrConnections() const {
        return curr_conns.load(std::memory_order::memory_order_consume);
    }

    /// The number of system connections
    std::atomic<unsigned int> system_conns;

    size_t getSystemConnections() const {
        return system_conns.load(std::memory_order::memory_order_consume);
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
};

extern stats stats;

class Connection;
struct thread_stats* get_thread_stats(Connection* c);

class BucketStatCollector;
class StatCollector;
class Bucket;
cb::engine_errc server_stats(const StatCollector& collector,
                             const Bucket& bucket);

class Timings;
/**
 * Add per-opcode timings stats to the given collector.
 */
void server_bucket_timing_stats(const BucketStatCollector& collector,
                                const Timings& timings);

/**
 * Add stats needed for Prometheus to the given collector.
 *
 * The groups of stats which will be added is dependent on @p cardinality.
 * The cardinality indicates if the incoming request was to the high
 * or low cardinality endpoint.
 */
cb::engine_errc server_prometheus_stats(
        const StatCollector& collector,
        cb::prometheus::Cardinality cardinality);

/*
 *  Macros for managing statistics inside memcached
 */

/* The item must always be called "it" */
#define SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_op++;

#define THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->thread_op++;

#define THREAD_GUTS2(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_op++; \
    thread_stats->thread_op++;

#define SLAB_THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    THREAD_GUTS(conn, thread_stats, slab_op, thread_op)

#define STATS_INCR1(GUTS, conn, slab_op, thread_op) { \
    struct thread_stats *thread_stats = get_thread_stats(conn); \
    GUTS(conn, thread_stats, slab_op, thread_op); \
}

#define STATS_INCR(conn, op) \
    STATS_INCR1(THREAD_GUTS, conn, op, op)

#define SLAB_INCR(conn, op) \
    STATS_INCR1(SLAB_GUTS, conn, op, op)

#define STATS_TWO(conn, slab_op, thread_op) \
    STATS_INCR1(THREAD_GUTS2, conn, slab_op, thread_op)

#define SLAB_TWO(conn, slab_op, thread_op) \
    STATS_INCR1(SLAB_THREAD_GUTS, conn, slab_op, thread_op)

#define STATS_HIT(conn, op) \
    SLAB_TWO(conn, op##_hits, cmd_##op)

#define STATS_MISS(conn, op) \
    STATS_TWO(conn, op##_misses, cmd_##op)

extern std::mutex stats_mutex;
extern char reset_stats_time[80];

class Cookie;
void stats_reset(Cookie& cookie);
