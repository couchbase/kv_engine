/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <relaxed_atomic.h>
#include <vector>

/// Stats collected on a per thread base we want to collect with high
/// resolution
struct HighResolutionThreadStats {
    HighResolutionThreadStats() {
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
        conn_timeslice_yields = 0;
        cmd_subdoc_lookup = 0;
        cmd_subdoc_mutation = 0;
        subdoc_offload_count = 0;
        cmd_lock = 0;
        lock_errors = 0;
        subdoc_update_races = 0;

        bytes_subdoc_lookup_total = 0;
        bytes_subdoc_lookup_extracted = 0;
        bytes_subdoc_mutation_total = 0;
        bytes_subdoc_mutation_inserted = 0;
    }

    HighResolutionThreadStats& operator+=(
            const HighResolutionThreadStats& other) {
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
        conn_timeslice_yields += other.conn_timeslice_yields;
        cmd_subdoc_lookup += other.cmd_subdoc_lookup;
        cmd_subdoc_mutation += other.cmd_subdoc_mutation;
        subdoc_offload_count += other.subdoc_offload_count;

        cmd_lock += other.cmd_lock;
        lock_errors += other.lock_errors;

        bytes_subdoc_lookup_total += other.bytes_subdoc_lookup_total;
        bytes_subdoc_lookup_extracted += other.bytes_subdoc_lookup_extracted;
        bytes_subdoc_mutation_total += other.bytes_subdoc_mutation_total;
        bytes_subdoc_mutation_inserted += other.bytes_subdoc_mutation_inserted;
        subdoc_update_races += other.subdoc_update_races;

        return *this;
    }

    void aggregate(const std::vector<HighResolutionThreadStats>& thread_stats) {
        for (const auto& ii : thread_stats) {
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
    cb::RelaxedAtomic<uint64_t> conn_timeslice_yields;
    /* # of subdoc lookup commands (GET/EXISTS/MULTI_LOOKUP) */
    cb::RelaxedAtomic<uint64_t> cmd_subdoc_lookup;
    /* # of subdoc mutation commands */
    cb::RelaxedAtomic<uint64_t> cmd_subdoc_mutation;
    /// The number of subdoc operations run on NonIO threads
    cb::RelaxedAtomic<uint64_t> subdoc_offload_count;

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

    /// The number of races updating subdoc documents (document changed
    /// between fetching and storing the document)
    cb::RelaxedAtomic<uint64_t> subdoc_update_races;
};

class Connection;
HighResolutionThreadStats& get_high_resolution_thread_stats(Connection& c);

void reset_high_resolution_thread_stats(
        std::vector<HighResolutionThreadStats>& thread_stats);

/*
 *  Macros for managing statistics inside memcached
 */

/* The item must always be called "it" */
#define SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats.slab_op++;

#define THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats.thread_op++;

#define THREAD_GUTS2(conn, thread_stats, slab_op, thread_op) \
    do {                                                     \
        thread_stats.slab_op++;                              \
        thread_stats.thread_op++;                            \
    } while (0)

#define SLAB_THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    SLAB_GUTS(conn, thread_stats, slab_op, thread_op)            \
    THREAD_GUTS(conn, thread_stats, slab_op, thread_op)

#define STATS_INCR1(GUTS, conn, slab_op, thread_op)                     \
    do {                                                                \
        auto& stats_incr1_ts = get_high_resolution_thread_stats(*conn); \
        GUTS(conn, stats_incr1_ts, slab_op, thread_op);                 \
    } while (0)

#define STATS_INCR(conn, op) STATS_INCR1(THREAD_GUTS, conn, op, op)

#define SLAB_INCR(conn, op) STATS_INCR1(SLAB_GUTS, conn, op, op)

#define STATS_TWO(conn, slab_op, thread_op) \
    STATS_INCR1(THREAD_GUTS2, conn, slab_op, thread_op)

#define SLAB_TWO(conn, slab_op, thread_op) \
    STATS_INCR1(SLAB_THREAD_GUTS, conn, slab_op, thread_op)

#define STATS_HIT(conn, op) SLAB_TWO(conn, op##_hits, cmd_##op)

#define STATS_MISS(conn, op) STATS_TWO(conn, op##_misses, cmd_##op)
