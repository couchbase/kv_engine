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

#include <cstdint>
#include "stats_counter.h"

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

        rbufs_allocated = 0;
        rbufs_loaned = 0;
        rbufs_existing = 0;
        wbufs_allocated = 0;
        wbufs_loaned = 0;

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
        rbufs_allocated += other.rbufs_allocated;
        rbufs_loaned += other.rbufs_loaned;
        rbufs_existing += other.rbufs_existing;
        wbufs_allocated += other.wbufs_allocated;
        wbufs_loaned += other.wbufs_loaned;

        iovused_high_watermark.setIfGreater(other.iovused_high_watermark);
        msgused_high_watermark.setIfGreater(other.msgused_high_watermark);

        return *this;
    }

    void aggregate(struct thread_stats *thread_stats, int num) {
        for (int ii = 0; ii < num; ++ii) {
            *this += thread_stats[ii];
        }
    }

    StatsCounter<uint64_t> cmd_get;
    StatsCounter<uint64_t> get_hits;
    StatsCounter<uint64_t> get_misses;
    StatsCounter<uint64_t> cmd_set;
    StatsCounter<uint64_t> delete_hits;
    StatsCounter<uint64_t> cas_hits;
    StatsCounter<uint64_t> cas_badval;
    StatsCounter<uint64_t> delete_misses;
    StatsCounter<uint64_t> incr_misses;
    StatsCounter<uint64_t> decr_misses;
    StatsCounter<uint64_t> incr_hits;
    StatsCounter<uint64_t> decr_hits;
    StatsCounter<uint64_t> cas_misses;
    StatsCounter<uint64_t> bytes_read;
    StatsCounter<uint64_t> bytes_written;
    StatsCounter<uint64_t> cmd_flush;
    StatsCounter<uint64_t> conn_yields; /* # of yields for connections (-R option)*/
    StatsCounter<uint64_t> auth_cmds;
    StatsCounter<uint64_t> auth_errors;
    /* # of read buffers allocated. */
    StatsCounter<uint64_t> rbufs_allocated;
    /* # of read buffers which could be loaned (and hence didn't need to be allocated). */
    StatsCounter<uint64_t> rbufs_loaned;
    /* # of read buffers which already existed (with partial data) on the connection
       (and hence didn't need to be allocated). */
    StatsCounter<uint64_t> rbufs_existing;
    /* # of write buffers allocated. */
    StatsCounter<uint64_t> wbufs_allocated;
    /* # of write buffers which could be loaned (and hence didn't need to be allocated). */
    StatsCounter<uint64_t> wbufs_loaned;

    // Right now we're protecting both the "high watermark" variables
    // between the same mutex
    std::mutex mutex;

    /* Highest value iovsize has got to */
    StatsCounter<int> iovused_high_watermark;
    /* High value conn->msgused has got to */
    StatsCounter<int> msgused_high_watermark;
};

/**
 * Listening port.
 */
struct listening_port {
    int port;
    int curr_conns;
    int maxconns;
};

/**
 * Global stats.
 */
struct stats {
    /** Number of connections used by the server itself (listen ports etc). */
    std::atomic<unsigned int> daemon_conns;

    /** The current number of connections to the server */
    std::atomic<unsigned int> curr_conns;

    /** The total number of connections to the server since start (or reset) */
    std::atomic<unsigned int> total_conns;

    /** The current number of allocated connection objects */
    std::atomic<unsigned int> conn_structs;

    /** The number of times I reject a client */
    std::atomic<uint64_t> rejected_conns;

    // TODO[C++]: convert listening_ports to std::vector.
    struct listening_port *listening_ports;
};



struct thread_stats *get_thread_stats(conn *c);

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

#define STATS_INCR1(GUTS, conn, slab_op, thread_op, key, nkey) { \
    struct thread_stats *thread_stats = get_thread_stats(conn); \
    GUTS(conn, thread_stats, slab_op, thread_op); \
}

#define STATS_INCR(conn, op, key, nkey) \
    STATS_INCR1(THREAD_GUTS, conn, op, op, key, nkey)

#define SLAB_INCR(conn, op, key, nkey) \
    STATS_INCR1(SLAB_GUTS, conn, op, op, key, nkey)

#define STATS_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(THREAD_GUTS2, conn, slab_op, thread_op, key, nkey)

#define SLAB_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(SLAB_THREAD_GUTS, conn, slab_op, thread_op, key, nkey)

#define STATS_HIT(conn, op, key, nkey) \
    SLAB_TWO(conn, op##_hits, cmd_##op, key, nkey)

#define STATS_MISS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_misses, cmd_##op, key, nkey)

/*
 * Set the statistic to the maximum of the current value, and the specified
 * value.
 */
#define STATS_MAX(conn, op, value) get_thread_stats(conn)->op.setIfGreater(value);
