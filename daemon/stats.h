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
#include <relaxed_atomic.h>
#include <mutex>

#include "timing_histogram.h"

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

        bytes_subdoc_lookup_total = 0;
        bytes_subdoc_lookup_extracted = 0;
        bytes_subdoc_mutation_total = 0;
        bytes_subdoc_mutation_inserted = 0;

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
        cmd_subdoc_lookup += other.cmd_subdoc_lookup;
        cmd_subdoc_mutation += other.cmd_subdoc_mutation;

        bytes_subdoc_lookup_total += other.bytes_subdoc_lookup_total;
        bytes_subdoc_lookup_extracted += other.bytes_subdoc_lookup_extracted;
        bytes_subdoc_mutation_total += other.bytes_subdoc_mutation_total;
        bytes_subdoc_mutation_inserted += other.bytes_subdoc_mutation_inserted;

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

    Couchbase::RelaxedAtomic<uint64_t> cmd_get;
    Couchbase::RelaxedAtomic<uint64_t> get_hits;
    Couchbase::RelaxedAtomic<uint64_t> get_misses;
    Couchbase::RelaxedAtomic<uint64_t> cmd_set;
    Couchbase::RelaxedAtomic<uint64_t> delete_hits;
    Couchbase::RelaxedAtomic<uint64_t> cas_hits;
    Couchbase::RelaxedAtomic<uint64_t> cas_badval;
    Couchbase::RelaxedAtomic<uint64_t> delete_misses;
    Couchbase::RelaxedAtomic<uint64_t> incr_misses;
    Couchbase::RelaxedAtomic<uint64_t> decr_misses;
    Couchbase::RelaxedAtomic<uint64_t> incr_hits;
    Couchbase::RelaxedAtomic<uint64_t> decr_hits;
    Couchbase::RelaxedAtomic<uint64_t> cas_misses;
    Couchbase::RelaxedAtomic<uint64_t> bytes_read;
    Couchbase::RelaxedAtomic<uint64_t> bytes_written;
    Couchbase::RelaxedAtomic<uint64_t> cmd_flush;
    Couchbase::RelaxedAtomic<uint64_t> conn_yields; /* # of yields for connections (-R option)*/
    Couchbase::RelaxedAtomic<uint64_t> auth_cmds;
    Couchbase::RelaxedAtomic<uint64_t> auth_errors;
    /* # of subdoc lookup commands (GET/EXISTS/MULTI_LOOKUP) */
    Couchbase::RelaxedAtomic<uint64_t> cmd_subdoc_lookup;
    /* # of subdoc mutation commands */
    Couchbase::RelaxedAtomic<uint64_t> cmd_subdoc_mutation;

    /* # of bytes in the complete document which subdoc lookups searched
       within. Compare with 'bytes_subdoc_lookup_extracted' */
    Couchbase::RelaxedAtomic<uint64_t> bytes_subdoc_lookup_total;
    /* # of bytes extracted during a subdoc lookup operation and sent back to
      the client. */
    Couchbase::RelaxedAtomic<uint64_t> bytes_subdoc_lookup_extracted;

    /* # of bytes in the complete document which subdoc mutations updated.
       Compare with 'bytes_subdoc_mutation_inserted' */
    Couchbase::RelaxedAtomic<uint64_t> bytes_subdoc_mutation_total;
    /* # of bytes inserted during a subdoc mutation operation (which were
       received from the client). */
    Couchbase::RelaxedAtomic<uint64_t> bytes_subdoc_mutation_inserted;

    /* # of read buffers allocated. */
    Couchbase::RelaxedAtomic<uint64_t> rbufs_allocated;
    /* # of read buffers which could be loaned (and hence didn't need to be allocated). */
    Couchbase::RelaxedAtomic<uint64_t> rbufs_loaned;
    /* # of read buffers which already existed (with partial data) on the connection
       (and hence didn't need to be allocated). */
    Couchbase::RelaxedAtomic<uint64_t> rbufs_existing;
    /* # of write buffers allocated. */
    Couchbase::RelaxedAtomic<uint64_t> wbufs_allocated;
    /* # of write buffers which could be loaned (and hence didn't need to be allocated). */
    Couchbase::RelaxedAtomic<uint64_t> wbufs_loaned;

    // Right now we're protecting both the "high watermark" variables
    // between the same mutex
    std::mutex mutex;

    /* Highest value iovsize has got to */
    Couchbase::RelaxedAtomic<int> iovused_high_watermark;
    /* High value Connection->msgused has got to */
    Couchbase::RelaxedAtomic<int> msgused_high_watermark;
};

/**
 * A class representing the properties used by Listening port.
 *
 * This class differs from the "interface" class that it represents
 * an actual port memcached have open. It contains some dynamic and
 * some fixed properties
 */
class ListeningPort {
public:
    ListeningPort(const in_port_t port_,
                  const std::string& host_,
                  bool tcp_nodelay_,
                  int backlog_,
                  bool management_,
                  const Protocol protocol_)
        : port(port_),
          curr_conns(1),
          maxconns(0),
          host(host_),
          backlog(backlog_),
          ipv6(false),
          ipv4(false),
          tcp_nodelay(tcp_nodelay_),
          management(management_),
          protocol(protocol_) {
    }

    /**
     * The actual port number being used by this connection. Please note
     * that you cannot configure the system to use the same port, but different
     * hostnames.
     */
    const in_port_t port;

    /** The current number of connections connected to this port */
    int curr_conns;

    /** The maximum number of connections allowed for this port */
    int maxconns;

    /** The hostname this port is bound to ("*" means all interfaces) */
    const std::string host;

    /** SSL related properties for the port */
    struct ifc_ssl_info {
        ifc_ssl_info()
            : enabled(false) {
        }

        ifc_ssl_info(const ifc_ssl_info& o)
            : enabled(o.enabled),
              key(o.key),
              cert(o.cert) {}

        /** Is ssl enabled or not */
        bool enabled;
        /** The name of the file containing the SSL key */
        std::string key;
        /** The name of the file containing the certificate */
        std::string cert;
    } ssl;

    /** The backlog size before the kernel will deny connect requests */
    int backlog;
    /** Is IPv6 enabled for this port */
    bool ipv6;
    /** Is IPv4 enabled for this port */
    bool ipv4;
    /** Should TCP_NODELAY be enabled or not */
    bool tcp_nodelay;
    // You can't change the purpose of a port dynamically (It is only
    // used during startup
    const bool management;
    // we can't change the protocol dynamically
    const Protocol protocol;
};

/**
 * Global stats.
 */
struct stats {
    /** Number of connections used by the server itself (listen ports etc). */
    Couchbase::RelaxedAtomic<unsigned int> daemon_conns;

    /** The current number of connections to the server */
    std::atomic<unsigned int> curr_conns;

    /** The total number of connections to the server since start (or reset) */
    Couchbase::RelaxedAtomic<unsigned int> total_conns;

    /** The current number of allocated connection objects */
    Couchbase::RelaxedAtomic<unsigned int> conn_structs;

    /** The number of times I reject a client */
    Couchbase::RelaxedAtomic<uint64_t> rejected_conns;

    std::vector<ListeningPort> listening_ports;
};



struct thread_stats *get_thread_stats(Connection *c);

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

/*
 * Set the statistic to the maximum of the current value, and the specified
 * value.
 */
#define STATS_MAX(conn, op, value) get_thread_stats(conn)->op.setIfGreater(value);

extern std::mutex stats_mutex;
extern char reset_stats_time[80];

void stats_reset(const void *cookie);

/**
 * Tap stats (these are only used by the tap thread, so they don't need
 * to be in the threadlocal struct right now...
 */
struct tap_cmd_stats {
    Couchbase::RelaxedAtomic<uint64_t> connect;
    Couchbase::RelaxedAtomic<uint64_t> mutation;
    Couchbase::RelaxedAtomic<uint64_t> checkpoint_start;
    Couchbase::RelaxedAtomic<uint64_t> checkpoint_end;
    Couchbase::RelaxedAtomic<uint64_t> del;
    Couchbase::RelaxedAtomic<uint64_t> flush;
    Couchbase::RelaxedAtomic<uint64_t> opaque;
    Couchbase::RelaxedAtomic<uint64_t> vbucket_set;
};

struct tap_stats {
    struct tap_cmd_stats sent;
    struct tap_cmd_stats received;
};

extern struct tap_stats tap_stats;
