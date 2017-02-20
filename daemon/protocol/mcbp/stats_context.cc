/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "config.h"
#include "engine_wrapper.h"
#include "stats_context.h"

#include <daemon/connections.h>
#include <daemon/debug_helpers.h>
#include <daemon/mc_time.h>
#include <daemon/mcbp.h>
#include <daemon/runtime.h>
#include <memcached/audit_interface.h>
#include <platform/checked_snprintf.h>

// Generic add_stat<T>. Uses std::to_string which requires heap allocation.
template<typename T>
void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, const T& val) {
    std::string value = std::to_string(val);
    add_stat_callback(name, uint16_t(strlen(name)),
                      value.c_str(), uint32_t(value.length()), cookie);
}

// Specializations for common, integer types. Uses stack buffer for
// int-to-string conversion.
void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, int32_t val) {
    char buf[16];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRId32, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING(nullptr, "add_stat failed to add stat for %s", name);
    } else {
        add_stat_callback(name, uint16_t(strlen(name)),
                          buf, uint32_t(len), cookie);
    }
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, uint32_t val) {
    char buf[16];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRIu32, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING(nullptr, "add_stat failed to add stat for %s", name);
    } else {
        add_stat_callback(name, uint16_t(strlen(name)),
                          buf, uint32_t(len), cookie);
    }
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, int64_t val) {
    char buf[32];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRId64, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING(nullptr, "add_stat failed to add stat for %s", name);
    } else {
        add_stat_callback(name, uint16_t(strlen(name)),
                          buf, uint32_t(len), cookie);
    }
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, uint64_t val) {
    char buf[32];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRIu64, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING(nullptr, "add_stat failed to add stat for %s", name);
    } else {
        add_stat_callback(name, uint16_t(strlen(name)),
                          buf, uint32_t(len), cookie);
    }
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, const std::string& value) {
    add_stat_callback(name, uint16_t(strlen(name)),
                      value.c_str(), uint32_t(value.length()), cookie);
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, const char* value) {
    add_stat_callback(name, uint16_t(strlen(name)),
                      value, uint32_t(strlen(value)), cookie);
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, const bool value) {
    if (value) {
        add_stat(cookie, add_stat_callback, name, "true");
    } else {
        add_stat(cookie, add_stat_callback, name, "false");
    }
}

/* return server specific stats only */
static ENGINE_ERROR_CODE server_stats(ADD_STAT add_stat_callback,
                                      McbpConnection* c) {
    rel_time_t now = mc_time_get_current_time();

    struct thread_stats thread_stats;
    thread_stats.aggregate(all_buckets[c->getBucketIndex()].stats,
                           settings.getNumWorkerThreads());

    auto* cookie = c->getCookie();

    try {
        std::lock_guard<std::mutex> guard(stats_mutex);

        add_stat(cookie, add_stat_callback, "pid", long(cb_getpid()));
        add_stat(cookie, add_stat_callback, "uptime", now);
        add_stat(cookie, add_stat_callback, "stat_reset",
                 (const char*)reset_stats_time);
        add_stat(cookie, add_stat_callback, "time",
                 mc_time_convert_to_abs_time(now));
        add_stat(cookie, add_stat_callback, "version", get_server_version());
        add_stat(cookie, add_stat_callback, "memcached_version", MEMCACHED_VERSION);
        add_stat(cookie, add_stat_callback, "libevent", event_get_version());
        add_stat(cookie, add_stat_callback, "pointer_size", (8 * sizeof(void*)));

        add_stat(cookie, add_stat_callback, "daemon_connections",
                 stats.daemon_conns);
        add_stat(cookie, add_stat_callback, "curr_connections",
                 stats.curr_conns.load(std::memory_order_relaxed));
        for (auto& instance : stats.listening_ports) {
            std::string key =
                "max_conns_on_port_" + std::to_string(instance.port);
            add_stat(cookie, add_stat_callback, key.c_str(), instance.maxconns);
            key = "curr_conns_on_port_" + std::to_string(instance.port);
            add_stat(cookie, add_stat_callback, key.c_str(), instance.curr_conns);
        }
        add_stat(cookie, add_stat_callback, "total_connections", stats.total_conns);
        add_stat(cookie, add_stat_callback, "connection_structures",
                 stats.conn_structs);
        add_stat(cookie, add_stat_callback, "cmd_get", thread_stats.cmd_get);
        add_stat(cookie, add_stat_callback, "cmd_set", thread_stats.cmd_set);
        add_stat(cookie, add_stat_callback, "cmd_flush", thread_stats.cmd_flush);

        add_stat(cookie, add_stat_callback, "cmd_subdoc_lookup",
                 thread_stats.cmd_subdoc_lookup);
        add_stat(cookie, add_stat_callback, "cmd_subdoc_mutation",
                 thread_stats.cmd_subdoc_mutation);

        add_stat(cookie, add_stat_callback, "bytes_subdoc_lookup_total",
                 thread_stats.bytes_subdoc_lookup_total);
        add_stat(cookie, add_stat_callback, "bytes_subdoc_lookup_extracted",
                 thread_stats.bytes_subdoc_lookup_extracted);
        add_stat(cookie, add_stat_callback, "bytes_subdoc_mutation_total",
                 thread_stats.bytes_subdoc_mutation_total);
        add_stat(cookie, add_stat_callback, "bytes_subdoc_mutation_inserted",
                 thread_stats.bytes_subdoc_mutation_inserted);

        // index 0 contains the aggregated timings for all buckets
        auto& timings = all_buckets[0].timings;
        uint64_t total_mutations = timings.get_aggregated_mutation_stats();
        uint64_t total_retrivals = timings.get_aggregated_retrival_stats();
        uint64_t total_ops = total_retrivals + total_mutations;
        add_stat(cookie, add_stat_callback, "cmd_total_sets", total_mutations);
        add_stat(cookie, add_stat_callback, "cmd_total_gets", total_retrivals);
        add_stat(cookie, add_stat_callback, "cmd_total_ops", total_ops);
        add_stat(cookie, add_stat_callback, "auth_cmds", thread_stats.auth_cmds);
        add_stat(cookie, add_stat_callback, "auth_errors", thread_stats.auth_errors);
        add_stat(cookie, add_stat_callback, "get_hits", thread_stats.get_hits);
        add_stat(cookie, add_stat_callback, "get_misses", thread_stats.get_misses);
        add_stat(cookie, add_stat_callback, "delete_misses",
                 thread_stats.delete_misses);
        add_stat(cookie, add_stat_callback, "delete_hits", thread_stats.delete_hits);
        add_stat(cookie, add_stat_callback, "incr_misses", thread_stats.incr_misses);
        add_stat(cookie, add_stat_callback, "incr_hits", thread_stats.incr_hits);
        add_stat(cookie, add_stat_callback, "decr_misses", thread_stats.decr_misses);
        add_stat(cookie, add_stat_callback, "decr_hits", thread_stats.decr_hits);
        add_stat(cookie, add_stat_callback, "cas_misses", thread_stats.cas_misses);
        add_stat(cookie, add_stat_callback, "cas_hits", thread_stats.cas_hits);
        add_stat(cookie, add_stat_callback, "cas_badval", thread_stats.cas_badval);
        add_stat(cookie, add_stat_callback, "bytes_read", thread_stats.bytes_read);
        add_stat(cookie, add_stat_callback, "bytes_written",
                 thread_stats.bytes_written);
        add_stat(cookie, add_stat_callback, "accepting_conns",
                 is_listen_disabled() ? 0 : 1);
        add_stat(cookie, add_stat_callback, "listen_disabled_num",
                 get_listen_disabled_num());
        add_stat(cookie, add_stat_callback, "rejected_conns", stats.rejected_conns);
        add_stat(cookie, add_stat_callback, "threads", settings.getNumWorkerThreads());
        add_stat(cookie, add_stat_callback, "conn_yields", thread_stats.conn_yields);
        add_stat(cookie, add_stat_callback, "rbufs_allocated",
                 thread_stats.rbufs_allocated);
        add_stat(cookie, add_stat_callback, "rbufs_loaned",
                 thread_stats.rbufs_loaned);
        add_stat(cookie, add_stat_callback, "rbufs_existing",
                 thread_stats.rbufs_existing);
        add_stat(cookie, add_stat_callback, "wbufs_allocated",
                 thread_stats.wbufs_allocated);
        add_stat(cookie, add_stat_callback, "wbufs_loaned",
                 thread_stats.wbufs_loaned);
        add_stat(cookie, add_stat_callback, "iovused_high_watermark",
                 thread_stats.iovused_high_watermark);
        add_stat(cookie, add_stat_callback, "msgused_high_watermark",
                 thread_stats.msgused_high_watermark);

        add_stat(cookie, add_stat_callback, "cmd_lock", thread_stats.cmd_lock);
        add_stat(cookie, add_stat_callback, "lock_errors",
                 thread_stats.lock_errors);

        auto lookup_latency = timings.get_interval_lookup_latency();
        add_stat(cookie, add_stat_callback, "cmd_lookup_10s_count",
                 lookup_latency.count);
        add_stat(cookie, add_stat_callback, "cmd_lookup_10s_duration_us",
                 lookup_latency.duration_ns / 1000);

        auto mutation_latency = timings.get_interval_mutation_latency();
        add_stat(cookie, add_stat_callback, "cmd_mutation_10s_count",
                 mutation_latency.count);
        add_stat(cookie, add_stat_callback, "cmd_mutation_10s_duration_us",
                 mutation_latency.duration_ns / 1000);

    } catch (std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    /*
     * Add tap stats (only if non-zero)
     */
    if (tap_stats.sent.connect) {
        add_stat(cookie, add_stat_callback, "tap_connect_sent",
                 tap_stats.sent.connect);
    }
    if (tap_stats.sent.mutation) {
        add_stat(cookie, add_stat_callback, "tap_mutation_sent",
                 tap_stats.sent.mutation);
    }
    if (tap_stats.sent.checkpoint_start) {
        add_stat(cookie, add_stat_callback, "tap_checkpoint_start_sent",
                 tap_stats.sent.checkpoint_start);
    }
    if (tap_stats.sent.checkpoint_end) {
        add_stat(cookie, add_stat_callback, "tap_checkpoint_end_sent",
                 tap_stats.sent.checkpoint_end);
    }
    if (tap_stats.sent.del) {
        add_stat(cookie, add_stat_callback, "tap_delete_sent", tap_stats.sent.del);
    }
    if (tap_stats.sent.flush) {
        add_stat(cookie, add_stat_callback, "tap_flush_sent", tap_stats.sent.flush);
    }
    if (tap_stats.sent.opaque) {
        add_stat(cookie, add_stat_callback, "tap_opaque_sent",
                 tap_stats.sent.opaque);
    }
    if (tap_stats.sent.vbucket_set) {
        add_stat(cookie, add_stat_callback, "tap_vbucket_set_sent",
                 tap_stats.sent.vbucket_set);
    }
    if (tap_stats.received.connect) {
        add_stat(cookie, add_stat_callback, "tap_connect_received",
                 tap_stats.received.connect);
    }
    if (tap_stats.received.mutation) {
        add_stat(cookie, add_stat_callback, "tap_mutation_received",
                 tap_stats.received.mutation);
    }
    if (tap_stats.received.checkpoint_start) {
        add_stat(cookie, add_stat_callback, "tap_checkpoint_start_received",
                 tap_stats.received.checkpoint_start);
    }
    if (tap_stats.received.checkpoint_end) {
        add_stat(cookie, add_stat_callback, "tap_checkpoint_end_received",
                 tap_stats.received.checkpoint_end);
    }
    if (tap_stats.received.del) {
        add_stat(cookie, add_stat_callback, "tap_delete_received",
                 tap_stats.received.del);
    }
    if (tap_stats.received.flush) {
        add_stat(cookie, add_stat_callback, "tap_flush_received",
                 tap_stats.received.flush);
    }
    if (tap_stats.received.opaque) {
        add_stat(cookie, add_stat_callback, "tap_opaque_received",
                 tap_stats.received.opaque);
    }
    if (tap_stats.received.vbucket_set) {
        add_stat(cookie, add_stat_callback, "tap_vbucket_set_received",
                 tap_stats.received.vbucket_set);
    }
    return ENGINE_SUCCESS;
}

static void process_stat_settings(ADD_STAT add_stat_callback,
                                  McbpConnection* c) {
    if (c == nullptr) {
        throw std::invalid_argument("process_stat_settings: "
                                        "cookie must be non-NULL");
    }
    if (add_stat_callback == nullptr) {
        throw std::invalid_argument("process_stat_settings: "
                                        "add_stat_callback must be non-NULL");
    }
    auto* cookie = c->getCookie();
    add_stat(cookie, add_stat_callback, "maxconns", settings.getMaxconns());

    try {
        for (auto& ifce : stats.listening_ports) {
            char interface[1024];
            int offset;
            if (ifce.host.empty()) {
                offset = checked_snprintf(interface, sizeof(interface),
                                          "interface-*:%u",
                                          ifce.port);
            } else {
                offset = checked_snprintf(interface, sizeof(interface),
                                          "interface-%s:%u",
                                          ifce.host.c_str(),
                                          ifce.port);
            }

            checked_snprintf(interface + offset, sizeof(interface) - offset,
                             "-maxconn");
            add_stat(cookie, add_stat_callback, interface, ifce.maxconns);
            checked_snprintf(interface + offset, sizeof(interface) - offset,
                             "-backlog");
            add_stat(cookie, add_stat_callback, interface, ifce.backlog);
            checked_snprintf(interface + offset, sizeof(interface) - offset,
                             "-ipv4");
            add_stat(cookie, add_stat_callback, interface, ifce.ipv4);
            checked_snprintf(interface + offset, sizeof(interface) - offset,
                             "-ipv6");
            add_stat(cookie, add_stat_callback, interface, ifce.ipv6);

            checked_snprintf(interface + offset, sizeof(interface) - offset,
                             "-tcp_nodelay");
            add_stat(cookie, add_stat_callback, interface, ifce.tcp_nodelay);
            checked_snprintf(interface + offset, sizeof(interface) - offset,
                             "-management");
            add_stat(cookie, add_stat_callback, interface, ifce.management);

            if (ifce.ssl.enabled) {
                checked_snprintf(interface + offset, sizeof(interface) - offset,
                                 "-ssl-pkey");
                add_stat(cookie, add_stat_callback, interface, ifce.ssl.key);
                checked_snprintf(interface + offset, sizeof(interface) - offset,
                                 "-ssl-cert");
                add_stat(cookie, add_stat_callback, interface, ifce.ssl.cert);
            } else {
                checked_snprintf(interface + offset, sizeof(interface) - offset,
                                 "-ssl");
                add_stat(cookie, add_stat_callback, interface, "false");
            }
        }
    } catch (std::exception& error) {
        LOG_WARNING(nullptr, "process_stats_settings: Error building stats: %s",
                    error.what());
    }

    add_stat(cookie, add_stat_callback, "verbosity", settings.getVerbose());
    add_stat(cookie, add_stat_callback, "num_threads", settings.getNumWorkerThreads());
    add_stat(cookie, add_stat_callback, "reqs_per_event_high_priority",
             settings.getRequestsPerEventNotification(EventPriority::High));
    add_stat(cookie, add_stat_callback, "reqs_per_event_med_priority",
             settings.getRequestsPerEventNotification(EventPriority::Medium));
    add_stat(cookie, add_stat_callback, "reqs_per_event_low_priority",
             settings.getRequestsPerEventNotification(EventPriority::Low));
    add_stat(cookie, add_stat_callback, "reqs_per_event_def_priority",
             settings.getRequestsPerEventNotification(EventPriority::Default));
    add_stat(cookie, add_stat_callback, "auth_enabled_sasl", "yes");
    add_stat(cookie, add_stat_callback, "auth_sasl_engine", "cbsasl");

    const char *sasl_mechs = nullptr;
    if (cbsasl_listmech(c->getSaslConn(), nullptr, "(", ",", ")",
                        &sasl_mechs, nullptr, nullptr)  == CBSASL_OK) {
        add_stat(cookie, add_stat_callback, "auth_sasl_mechanisms", sasl_mechs);
    }

    add_stat(cookie, add_stat_callback, "auth_required_sasl",
             settings.isRequireSasl());
    {
        EXTENSION_DAEMON_DESCRIPTOR* ptr;
        for (ptr = settings.extensions.daemons; ptr != NULL; ptr = ptr->next) {
            add_stat(cookie, add_stat_callback, "extension", ptr->get_name());
        }
    }

    add_stat(cookie, add_stat_callback, "logger",
             settings.extensions.logger->get_name());
    {
        EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* ptr;
        for (ptr = settings.extensions.binary; ptr != NULL; ptr = ptr->next) {
            add_stat(cookie, add_stat_callback, "binary_extension", ptr->get_name());
        }
    }

    add_stat(cookie, add_stat_callback, "audit",
             settings.getAuditFile().c_str());

    add_stat(cookie, add_stat_callback, "connection_idle_time",
             std::to_string(settings.getConnectionIdleTime()).c_str());
    add_stat(cookie, add_stat_callback, "datatype",
             settings.isDatatypeSupport() ? "true" : "false");
    add_stat(cookie, add_stat_callback, "dedupe_nmvb_maps",
             settings.isDedupeNmvbMaps() ? "true" : "false");
    add_stat(cookie, add_stat_callback, "max_packet_size",
             std::to_string(settings.getMaxPacketSize()).c_str());
    add_stat(cookie, add_stat_callback, "xattr_enabled",
            settings.isXattrEnabled());
    add_stat(cookie, add_stat_callback, "privilege_debug",
             settings.isPrivilegeDebug());
}

static void append_bin_stats(const char* key, const uint16_t klen,
                             const char* val, const uint32_t vlen,
                             McbpConnection* c) {
    auto& dbuf = c->getDynamicBuffer();
    // We've ensured that there is enough room in the buffer before calling
    // this method
    char* buf = dbuf.getCurrent();

    uint32_t bodylen = klen + vlen;
    protocol_binary_response_header header;

    memset(&header, 0, sizeof(header));
    header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header.response.opcode = PROTOCOL_BINARY_CMD_STAT;
    header.response.keylen = (uint16_t)htons(klen);
    header.response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
    header.response.bodylen = htonl(bodylen);
    header.response.opaque = c->getOpaque();

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (klen > 0) {
        cb_assert(key != NULL);
        memcpy(buf, key, klen);
        buf += klen;
    }

    if (vlen > 0) {
        memcpy(buf, val, vlen);
    }

    dbuf.moveOffset(sizeof(header.response) + bodylen);
}


static void append_stats(const char* key, const uint16_t klen,
                         const char* val, const uint32_t vlen,
                         const void* void_cookie) {
    size_t needed;

    auto* cookie = reinterpret_cast<const Cookie*>(void_cookie);
    if (cookie->connection == nullptr) {
        throw std::logic_error("append_stats: connection can't be null");
    }
    // Using dynamic cast to ensure a coredump when we implement this for
    // Greenstack and fix it
    auto* c = dynamic_cast<McbpConnection*>(cookie->connection);
    needed = vlen + klen + sizeof(protocol_binary_response_header);
    if (!c->growDynamicBuffer(needed)) {
        return;
    }
    append_bin_stats(key, klen, val, vlen, c);
}


/**
 * This is a very slow thing that you shouldn't use in production ;-)
 *
 * @param c the connection to return the details for
 */
static void process_bucket_details(McbpConnection* c) {
    cJSON* obj = cJSON_CreateObject();

    cJSON* array = cJSON_CreateArray();
    for (size_t ii = 0; ii < all_buckets.size(); ++ii) {
        cJSON* o = get_bucket_details(ii);
        if (o != NULL) {
            cJSON_AddItemToArray(array, o);
        }
    }
    cJSON_AddItemToObject(obj, "buckets", array);

    char* stats_str = cJSON_PrintUnformatted(obj);
    append_stats("bucket details", 14, stats_str, uint32_t(strlen(stats_str)),
                 c->getCookie());
    cJSON_Free(stats_str);
    cJSON_Delete(obj);
}

/**
 * Handler for the <code>stats reset</code> command.
 *
 * Clear the global and the connected buckets stats.
 *
 * It is possible to clear a subset of the stats by using its submodule.
 * The following submodules exists:
 * <ul>
 *    <li>timings</li>
 * </ul>
 *
 * @todo I would have assumed that we wanted to clear the stats from
 *       <b>all</b> of the buckets?
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_reset_executor(const std::string& arg,
                                             McbpConnection& connection) {
    if (arg.empty()) {
        stats_reset(connection.getCookie());
        bucket_reset_stats(&connection);
        all_buckets[0].timings.reset();
        all_buckets[connection.getBucketIndex()].timings.reset();
        return ENGINE_SUCCESS;
    } else if (arg == "timings") {
        // Nuke the command timings section for the connected bucket
        all_buckets[connection.getBucketIndex()].timings.reset();
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats settings</code> used to get the current
 * settings.
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_settings_executor(const std::string& arg,
                                                McbpConnection& connection) {

    if (arg.empty()) {
        process_stat_settings(&append_stats, &connection);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats audit</code> used to get statistics from
 * the audit subsystem.
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_audit_executor(const std::string& arg,
                                             McbpConnection& connection) {
    if (arg.empty()) {
        process_auditd_stats(get_audit_handle(),
                             &append_stats,
                             const_cast<void*>(connection.getCookie()));
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}


/**
 * Handler for the <code>stats bucket details</code> used to get information
 * of the buckets (type, state, #clients etc)
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_bucket_details_executor(const std::string& arg,
                                                      McbpConnection& connection) {
    if (arg.empty()) {
        process_bucket_details(&connection);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats aggregate</code>.. probably not used anymore
 * as it gives just a subset of what you'll get from an empty stat.
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_aggregate_executor(const std::string& arg,
                                                 McbpConnection& connection) {
    if (arg.empty()) {
        return server_stats(&append_stats, &connection);
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats connection[ fd]</code> command to retrieve
 * information about connection specific details.
 *
 * @param arg an optional file descriptor representing the connection
 *            object to retrieve information about. If empty dump all.
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_connections_executor(const std::string& arg,
                                                   McbpConnection& connection) {
    int64_t fd = -1;

    if (!arg.empty()) {
        try {
            fd = std::stoll(arg);
        } catch (...) {
            return ENGINE_EINVAL;
        }
    }

    connection_stats(&append_stats, connection.getCookie(), fd);
    return ENGINE_SUCCESS;
}

/**
 * Handler for the <code>stats topkeys</code> command used to retrieve
 * the most popular keys in the attached bucket.
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_topkeys_executor(const std::string& arg,
                                               McbpConnection& connection) {
    if (arg.empty()) {
        auto& bucket = all_buckets[connection.getBucketIndex()];
        return bucket.topkeys->stats(connection.getCookie(),
                                     mc_time_get_current_time(),
                                     append_stats);
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats topkeys</code> command used to retrieve
 * a JSON document containing the most popular keys in the attached bucket.
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_topkeys_json_executor(const std::string& arg,
                                                    McbpConnection& connection) {
    if (arg.empty()) {
        ENGINE_ERROR_CODE ret;

        cJSON* topkeys_doc = cJSON_CreateObject();
        if (topkeys_doc == nullptr) {
            ret = ENGINE_ENOMEM;
        } else {
            auto& bucket = all_buckets[connection.getBucketIndex()];
            ret = bucket.topkeys->json_stats(topkeys_doc,
                                             mc_time_get_current_time());

            if (ret == ENGINE_SUCCESS) {
                char key[] = "topkeys_json";
                char* topkeys_str = cJSON_PrintUnformatted(topkeys_doc);
                if (topkeys_str != nullptr) {
                    append_stats(key, (uint16_t)strlen(key),
                                 topkeys_str, (uint32_t)strlen(topkeys_str),
                                 connection.getCookie());
                    cJSON_Free(topkeys_str);
                } else {
                    ret = ENGINE_ENOMEM;
                }
            }
            cJSON_Delete(topkeys_doc);
        }
        return ret;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats subdoc_execute</code> command used to retrieve
 * information from the subdoc subsystem.
 *
 * @param arg - should be empty
 * @param connection the connection that requested the operation
 */
static ENGINE_ERROR_CODE stat_subdoc_execute_executor(const std::string& arg,
                                                      McbpConnection& connection) {
    if (arg.empty()) {
        const auto index = connection.getBucketIndex();
        std::string json_str;
        if (index == 0) {
            // Aggregrated timings for all buckets.
            TimingHistogram aggregated;
            for (const auto& bucket : all_buckets) {
                aggregated += bucket.subjson_operation_times;
            }
            json_str = aggregated.to_string();
        } else {
            // Timings for a specific bucket.
            auto& bucket = all_buckets[connection.getBucketIndex()];
            json_str = bucket.subjson_operation_times.to_string();
        }
        append_stats(nullptr, 0, json_str.c_str(), json_str.size(),
                     connection.getCookie());
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

ENGINE_ERROR_CODE StatsCommandContext::step() {
    struct stat_handler {
        /**
         * Is this a privileged stat or may it be requested by anyone
         */
        bool privileged;
        /**
         * The callback function to handle the stat request
         */
        ENGINE_ERROR_CODE (*handler)(const std::string &arg,
                                     McbpConnection& connection);
    };

    /**
     * A mapping from all stat subgroups to the callback providing the
     * statistics
     */
    static std::unordered_map<std::string, struct stat_handler> handlers = {
        {"reset", {true, stat_reset_executor}},
        {"settings", {false, stat_settings_executor}},
        {"audit", {true, stat_audit_executor}},
        {"bucket_details", {true, stat_bucket_details_executor}},
        {"aggregate", {false, stat_aggregate_executor}},
        {"connections", {false, stat_connections_executor}},
        {"topkeys", {false, stat_topkeys_executor}},
        {"topkeys_json", {false, stat_topkeys_json_executor}},
        {"subdoc_execute", {false, stat_subdoc_execute_executor}}
    };

    if (settings.getVerbose() > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), connection.getId(),
                                    true, "STATS",
                                    reinterpret_cast<const char*>(key.data()),
                                    key.size()) != -1) {
            LOG_DEBUG(&connection, "%s", buffer);
        }
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (key.empty()) {
        /* request all statistics */
        ret = get_stats({reinterpret_cast<const char*>(key.data()), key.size()});
        if (ret == ENGINE_SUCCESS) {
            ret = server_stats(&append_stats, &connection);
        }
    } else {
        // The raw representing the key
        const std::string statkey{ reinterpret_cast<const char*>(key.data()),
                                   key.size() };

        // Split the key into a command and argument.
        auto index = statkey.find(' ');
        std::string command;
        std::string argument;

        if (index == key.npos) {
            command = statkey;
        } else {
            command = statkey.substr(0, index);
            argument = statkey.substr(++index);
        }

        auto iter = handlers.find(command);
        if (iter == handlers.end()) {
            // This may be specific to the underlying engine
            ret = get_stats({reinterpret_cast<const char*>(key.data()),
                             key.size()});
        } else {
            if (iter->second.privileged) {
                switch (connection.checkPrivilege(cb::rbac::Privilege::Stats)) {
                case cb::rbac::PrivilegeAccess::Ok:
                    ret = iter->second.handler(argument, connection);
                    break;
                case cb::rbac::PrivilegeAccess::Fail:
                    ret = ENGINE_EACCESS;
                    break;
                case cb::rbac::PrivilegeAccess::Stale:
                    ret = ENGINE_AUTH_STALE;
                    break;
                }
            } else {
                ret = iter->second.handler(argument, connection);
            }
        }
    }

    if (ret == ENGINE_SUCCESS) {
        append_stats(nullptr, 0, nullptr, 0, connection.getCookie());
        mcbp_write_and_free(&connection, &connection.getDynamicBuffer());
    }

    return ret;
}

ENGINE_ERROR_CODE StatsCommandContext::get_stats(const cb::const_char_buffer& k) {
    // Use a shorter name to avoid line wrapping..
    auto& conn = connection;
    if (k.empty()) {
        // Some backeds rely on key being nullptr if klen = 0
        return conn.getBucketEngine()->get_stats(conn.getBucketEngineAsV0(),
                                                 conn.getCookie(),
                                                 nullptr, 0, append_stats);
    } else {
        return conn.getBucketEngine()->get_stats(conn.getBucketEngineAsV0(),
                                                 conn.getCookie(),
                                                 k.data(),
                                                 int(k.size()),
                                                 append_stats);

    }
}
