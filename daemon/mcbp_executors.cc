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

#include "mcbp_executors.h"

#include "mcbp.h"
#include "memcached.h"
#include "session_cas.h"
#include "buckets.h"
#include "config_parse.h"
#include "ioctl.h"
#include "runtime.h"
#include "debug_helpers.h"
#include "mcaudit.h"
#include "subdocument.h"
#include "mc_time.h"
#include "connections.h"
#include "mcbp_validators.h"
#include "mcbp_topkeys.h"
#include "enginemap.h"
#include "mcbpdestroybuckettask.h"
#include "sasl_tasks.h"

#include <memcached/audit_interface.h>
#include <snappy-c.h>
#include <utilities/protocol2text.h>

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
} tap_stats;

std::array<bool, 0x100>&  topkey_commands = get_mcbp_topkeys();
std::array<mcbp_package_execute, 0x100>& executors = get_mcbp_executors();

static bool authenticated(McbpConnection* c) {
    bool rv;

    switch (c->getCmd()) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_HELLO:
        rv = true;
        break;
    default:
        rv = c->isAuthenticated();
    }

    if (settings.verbose > 1) {
        LOG_DEBUG(c, "%u: authenticated() in cmd 0x%02x is %s",
                  c->getId(), c->getCmd(), rv ? "true" : "false");
    }

    return rv;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
static void* binary_get_request(McbpConnection* c) {
    char* ret = c->read.curr;
    ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen +
            c->binary_header.request.extlen);

    cb_assert(ret >= c->read.buf);
    return ret;
}

/**
 * get a pointer to the key in this request
 */
static char* binary_get_key(McbpConnection* c) {
    return c->read.curr - (c->binary_header.request.keylen);
}


static void bin_read_chunk(McbpConnection* c, uint32_t chunk) {
    ptrdiff_t offset;
    c->setRlbytes(chunk);

    /* Ok... do we have room for everything in our buffer? */
    offset =
        c->read.curr + sizeof(protocol_binary_request_header) - c->read.buf;
    if (c->getRlbytes() > c->read.size - offset) {
        size_t nsize = c->read.size;
        size_t size = c->getRlbytes() + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->read.size) {
            char* newm;
            if (settings.verbose > 1) {
                LOG_DEBUG(c, "%u: Need to grow buffer from %lu to %lu",
                          c->getId(), (unsigned long)c->read.size,
                          (unsigned long)nsize);
            }
            newm = reinterpret_cast<char*>(realloc(c->read.buf, nsize));
            if (newm == NULL) {
                LOG_WARNING(c, "%u: Failed to grow buffer.. closing connection",
                            c->getId());
                c->setState(conn_closing);
                return;
            }

            c->read.buf = newm;
            /* rcurr should point to the same offset in the packet */
            c->read.curr =
                c->read.buf + offset - sizeof(protocol_binary_request_header);
            c->read.size = (int)nsize;
        }
        if (c->read.buf != c->read.curr) {
            memmove(c->read.buf, c->read.curr, c->read.bytes);
            c->read.curr = c->read.buf;
            if (settings.verbose > 1) {
                LOG_DEBUG(c, "%u: Repack input buffer", c->getId());
            }
        }
    }

    /* preserve the header in the buffer.. */
    c->setRitem(c->read.curr + sizeof(protocol_binary_request_header));
    c->setState(conn_nread);
}

/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(McbpConnection* c) {
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
    LOG_NOTICE(c, "%u: Protocol error (opcode %02x), close connection",
               c->getId(), c->binary_header.request.opcode);
    c->setWriteAndGo(conn_closing);
}

/**
 * Triggers topkeys_update (i.e., increments topkeys stats) if called by a
 * valid operation.
 */
void update_topkeys(const char* key, size_t nkey, McbpConnection* c) {

    if (topkey_commands[c->binary_header.request.opcode]) {
        if (all_buckets[c->getBucketIndex()].topkeys != nullptr) {
            all_buckets[c->getBucketIndex()].topkeys->updateKey(key, nkey,
                                                                mc_time_get_current_time());
        }
    }
}

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
    int len = snprintf(buf, sizeof(buf), "%" PRId32, val);
    add_stat_callback(name, uint16_t(strlen(name)),
                      buf, uint32_t(len), cookie);
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, uint32_t val) {
    char buf[16];
    int len = snprintf(buf, sizeof(buf), "%" PRIu32, val);
    add_stat_callback(name, uint16_t(strlen(name)),
                      buf, uint32_t(len), cookie);
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, int64_t val) {
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%" PRId64, val);
    add_stat_callback(name, uint16_t(strlen(name)),
                      buf, uint32_t(len), cookie);
}

void add_stat(const void* cookie, ADD_STAT add_stat_callback,
              const char* name, uint64_t val) {
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%" PRIu64, val);
    add_stat_callback(name, uint16_t(strlen(name)),
                      buf, uint32_t(len), cookie);
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
#ifdef WIN32
    long pid = GetCurrentProcessId();
#else
    long pid = (long)getpid();
#endif
    rel_time_t now = mc_time_get_current_time();

    struct thread_stats thread_stats;
    thread_stats.aggregate(all_buckets[c->getBucketIndex()].stats,
                           settings.num_threads);

    auto* cookie = c->getCookie();

    try {
        std::lock_guard<std::mutex> guard(stats_mutex);

        add_stat(cookie, add_stat_callback, "pid", pid);
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
        add_stat(cookie, add_stat_callback, "threads", settings.num_threads);
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
    add_stat(cookie, add_stat_callback, "maxconns", settings.maxconns);

    for (auto& ifce : stats.listening_ports) {
        char interface[1024];
        int offset;
        if (ifce.host.empty()) {
            offset = sprintf(interface, "interface-*:%u", ifce.port);
        } else {
            offset = snprintf(interface, sizeof(interface), "interface-%s:%u",
                              ifce.host.c_str(),
                              ifce.port);
        }

        snprintf(interface + offset, sizeof(interface) - offset, "-maxconn");
        add_stat(cookie, add_stat_callback, interface, ifce.maxconns);
        snprintf(interface + offset, sizeof(interface) - offset, "-backlog");
        add_stat(cookie, add_stat_callback, interface, ifce.backlog);
        snprintf(interface + offset, sizeof(interface) - offset, "-ipv4");
        add_stat(cookie, add_stat_callback, interface, ifce.ipv4);
        snprintf(interface + offset, sizeof(interface) - offset, "-ipv6");
        add_stat(cookie, add_stat_callback, interface, ifce.ipv6);

        snprintf(interface + offset, sizeof(interface) - offset,
                 "-tcp_nodelay");
        add_stat(cookie, add_stat_callback, interface, ifce.tcp_nodelay);
        snprintf(interface + offset, sizeof(interface) - offset, "-management");
        add_stat(cookie, add_stat_callback, interface, ifce.management);

        if (ifce.ssl.enabled) {
            snprintf(interface + offset, sizeof(interface) - offset,
                     "-ssl-pkey");
            add_stat(cookie, add_stat_callback, interface, ifce.ssl.key);
            snprintf(interface + offset, sizeof(interface) - offset,
                     "-ssl-cert");
            add_stat(cookie, add_stat_callback, interface, ifce.ssl.cert);
        } else {
            snprintf(interface + offset, sizeof(interface) - offset,
                     "-ssl");
            add_stat(cookie, add_stat_callback, interface, "false");
        }
    }

    add_stat(cookie, add_stat_callback, "verbosity", settings.verbose.load());
    add_stat(cookie, add_stat_callback, "num_threads", settings.num_threads);
    add_stat(cookie, add_stat_callback, "reqs_per_event_high_priority",
             settings.reqs_per_event_high_priority);
    add_stat(cookie, add_stat_callback, "reqs_per_event_med_priority",
             settings.reqs_per_event_med_priority);
    add_stat(cookie, add_stat_callback, "reqs_per_event_low_priority",
             settings.reqs_per_event_low_priority);
    add_stat(cookie, add_stat_callback, "reqs_per_event_def_priority",
             settings.default_reqs_per_event);
    add_stat(cookie, add_stat_callback, "auth_enabled_sasl", "yes");
    add_stat(cookie, add_stat_callback, "auth_sasl_engine", "cbsasl");

    const char *sasl_mechs = nullptr;
    if (cbsasl_listmech(c->getSaslConn(), nullptr, "(", ",", ")",
                        &sasl_mechs, nullptr, nullptr)  == CBSASL_OK) {
        add_stat(cookie, add_stat_callback, "auth_sasl_mechanisms", sasl_mechs);
    }

    add_stat(cookie, add_stat_callback, "auth_required_sasl", settings.require_sasl);
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

    if (settings.config) {
        add_stat(cookie, add_stat_callback, "config", settings.config);
    }

    if (settings.rbac_file) {
        add_stat(cookie, add_stat_callback, "rbac", settings.rbac_file);
    }

    if (settings.audit_file) {
        add_stat(cookie, add_stat_callback, "audit", settings.audit_file);
    }

    add_stat(cookie, add_stat_callback, "connection_idle_time",
             std::to_string(settings.connection_idle_time.load()).c_str());
    add_stat(cookie, add_stat_callback, "datatype",
            settings.datatype ? "true" : "false");
    add_stat(cookie, add_stat_callback, "dedupe_nmvb_maps",
            settings.dedupe_nmvb_maps.load() ? "true" : "false");
    add_stat(cookie, add_stat_callback, "max_packet_size",
             std::to_string(settings.max_packet_size).c_str());
}


static void process_bin_get(McbpConnection* c) {
    item* it;
    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->write.buf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    uint16_t keylen;
    uint32_t bodylen;
    int ii;
    ENGINE_ERROR_CODE ret;
    uint8_t datatype;
    bool need_inflate = false;

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
                                    "GET", key, nkey) != -1) {
            LOG_DEBUG(c, "%s", buffer);
        }
    }

    ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    if (ret == ENGINE_SUCCESS) {
        ret = bucket_get(c, &it, key, (int)nkey,
                         c->binary_header.request.vbucket);
    }

    item_info_holder info;
    info.info.clsid = 0;
    info.info.nvalue = IOV_MAX;

    switch (ret) {
    case ENGINE_SUCCESS: STATS_HIT(c, get, key, nkey);

        if (!bucket_get_item_info(c, it, &info.info)) {
            bucket_release_item(c, it);
            LOG_WARNING(c, "%u: Failed to get item info", c->getId());
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            break;
        }

        datatype = info.info.datatype;
        if (!c->isSupportsDatatype()) {
            if ((datatype & PROTOCOL_BINARY_DATATYPE_COMPRESSED) ==
                PROTOCOL_BINARY_DATATYPE_COMPRESSED) {
                need_inflate = true;
            } else {
                datatype = PROTOCOL_BINARY_RAW_BYTES;
            }
        }

        keylen = 0;
        bodylen = sizeof(rsp->message.body) + info.info.nbytes;

        if ((c->getCmd() == PROTOCOL_BINARY_CMD_GETK) ||
            (c->getCmd() == PROTOCOL_BINARY_CMD_GETKQ)) {
            bodylen += (uint32_t)nkey;
            keylen = (uint16_t)nkey;
        }

        if (need_inflate) {
            if (info.info.nvalue != 1) {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            } else if (mcbp_response_handler(key, keylen,
                                             &info.info.flags, 4,
                                             info.info.value[0].iov_base,
                                             (uint32_t)info.info.value[0].iov_len,
                                             datatype,
                                             PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                             info.info.cas, c->getCookie())) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
                bucket_release_item(c, it);
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            }
        } else {
            if (mcbp_add_header(c, 0, sizeof(rsp->message.body),
                                keylen, bodylen, datatype) == -1) {
                c->setState(conn_closing);
                return;
            }
            rsp->message.header.response.cas = htonll(info.info.cas);

            /* add the flags */
            rsp->message.body.flags = info.info.flags;
            c->addIov(&rsp->message.body, sizeof(rsp->message.body));

            if ((c->getCmd() == PROTOCOL_BINARY_CMD_GETK) ||
                (c->getCmd() == PROTOCOL_BINARY_CMD_GETKQ)) {
                c->addIov(info.info.key, nkey);
            }

            for (ii = 0; ii < info.info.nvalue; ++ii) {
                c->addIov(info.info.value[ii].iov_base,
                          info.info.value[ii].iov_len);
            }
            c->setState(conn_mwrite);
            /* Remember this item so we can garbage collect it later */
            c->setItem(it);
        }
        update_topkeys(key, nkey, c);
        break;
    case ENGINE_KEY_ENOENT: STATS_MISS(c, get, key, nkey);

        MEMCACHED_COMMAND_GET(c->getId(), key, nkey, -1, 0);

        if (c->isNoReply()) {
            c->setState(conn_new_cmd);
        } else {
            if ((c->getCmd() == PROTOCOL_BINARY_CMD_GETK) ||
                (c->getCmd() == PROTOCOL_BINARY_CMD_GETKQ)) {
                char* ofs =
                    c->write.buf + sizeof(protocol_binary_response_header);
                if (mcbp_add_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                                    0, (uint16_t)nkey,
                                    (uint32_t)nkey,
                                    PROTOCOL_BINARY_RAW_BYTES) == -1) {
                    c->setState(conn_closing);
                    return;
                }
                memcpy(ofs, key, nkey);
                c->addIov(ofs, nkey);
                c->setState(conn_mwrite);
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
            }
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
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


void ship_mcbp_tap_log(McbpConnection* c) {
    bool more_data = true;
    bool send_data = false;
    bool disconnect = false;
    item* it;
    uint32_t bodylen;
    int ii = 0;

    if (!c->addMsgHdr(true)) {
        LOG_WARNING(c,
                    "%u: Failed to create output headers. Shutting down tap "
                        "connection", c->getId());
        c->setState(conn_closing);
        return;
    }
    /* @todo add check for buffer overflow of c->write.buf) */
    c->write.bytes = 0;
    c->write.curr = c->write.buf;

    auto tap_iterator = c->getTapIterator();
    do {
        /* @todo fixme! */
        void* engine;
        uint16_t nengine;
        uint8_t ttl;
        uint16_t tap_flags;
        uint32_t seqno;
        uint16_t vbucket;
        tap_event_t event;
        bool inflate = false;
        size_t inflated_length = 0;

        union {
            protocol_binary_request_tap_mutation mutation;
            protocol_binary_request_tap_delete del;
            protocol_binary_request_tap_flush flush;
            protocol_binary_request_tap_opaque opaque;
            protocol_binary_request_noop noop;
        } msg;
        item_info_holder info;

        if (ii++ == 10) {
            break;
        }

        event = tap_iterator(c->getBucketEngineAsV0(), c->getCookie(), &it,
                             &engine, &nengine, &ttl,
                             &tap_flags, &seqno, &vbucket);
        memset(&msg, 0, sizeof(msg));
        msg.opaque.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
        msg.opaque.message.header.request.opaque = htonl(seqno);
        msg.opaque.message.body.tap.enginespecific_length = htons(nengine);
        msg.opaque.message.body.tap.ttl = ttl;
        msg.opaque.message.body.tap.flags = htons(tap_flags);
        msg.opaque.message.header.request.extlen = 8;
        msg.opaque.message.header.request.vbucket = htons(vbucket);
        info.info.nvalue = IOV_MAX;

        switch (event) {
        case TAP_NOOP :
            send_data = true;
            msg.noop.message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
            msg.noop.message.header.request.extlen = 0;
            msg.noop.message.header.request.bodylen = htonl(0);
            memcpy(c->write.curr, msg.noop.bytes, sizeof(msg.noop.bytes));
            c->addIov(c->write.curr, sizeof(msg.noop.bytes));
            c->write.curr += sizeof(msg.noop.bytes);
            c->write.bytes += sizeof(msg.noop.bytes);
            break;
        case TAP_PAUSE :
            more_data = false;
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
        case TAP_MUTATION:
            if (!bucket_get_item_info(c, it, &info.info)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to get item info", c->getId());
                break;
            }

            if (!c->reserveItem(it)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
                break;
            }
            send_data = true;

            if (event == TAP_CHECKPOINT_START) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START;
                tap_stats.sent.checkpoint_start++;
            } else if (event == TAP_CHECKPOINT_END) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END;
                tap_stats.sent.checkpoint_end++;
            } else if (event == TAP_MUTATION) {
                msg.mutation.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_MUTATION;
                tap_stats.sent.mutation++;
            }

            msg.mutation.message.header.request.cas = htonll(info.info.cas);
            msg.mutation.message.header.request.keylen = htons(info.info.nkey);
            msg.mutation.message.header.request.extlen = 16;
            if (c->isSupportsDatatype()) {
                msg.mutation.message.header.request.datatype = info.info.datatype;
            } else {
                switch (info.info.datatype) {
                case 0:
                    break;
                case PROTOCOL_BINARY_DATATYPE_JSON:
                    break;
                case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
                case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                    inflate = true;
                    break;
                default:
                    LOG_WARNING(c,
                                "%u: shipping data with an invalid datatype "
                                    "(stripping info)", c->getId());
                }
                msg.mutation.message.header.request.datatype = 0;
            }

            bodylen = 16 + info.info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                if (inflate) {
                    if (snappy_uncompressed_length
                            (reinterpret_cast<const char*>(info.info.value[0].iov_base),
                             info.info.nbytes, &inflated_length) == SNAPPY_OK) {
                        bodylen += (uint32_t)inflated_length;
                    } else {
                        LOG_WARNING(c,
                                    "<%u Failed to determine inflated size. "
                                        "Sending as compressed",
                                    c->getId());
                        inflate = false;
                        bodylen += info.info.nbytes;
                    }
                } else {
                    bodylen += info.info.nbytes;
                }
            }
            msg.mutation.message.header.request.bodylen = htonl(bodylen);

            if ((tap_flags & TAP_FLAG_NETWORK_BYTE_ORDER) == 0) {
                msg.mutation.message.body.item.flags = htonl(info.info.flags);
            } else {
                msg.mutation.message.body.item.flags = info.info.flags;
            }
            msg.mutation.message.body.item.expiration = htonl(
                info.info.exptime);
            msg.mutation.message.body.tap.enginespecific_length = htons(
                nengine);
            msg.mutation.message.body.tap.ttl = ttl;
            msg.mutation.message.body.tap.flags = htons(tap_flags);
            memcpy(c->write.curr, msg.mutation.bytes,
                   sizeof(msg.mutation.bytes));

            c->addIov(c->write.curr, sizeof(msg.mutation.bytes));
            c->write.curr += sizeof(msg.mutation.bytes);
            c->write.bytes += sizeof(msg.mutation.bytes);

            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                c->addIov(c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }

            c->addIov(info.info.key, info.info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                if (inflate) {
                    char* buf = reinterpret_cast<char*>(malloc(
                        inflated_length));
                    if (buf == NULL) {
                        LOG_WARNING(c,
                                    "%u: FATAL: failed to allocate buffer "
                                        "of size %" PRIu64
                                        " to inflate object into. Shutting "
                                        "down connection",
                                    c->getId(), inflated_length);
                        c->setState(conn_closing);
                        return;
                    }
                    const char* body = reinterpret_cast<const char*>(info.info.value[0].iov_base);
                    size_t input_length = info.info.value[0].iov_len;
                    if (snappy_uncompress(body, input_length,
                                          buf, &inflated_length) == SNAPPY_OK) {
                        if (!c->pushTempAlloc(buf)) {
                            free(buf);
                            LOG_WARNING(c,
                                        "%u: FATAL: failed to allocate space "
                                            "to keep temporary buffer",
                                        c->getId());
                            c->setState(conn_closing);
                            return;
                        }
                        c->addIov(buf, inflated_length);
                    } else {
                        free(buf);
                        LOG_WARNING(c,
                                    "%u: FATAL: failed to inflate object. "
                                        "shutting down connection",
                                    c->getId());
                        c->setState(conn_closing);
                        return;
                    }
                } else {
                    int xx;
                    for (xx = 0; xx < info.info.nvalue; ++xx) {
                        c->addIov(info.info.value[xx].iov_base,
                                  info.info.value[xx].iov_len);
                    }
                }
            }

            break;
        case TAP_DELETION:
            /* This is a delete */
            if (!bucket_get_item_info(c, it, &info.info)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to get item info", c->getId());
                break;
            }

            if (!c->reserveItem(it)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
                break;
            }
            send_data = true;
            msg.del.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_DELETE;
            msg.del.message.header.request.cas = htonll(info.info.cas);
            msg.del.message.header.request.keylen = htons(info.info.nkey);

            bodylen = 8 + info.info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                bodylen += info.info.nbytes;
            }
            msg.del.message.header.request.bodylen = htonl(bodylen);

            memcpy(c->write.curr, msg.del.bytes, sizeof(msg.del.bytes));
            c->addIov(c->write.curr, sizeof(msg.del.bytes));
            c->write.curr += sizeof(msg.del.bytes);
            c->write.bytes += sizeof(msg.del.bytes);

            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                c->addIov(c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }

            c->addIov(info.info.key, info.info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                int xx;
                for (xx = 0; xx < info.info.nvalue; ++xx) {
                    c->addIov(info.info.value[xx].iov_base,
                              info.info.value[xx].iov_len);
                }
            }

            tap_stats.sent.del++;
            break;

        case TAP_DISCONNECT:
            disconnect = true;
            more_data = false;
            break;
        case TAP_VBUCKET_SET:
        case TAP_FLUSH:
        case TAP_OPAQUE:
            send_data = true;

            if (event == TAP_OPAQUE) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_OPAQUE;
                tap_stats.sent.opaque++;
            } else if (event == TAP_FLUSH) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_FLUSH;
                tap_stats.sent.flush++;
            } else if (event == TAP_VBUCKET_SET) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET;
                msg.flush.message.body.tap.flags = htons(tap_flags);
                tap_stats.sent.vbucket_set++;
            }

            msg.flush.message.header.request.bodylen = htonl(8 + nengine);
            memcpy(c->write.curr, msg.flush.bytes, sizeof(msg.flush.bytes));
            c->addIov(c->write.curr, sizeof(msg.flush.bytes));
            c->write.curr += sizeof(msg.flush.bytes);
            c->write.bytes += sizeof(msg.flush.bytes);
            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                c->addIov(c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }
            break;
        default:
            LOG_WARNING(c,
                        "%u: ship_tap_log: event (which is %d) is not a valid "
                            "tap_event_t - closing connection", event);
            c->setState(conn_closing);
            return;
        }
    } while (more_data);

    c->setEwouldblock(false);
    if (send_data) {
        c->setState(conn_mwrite);
        if (disconnect) {
            c->setWriteAndGo(conn_closing);
        } else {
            c->setWriteAndGo(conn_ship_log);
        }
    } else {
        if (disconnect) {
            c->setState(conn_closing);
        } else {
            /* No more items to ship to the slave at this time.. suspend.. */
            if (settings.verbose > 1) {
                LOG_DEBUG(c, "%u: No more items in tap log.. waiting",
                          c->getId());
            }
            c->setEwouldblock(true);
        }
    }
}

static ENGINE_ERROR_CODE default_unknown_command(
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*,
    ENGINE_HANDLE*,
    const void* void_cookie,
    protocol_binary_request_header* request,
    ADD_RESPONSE response) {

    auto* cookie = reinterpret_cast<const Cookie*>(void_cookie);
    if (cookie->connection == nullptr) {
        throw std::logic_error("default_unknown_command: connection can't be null");
    }
    // Using dynamic cast to ensure a coredump when we implement this for
    // Greenstack and fix it
    auto* c = dynamic_cast<McbpConnection*>(cookie->connection);

    if (!c->isSupportsDatatype() &&
        request->request.datatype != PROTOCOL_BINARY_RAW_BYTES) {
        if (response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, c->getCookie())) {
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_DISCONNECT;
        }
    } else {
        return bucket_unknown_command(c, request, response);
    }
}

struct request_lookup {
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* descriptor;
    BINARY_COMMAND_CALLBACK callback;
};

static struct request_lookup request_handlers[0x100];

typedef void (* RESPONSE_HANDLER)(McbpConnection*);

/**
 * A map between the response packets op-code and the function to handle
 * the response message.
 */
static std::array<RESPONSE_HANDLER, 0x100> response_handlers;

void setup_mcbp_lookup_cmd(
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* descriptor,
    uint8_t cmd,
    BINARY_COMMAND_CALLBACK new_handler) {
    request_handlers[cmd].descriptor = descriptor;
    request_handlers[cmd].callback = new_handler;
}

static void process_bin_unknown_packet(McbpConnection* c) {
    char* packet = c->read.curr -
                   (c->binary_header.request.bodylen +
                    sizeof(c->binary_header));

    auto* req = reinterpret_cast<protocol_binary_request_header*>(packet);
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        struct request_lookup* rq =
            request_handlers + c->binary_header.request.opcode;
        ret = rq->callback(rq->descriptor,
                           c->getBucketEngineAsV0(), c->getCookie(), req,
                           mcbp_response_handler);
    }


    switch (ret) {
    case ENGINE_SUCCESS: {
        if (c->getDynamicBuffer().getRoot() != nullptr) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            c->setState(conn_new_cmd);
        }
        const char* key = packet +
                          sizeof(c->binary_header.request) +
                          c->binary_header.request.extlen;
        update_topkeys(key, c->binary_header.request.keylen, c);
        break;
    }
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        /* Release the dynamic buffer.. it may be partial.. */
        c->clearDynamicBuffer();
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void process_bin_tap_connect(McbpConnection* c) {
    TAP_ITERATOR iterator;
    char* packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));
    auto* req = reinterpret_cast<protocol_binary_request_tap_connect*>(packet);
    const char* key = packet + sizeof(req->bytes);
    const char* data = key + c->binary_header.request.keylen;
    uint32_t flags = 0;
    size_t ndata = c->binary_header.request.bodylen -
                   c->binary_header.request.extlen -
                   c->binary_header.request.keylen;

    if (c->binary_header.request.extlen == 4) {
        flags = ntohl(req->message.body.flags);

        if (flags & TAP_CONNECT_FLAG_BACKFILL) {
            /* the userdata has to be at least 8 bytes! */
            if (ndata < 8) {
                LOG_WARNING(c, "%u: ERROR: Invalid tap connect message",
                            c->getId());
                c->setState(conn_closing);
                return;
            }
        }
    } else {
        data -= 4;
        key -= 4;
    }

    if (settings.verbose && c->binary_header.request.keylen > 0) {
        char buffer[1024];
        size_t len = c->binary_header.request.keylen;
        if (len >= sizeof(buffer)) {
            len = sizeof(buffer) - 1;
        }
        memcpy(buffer, key, len);
        buffer[len] = '\0';
        LOG_DEBUG(c, "%u: Trying to connect with named tap connection: <%s>",
                  c->getId(), buffer);
    }

    iterator = c->getBucketEngine()->get_tap_iterator(c->getBucketEngineAsV0(),
                                                      c->getCookie(), key,
                                                      c->binary_header.request.keylen,
                                                      flags, data, ndata);

    if (iterator == NULL) {
        LOG_WARNING(c, "%u: FATAL: The engine does not support tap",
                    c->getId());
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
        c->setWriteAndGo(conn_closing);
    } else {
        c->setPriority(Connection::Priority::High);
        c->setTapIterator(iterator);
        c->setCurrentEvent(EV_WRITE);
        c->setState(conn_ship_log);
    }
}

static void process_bin_tap_packet(tap_event_t event, McbpConnection* c) {
    char* packet;
    uint16_t nengine;
    uint16_t tap_flags;
    uint32_t seqno;
    uint8_t ttl;
    char* engine_specific;
    char* key;
    uint16_t nkey;
    char* data;
    uint32_t flags;
    uint32_t exptime;
    uint32_t ndata;
    ENGINE_ERROR_CODE ret;

    packet = (c->read.curr - (c->binary_header.request.bodylen +
                              sizeof(c->binary_header)));
    auto* tap = reinterpret_cast<protocol_binary_request_tap_no_extras*>(packet);
    nengine = ntohs(tap->message.body.tap.enginespecific_length);
    tap_flags = ntohs(tap->message.body.tap.flags);
    seqno = ntohl(tap->message.header.request.opaque);
    ttl = tap->message.body.tap.ttl;
    engine_specific = packet + sizeof(tap->bytes);
    key = engine_specific + nengine;
    nkey = c->binary_header.request.keylen;
    data = key + nkey;
    flags = 0;
    exptime = 0;
    ndata = c->binary_header.request.bodylen - nengine - nkey - 8;
    ret = c->getAiostat();

    if (ttl == 0) {
        ret = ENGINE_EINVAL;
    } else {
        if (event == TAP_MUTATION || event == TAP_CHECKPOINT_START ||
            event == TAP_CHECKPOINT_END) {
            auto* mutation =
                reinterpret_cast<protocol_binary_request_tap_mutation*>(tap);

            /* engine_specific data in protocol_binary_request_tap_mutation is */
            /* at a different offset than protocol_binary_request_tap_no_extras */
            engine_specific = packet + sizeof(mutation->bytes);

            flags = mutation->message.body.item.flags;
            if ((tap_flags & TAP_FLAG_NETWORK_BYTE_ORDER) == 0) {
                flags = ntohl(flags);
            }

            exptime = ntohl(mutation->message.body.item.expiration);
            key += 8;
            data += 8;
            ndata -= 8;
        }

        if (ret == ENGINE_SUCCESS) {
            uint8_t datatype = c->binary_header.request.datatype;
            if (event == TAP_MUTATION && !c->isSupportsDatatype()) {
                auto* validator = c->getThread()->validator;
                try {
                    if (validator->validate(reinterpret_cast<uint8_t*>(data),
                                            ndata)) {
                        datatype = PROTOCOL_BINARY_DATATYPE_JSON;
                    }
                } catch (std::bad_alloc&) {
                    // @todo send ENOMEM
                    c->setState(conn_closing);
                    return;
                }
            }

            ret = c->getBucketEngine()->tap_notify(c->getBucketEngineAsV0(),
                                                   c->getCookie(),
                                                   engine_specific, nengine,
                                                   ttl - 1, tap_flags,
                                                   event, seqno,
                                                   key, nkey,
                                                   flags, exptime,
                                                   ntohll(
                                                       tap->message.header.request.cas),
                                                   datatype,
                                                   data, ndata,
                                                   c->binary_header.request.vbucket);
        }
    }

    switch (ret) {
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    default:
        if ((tap_flags & TAP_FLAG_ACK) || (ret != ENGINE_SUCCESS)) {
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        } else {
            c->setState(conn_new_cmd);
        }
    }
}

static void process_bin_tap_ack(McbpConnection* c) {
    char* packet;
    uint32_t seqno;
    uint16_t status;
    char* key;
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    packet = (c->read.curr -
              (c->binary_header.request.bodylen + sizeof(c->binary_header)));
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(packet);
    seqno = ntohl(rsp->message.header.response.opaque);
    status = ntohs(rsp->message.header.response.status);
    key = packet + sizeof(rsp->bytes);

    if (c->getBucketEngine()->tap_notify != NULL) {
        ret = c->getBucketEngine()->tap_notify(c->getBucketEngineAsV0(),
                                               c->getCookie(),
                                               NULL, 0,
                                               0, status,
                                               TAP_ACK, seqno, key,
                                               c->binary_header.request.keylen,
                                               0, 0,
                                               0,
                                               c->binary_header.request.datatype,
                                               NULL,
                                               0, 0);
    }

    if (ret == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        c->setState(conn_ship_log);
    }
}

/**
 * We received a noop response.. just ignore it
 */
static void process_bin_noop_response(McbpConnection* c) {
    c->setState(conn_new_cmd);
}

/*******************************************************************************
 **                             DCP MESSAGE PRODUCERS                         **
 ******************************************************************************/

static McbpConnection* cookie2mcbp(const void* void_cookie,
                                   const char* function) {
    const auto * cookie = reinterpret_cast<const Cookie *>(void_cookie);
    if (cookie == nullptr) {
        throw std::invalid_argument(std::string(function) +
                                        ": cookie is nullptr");
    }
    cookie->validate();
    auto* c = dynamic_cast<McbpConnection*>(cookie->connection);
    if (c == nullptr) {
        throw std::invalid_argument(std::string(function) +
                                        ": connection is nullptr");
    }
    return c;
}

static ENGINE_ERROR_CODE dcp_message_get_failover_log(const void* void_cookie,
                                                      uint32_t opaque,
                                                      uint16_t vbucket) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG);

    protocol_binary_request_dcp_get_failover_log packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_stream_req(const void* void_cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                uint32_t flags,
                                                uint64_t start_seqno,
                                                uint64_t end_seqno,
                                                uint64_t vbucket_uuid,
                                                uint64_t snap_start_seqno,
                                                uint64_t snap_end_seqno) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);

    protocol_binary_request_dcp_stream_req packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    packet.message.header.request.extlen = 48;
    packet.message.header.request.bodylen = htonl(48);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    packet.message.body.flags = ntohl(flags);
    packet.message.body.start_seqno = ntohll(start_seqno);
    packet.message.body.end_seqno = ntohll(end_seqno);
    packet.message.body.vbucket_uuid = ntohll(vbucket_uuid);
    packet.message.body.snap_start_seqno = ntohll(snap_start_seqno);
    packet.message.body.snap_end_seqno = ntohll(snap_end_seqno);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_add_stream_response(const void* void_cookie,
                                                         uint32_t opaque,
                                                         uint32_t dialogopaque,
                                                         uint8_t status) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);
    protocol_binary_response_dcp_add_stream packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_ADD_STREAM;
    packet.message.header.response.extlen = 4;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = htonl(4);
    packet.message.header.response.opaque = opaque;
    packet.message.body.opaque = ntohl(dialogopaque);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_marker_response(const void* void_cookie,
                                                     uint32_t opaque,
                                                     uint8_t status) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    protocol_binary_response_dcp_snapshot_marker packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state_response(
    const void* void_cookie,
    uint32_t opaque,
    uint8_t status) {

    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE);
    protocol_binary_response_dcp_set_vbucket_state packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_stream_end(const void* void_cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                uint32_t flags) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_STREAM_END);
    protocol_binary_request_dcp_stream_end packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_END;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.bodylen = htonl(4);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.flags = ntohl(flags);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_marker(const void* void_cookie,
                                            uint32_t opaque,
                                            uint16_t vbucket,
                                            uint64_t start_seqno,
                                            uint64_t end_seqno,
                                            uint32_t flags) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    protocol_binary_request_dcp_snapshot_marker packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.extlen = 20;
    packet.message.header.request.bodylen = htonl(20);
    packet.message.body.start_seqno = htonll(start_seqno);
    packet.message.body.end_seqno = htonll(end_seqno);
    packet.message.body.flags = htonl(flags);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_mutation(const void* void_cookie,
                                              uint32_t opaque,
                                              item* it,
                                              uint16_t vbucket,
                                              uint64_t by_seqno,
                                              uint64_t rev_seqno,
                                              uint32_t lock_time,
                                              const void* meta,
                                              uint16_t nmeta,
                                              uint8_t nru) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_MUTATION);
    item_info_holder info;
    protocol_binary_request_dcp_mutation packet;
    int xx;

    if (c->write.bytes + sizeof(packet.bytes) + nmeta >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    info.info.nvalue = IOV_MAX;

    if (!bucket_get_item_info(c, it, &info.info)) {
        bucket_release_item(c, it);
        LOG_WARNING(c, "%u: Failed to get item info", c->getId());
        return ENGINE_FAILED;
    }

    if (!c->reserveItem(it)) {
        bucket_release_item(c, it);
        LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
        return ENGINE_FAILED;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_MUTATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(info.info.cas);
    packet.message.header.request.keylen = htons(info.info.nkey);
    packet.message.header.request.extlen = 31;
    packet.message.header.request.bodylen = ntohl(
        31 + info.info.nkey + info.info.nbytes + nmeta);
    packet.message.header.request.datatype = info.info.datatype;
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.lock_time = htonl(lock_time);
    packet.message.body.flags = info.info.flags;
    packet.message.body.expiration = htonl(info.info.exptime);
    packet.message.body.nmeta = htons(nmeta);
    packet.message.body.nru = nru;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    c->addIov(info.info.key, info.info.nkey);
    for (xx = 0; xx < info.info.nvalue; ++xx) {
        c->addIov(info.info.value[xx].iov_base, info.info.value[xx].iov_len);
    }

    memcpy(c->write.curr, meta, nmeta);
    c->addIov(c->write.curr, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_deletion(const void* void_cookie,
                                              uint32_t opaque,
                                              const void* key,
                                              uint16_t nkey,
                                              uint64_t cas,
                                              uint16_t vbucket,
                                              uint64_t by_seqno,
                                              uint64_t rev_seqno,
                                              const void* meta,
                                              uint16_t nmeta) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_DELETION);
    protocol_binary_request_dcp_deletion packet;
    if (c->write.bytes + sizeof(packet.bytes) + nkey + nmeta >= c->write.size) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_DELETION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(cas);
    packet.message.header.request.keylen = htons(nkey);
    packet.message.header.request.extlen = 18;
    packet.message.header.request.bodylen = ntohl(18 + nkey + nmeta);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.nmeta = htons(nmeta);

    c->addIov(c->write.curr, sizeof(packet.bytes) + nkey + nmeta);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;
    memcpy(c->write.curr, meta, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_expiration(const void* void_cookie,
                                                uint32_t opaque,
                                                const void* key,
                                                uint16_t nkey,
                                                uint64_t cas,
                                                uint16_t vbucket,
                                                uint64_t by_seqno,
                                                uint64_t rev_seqno,
                                                const void* meta,
                                                uint16_t nmeta) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_EXPIRATION);
    protocol_binary_request_dcp_deletion packet;

    if (c->write.bytes + sizeof(packet.bytes) + nkey + nmeta >= c->write.size) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_EXPIRATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(cas);
    packet.message.header.request.keylen = htons(nkey);
    packet.message.header.request.extlen = 18;
    packet.message.header.request.bodylen = ntohl(18 + nkey + nmeta);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.nmeta = htons(nmeta);

    c->addIov(c->write.curr, sizeof(packet.bytes) + nkey + nmeta);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;
    memcpy(c->write.curr, meta, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_flush(const void* void_cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_FLUSH);
    protocol_binary_request_dcp_flush packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_FLUSH;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state(const void* void_cookie,
                                                       uint32_t opaque,
                                                       uint16_t vbucket,
                                                       vbucket_state_t state) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE);
    protocol_binary_request_dcp_set_vbucket_state packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    if (!is_valid_vbucket_state_t(state)) {
        return ENGINE_EINVAL;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    packet.message.header.request.extlen = 1;
    packet.message.header.request.bodylen = htonl(1);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.state = uint8_t(state);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_noop(const void* void_cookie,
                                          uint32_t opaque) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_NOOP);
    protocol_binary_request_dcp_noop packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_NOOP;
    packet.message.header.request.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_buffer_acknowledgement(const void* void_cookie,
                                                            uint32_t opaque,
                                                            uint16_t vbucket,
                                                            uint32_t buffer_bytes) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT);
    protocol_binary_request_dcp_buffer_acknowledgement packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.bodylen = ntohl(4);
    packet.message.body.buffer_bytes = ntohl(buffer_bytes);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_control(const void* void_cookie,
                                             uint32_t opaque,
                                             const void* key,
                                             uint16_t nkey,
                                             const void* value,
                                             uint32_t nvalue) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_CONTROL);
    protocol_binary_request_dcp_control packet;
    if (c->write.bytes + sizeof(packet.bytes) + nkey + nvalue >=
        c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_CONTROL;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.keylen = ntohs(nkey);
    packet.message.header.request.bodylen = ntohl(nvalue + nkey);

    c->addIov(c->write.curr, sizeof(packet.bytes) + nkey + nvalue);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;

    memcpy(c->write.curr, value, nvalue);
    c->write.curr += nvalue;
    c->write.bytes += nvalue;

    return ENGINE_SUCCESS;
}

void ship_mcbp_dcp_log(McbpConnection* c) {
    static struct dcp_message_producers producers = {
        dcp_message_get_failover_log,
        dcp_message_stream_req,
        dcp_message_add_stream_response,
        dcp_message_marker_response,
        dcp_message_set_vbucket_state_response,
        dcp_message_stream_end,
        dcp_message_marker,
        dcp_message_mutation,
        dcp_message_deletion,
        dcp_message_expiration,
        dcp_message_flush,
        dcp_message_set_vbucket_state,
        dcp_message_noop,
        dcp_message_buffer_acknowledgement,
        dcp_message_control
    };
    ENGINE_ERROR_CODE ret;

    // Begin timing DCP, each dcp callback needs to set the c->cmd for the timing
    // to be recorded.
    c->setStart(gethrtime());

    if (!c->addMsgHdr(true)) {
        if (settings.verbose) {
            LOG_WARNING(c,
                        "%u: Failed to create output headers. Shutting down "
                            "DCP connection",
                        c->getId());
        }
        c->setState(conn_closing);
        return;
    }

    c->write.bytes = 0;
    c->write.curr = c->write.buf;
    c->setEwouldblock(false);
    ret = c->getBucketEngine()->dcp.step(c->getBucketEngineAsV0(), c->getCookie(),
                                         &producers);
    if (ret == ENGINE_SUCCESS) {
        /* the engine don't have more data to send at this moment */
        c->setEwouldblock(true);
    } else if (ret == ENGINE_WANT_MORE) {
        /* The engine got more data it wants to send */
        ret = ENGINE_SUCCESS;
        c->setState(conn_mwrite);
        c->setWriteAndGo(conn_ship_log);
    }

    if (ret != ENGINE_SUCCESS) {
        c->setState(conn_closing);
    }
}

/******************************************************************************
 *                        TAP packet executors                                *
 ******************************************************************************/
static void tap_connect_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->get_tap_iterator == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.connect++;
        c->setState(conn_setup_tap_stream);
    }
}

static void tap_mutation_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.mutation++;
        process_bin_tap_packet(TAP_MUTATION, c);
    }
}

static void tap_delete_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.del++;
        process_bin_tap_packet(TAP_DELETION, c);
    }
}

static void tap_flush_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.flush++;
        process_bin_tap_packet(TAP_FLUSH, c);
    }
}

static void tap_opaque_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.opaque++;
        process_bin_tap_packet(TAP_OPAQUE, c);
    }
}

static void tap_vbucket_set_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.vbucket_set++;
        process_bin_tap_packet(TAP_VBUCKET_SET, c);
    }
}

static void tap_checkpoint_start_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.checkpoint_start++;
        process_bin_tap_packet(TAP_CHECKPOINT_START, c);
    }
}

static void tap_checkpoint_end_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.checkpoint_end++;
        process_bin_tap_packet(TAP_CHECKPOINT_END, c);
    }
}

/*******************************************************************************
 *                         DCP packet executors                                *
 ******************************************************************************/
static void dcp_open_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_open*>(packet);

    if (c->getBucketEngine()->dcp.open == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);
        c->setSupportsDatatype(true);

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.open(c->getBucketEngineAsV0(), c->getCookie(),
                                                 req->message.header.request.opaque,
                                                 ntohl(req->message.body.seqno),
                                                 ntohl(req->message.body.flags),
                                                 (void*)(req->bytes +
                                                         sizeof(req->bytes)),
                                                 ntohs(
                                                     req->message.header.request.keylen));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            audit_dcp_open(c);
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_add_stream_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_add_stream*>(packet);

    if (c->getBucketEngine()->dcp.add_stream == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.add_stream(c->getBucketEngineAsV0(),
                                                       c->getCookie(),
                                                       req->message.header.request.opaque,
                                                       ntohs(
                                                           req->message.header.request.vbucket),
                                                       ntohl(
                                                           req->message.body.flags));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setDCP(true);
            c->setState(conn_ship_log);
            break;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_close_stream_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_close_stream*>(packet);

    if (c->getBucketEngine()->dcp.close_stream == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint32_t opaque = ntohl(req->message.header.request.opaque);
            ret = c->getBucketEngine()->dcp.close_stream(
                c->getBucketEngineAsV0(), c->getCookie(),
                opaque, vbucket);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

/** Callback from the engine adding the response */
static ENGINE_ERROR_CODE add_failover_log(vbucket_failover_t* entries,
                                          size_t nentries,
                                          const void* cookie) {
    ENGINE_ERROR_CODE ret;
    size_t ii;
    for (ii = 0; ii < nentries; ++ii) {
        entries[ii].uuid = htonll(entries[ii].uuid);
        entries[ii].seqno = htonll(entries[ii].seqno);
    }

    if (mcbp_response_handler(NULL, 0, NULL, 0, entries,
                              (uint32_t)(nentries * sizeof(vbucket_failover_t)),
                              0,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                              (void*)cookie)) {
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }

    for (ii = 0; ii < nentries; ++ii) {
        entries[ii].uuid = htonll(entries[ii].uuid);
        entries[ii].seqno = htonll(entries[ii].seqno);
    }

    return ret;
}

static void dcp_get_failover_log_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_get_failover_log*>(packet);

    if (c->getBucketEngine()->dcp.get_failover_log == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.get_failover_log(
                c->getBucketEngineAsV0(), c->getCookie(),
                req->message.header.request.opaque,
                ntohs(req->message.header.request.vbucket),
                add_failover_log);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (c->getDynamicBuffer().getRoot() != nullptr) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            }
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_stream_req_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_stream_req*>(packet);

    if (c->getBucketEngine()->dcp.stream_req == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        uint32_t flags = ntohl(req->message.body.flags);
        uint64_t start_seqno = ntohll(req->message.body.start_seqno);
        uint64_t end_seqno = ntohll(req->message.body.end_seqno);
        uint64_t vbucket_uuid = ntohll(req->message.body.vbucket_uuid);
        uint64_t snap_start_seqno = ntohll(req->message.body.snap_start_seqno);
        uint64_t snap_end_seqno = ntohll(req->message.body.snap_end_seqno);
        uint64_t rollback_seqno;

        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_ROLLBACK) {
            LOG_WARNING(c,
                        "%u: dcp_stream_req_executor: Unexpected AIO stat"
                            " result ROLLBACK. Shutting down DCP connection",
                        c->getId());
            c->setState(conn_closing);
            return;
        }

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.stream_req(c->getBucketEngineAsV0(),
                                                       c->getCookie(),
                                                       flags,
                                                       c->binary_header.request.opaque,
                                                       c->binary_header.request.vbucket,
                                                       start_seqno, end_seqno,
                                                       vbucket_uuid,
                                                       snap_start_seqno,
                                                       snap_end_seqno,
                                                       &rollback_seqno,
                                                       add_failover_log);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setDCP(true);
            c->setPriority(Connection::Priority::Medium);
            if (c->getDynamicBuffer().getRoot() != nullptr) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            }
            break;

        case ENGINE_ROLLBACK:
            rollback_seqno = htonll(rollback_seqno);
            if (mcbp_response_handler(NULL, 0, NULL, 0, &rollback_seqno,
                                      sizeof(rollback_seqno), 0,
                                      PROTOCOL_BINARY_RESPONSE_ROLLBACK, 0,
                                      c->getCookie())) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            }
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_stream_end_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_stream_end*>(packet);

    if (c->getBucketEngine()->dcp.stream_end == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.stream_end(c->getBucketEngineAsV0(),
                                                       c->getCookie(),
                                                       req->message.header.request.opaque,
                                                       ntohs(
                                                           req->message.header.request.vbucket),
                                                       ntohl(
                                                           req->message.body.flags));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_ship_log);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_snapshot_marker_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_snapshot_marker*>(packet);

    if (c->getBucketEngine()->dcp.snapshot_marker == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        uint16_t vbucket = ntohs(req->message.header.request.vbucket);
        uint32_t opaque = req->message.header.request.opaque;
        uint32_t flags = ntohl(req->message.body.flags);
        uint64_t start_seqno = ntohll(req->message.body.start_seqno);
        uint64_t end_seqno = ntohll(req->message.body.end_seqno);

        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.snapshot_marker(
                c->getBucketEngineAsV0(), c->getCookie(),
                opaque, vbucket,
                start_seqno,
                end_seqno, flags);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_ship_log);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_mutation_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_mutation*>(packet);

    if (c->getBucketEngine()->dcp.mutation == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            char* key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            void* value = key + nkey;
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint32_t flags = req->message.body.flags;
            uint8_t datatype = req->message.header.request.datatype;
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            uint32_t expiration = ntohl(req->message.body.expiration);
            uint32_t lock_time = ntohl(req->message.body.lock_time);
            uint16_t nmeta = ntohs(req->message.body.nmeta);
            uint32_t nvalue = ntohl(req->message.header.request.bodylen) - nkey
                              - req->message.header.request.extlen - nmeta;

            ret = c->getBucketEngine()->dcp.mutation(c->getBucketEngineAsV0(),
                                                     c->getCookie(),
                                                     req->message.header.request.opaque,
                                                     key, nkey, value, nvalue,
                                                     cas, vbucket,
                                                     flags, datatype, by_seqno,
                                                     rev_seqno,
                                                     expiration, lock_time,
                                                     (char*)value + nvalue,
                                                     nmeta,
                                                     req->message.body.nru);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_deletion_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_deletion*>(packet);

    if (c->getBucketEngine()->dcp.deletion == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            char* key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            uint16_t nmeta = ntohs(req->message.body.nmeta);

            ret = c->getBucketEngine()->dcp.deletion(c->getBucketEngineAsV0(),
                                                     c->getCookie(),
                                                     req->message.header.request.opaque,
                                                     key, nkey, cas, vbucket,
                                                     by_seqno, rev_seqno,
                                                     key + nkey, nmeta);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_expiration_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_expiration*>(packet);

    if (c->getBucketEngine()->dcp.expiration == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            char* key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            uint16_t nmeta = ntohs(req->message.body.nmeta);

            ret = c->getBucketEngine()->dcp.expiration(c->getBucketEngineAsV0(),
                                                       c->getCookie(),
                                                       req->message.header.request.opaque,
                                                       key, nkey, cas, vbucket,
                                                       by_seqno, rev_seqno,
                                                       key + nkey, nmeta);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_flush_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_flush*>(packet);

    if (c->getBucketEngine()->dcp.flush == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.flush(c->getBucketEngineAsV0(), c->getCookie(),
                                                  req->message.header.request.opaque,
                                                  ntohs(
                                                      req->message.header.request.vbucket));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_set_vbucket_state_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_set_vbucket_state*>(packet);

    if (c->getBucketEngine()->dcp.set_vbucket_state == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            vbucket_state_t state = (vbucket_state_t)req->message.body.state;
            ret = c->getBucketEngine()->dcp.set_vbucket_state(
                c->getBucketEngineAsV0(), c->getCookie(),
                c->binary_header.request.opaque,
                c->binary_header.request.vbucket,
                state);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_ship_log);
            break;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            c->setState(conn_closing);
            break;
        }
    }
}

static void dcp_noop_executor(McbpConnection* c, void* packet) {
    (void)packet;

    if (c->getBucketEngine()->dcp.noop == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.noop(c->getBucketEngineAsV0(), c->getCookie(),
                                                 c->binary_header.request.opaque);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_buffer_acknowledgement_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_buffer_acknowledgement*>(packet);

    if (c->getBucketEngine()->dcp.buffer_acknowledgement == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            uint32_t bbytes;
            memcpy(&bbytes, &req->message.body.buffer_bytes, 4);
            ret = c->getBucketEngine()->dcp.buffer_acknowledgement(
                c->getBucketEngineAsV0(), c->getCookie(),
                c->binary_header.request.opaque,
                c->binary_header.request.vbucket,
                ntohl(bbytes));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            c->setState(conn_new_cmd);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void dcp_control_executor(McbpConnection* c, void* packet) {
    if (c->getBucketEngine()->dcp.control == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            auto* req = reinterpret_cast<protocol_binary_request_dcp_control*>(packet);
            const uint8_t* key = req->bytes + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            const uint8_t* value = key + nkey;
            uint32_t nvalue = ntohl(req->message.header.request.bodylen) - nkey;
            ret = c->getBucketEngine()->dcp.control(c->getBucketEngineAsV0(), c->getCookie(),
                                                    c->binary_header.request.opaque,
                                                    key, nkey, value, nvalue);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
            break;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            break;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        }
    }
}

static void add_set_replace_executor(McbpConnection* c, void* packet,
                                     ENGINE_STORE_OPERATION store_op) {
    auto* req = reinterpret_cast<protocol_binary_request_add*>(packet);
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    uint8_t extlen = req->message.header.request.extlen;
    char* key = (char*)packet + sizeof(req->bytes);
    uint16_t nkey = ntohs(req->message.header.request.keylen);
    uint32_t vlen = ntohl(req->message.header.request.bodylen) - nkey - extlen;
    item_info_holder info;
    info.info.clsid = 0;
    info.info.nvalue = 1;

    if (req->message.header.request.cas != 0) {
        store_op = OPERATION_CAS;
    }

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
                                    memcached_opcode_2_text(store_op), key,
                                    nkey) != -1) {
            LOG_DEBUG(c, "%s", buffer);
        }
    }

    if (c->getItem() == nullptr) {
        item* it;

        if (ret == ENGINE_SUCCESS) {
            rel_time_t expiration = ntohl(req->message.body.expiration);
            ret = c->getBucketEngine()->allocate(c->getBucketEngineAsV0(), c->getCookie(),
                                                 &it, key, nkey, vlen,
                                                 req->message.body.flags,
                                                 expiration,
                                                 req->message.header.request.datatype);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            update_topkeys(key, nkey, c);
            break;
        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            return;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            return;
        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
            return;
        }

        bucket_item_set_cas(c, it, ntohll(req->message.header.request.cas));
        if (!bucket_get_item_info(c, it, &info.info)) {
            bucket_release_item(c, it);
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return;
        }

        c->setItem(it);
        cb_assert(info.info.value[0].iov_len == vlen);
        memcpy(info.info.value[0].iov_base, key + nkey, vlen);

        if (!c->isSupportsDatatype()) {
            auto* validator = c->getThread()->validator;

            try {
                auto* ptr = reinterpret_cast<uint8_t*>(info.info.value[0].iov_base);
                if (validator->validate(ptr, info.info.value[0].iov_len)) {
                    info.info.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
                    if (!bucket_set_item_info(c, it, &info.info)) {
                        LOG_WARNING(c, "%u: Failed to set item info",
                                    c->getId());
                    }
                }
            } catch (std::bad_alloc&) {
                // @todo return error message back to client
                bucket_release_item(c, c->getItem());
                c->setItem(nullptr);
                c->setState(conn_closing);
                return;
            }
        }
    }

    if (ret == ENGINE_SUCCESS) {
        uint64_t cas = c->getCAS();
        ret = bucket_store(c, c->getItem(), &cas, store_op,
                           ntohs(req->message.header.request.vbucket));
        if (ret == ENGINE_SUCCESS) {
            c->setCAS(cas);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        /* Stored */
        if (c->isSupportsMutationExtras()) {
            info.info.nvalue = 1;
            if (!bucket_get_item_info(c, c->getItem(), &info.info)) {
                bucket_release_item(c, c->getItem());
                LOG_WARNING(c, "%u: Failed to get item info", c->getId());
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
                return;
            }
            mutation_descr_t* const extras = (mutation_descr_t*)
                (c->write.buf + sizeof(protocol_binary_response_no_extras));
            extras->vbucket_uuid = htonll(info.info.vbucket_uuid);
            extras->seqno = htonll(info.info.seqno);
            mcbp_write_response(c, extras, sizeof(*extras), 0, sizeof(*extras));
        } else {
            mcbp_write_response(c, NULL, 0, 0, 0);
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_NOT_STORED:
        if (store_op == OPERATION_ADD) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        } else if (store_op == OPERATION_REPLACE) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED);
        }
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }

    if (store_op == OPERATION_CAS) {
        switch (ret) {
        case ENGINE_SUCCESS: SLAB_INCR(c, cas_hits, key, nkey);
            break;
        case ENGINE_KEY_EEXISTS: SLAB_INCR(c, cas_badval, key, nkey);
            break;
        case ENGINE_KEY_ENOENT:
            get_thread_stats(c)->cas_misses++;
            break;
        default:;
        }
    } else if (ret != ENGINE_EWOULDBLOCK) {
        SLAB_INCR(c, cmd_set, key, nkey);
    }

    if (!c->isEwouldblock()) {
        /* release the c->item reference */
        bucket_release_item(c, c->getItem());
        c->setItem(nullptr);
    }
}


static void add_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_ADD);
}

static void addq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_ADD);
}

static void set_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_SET);
}

static void setq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_SET);
}

static void replace_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_REPLACE);
}

static void replaceq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_REPLACE);
}

static void append_prepend_executor(McbpConnection* c,
                                    void* packet,
                                    ENGINE_STORE_OPERATION store_op) {
    auto* req = reinterpret_cast<protocol_binary_request_append*>(packet);
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    char* key = (char*)packet + sizeof(req->bytes);
    uint16_t nkey = ntohs(req->message.header.request.keylen);
    uint32_t vlen = ntohl(req->message.header.request.bodylen) - nkey;
    item_info_holder info;
    info.info.clsid = 0;
    info.info.nvalue = 1;

    if (c->getItem() == NULL) {
        item* it;

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->allocate(c->getBucketEngineAsV0(), c->getCookie(),
                                                 &it, key, nkey,
                                                 vlen, 0, 0,
                                                 req->message.header.request.datatype);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            break;
        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            return;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            return;
        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
            return;
        }

        bucket_item_set_cas(c, it, ntohll(req->message.header.request.cas));
        if (!bucket_get_item_info(c, it, &info.info)) {
            bucket_release_item(c, it);
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return;
        }

        c->setItem(it);
        cb_assert(info.info.value[0].iov_len == vlen);
        memcpy(info.info.value[0].iov_base, key + nkey, vlen);

        if (!c->isSupportsDatatype()) {
            auto* validator = c->getThread()->validator;
            try {
                auto* ptr = reinterpret_cast<uint8_t*>(info.info.value[0].iov_base);
                if (validator->validate(ptr, info.info.value[0].iov_len)) {
                    info.info.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
                    if (!bucket_set_item_info(c, it, &info.info)) {
                        LOG_WARNING(c, "%u: Failed to set item info",
                                    c->getId());
                    }
                }
            } catch (std::bad_alloc&) {
                bucket_release_item(c, c->getItem());
                c->setItem(nullptr);
                c->setState(conn_closing);
                return;
            }
        }
        update_topkeys(key, nkey, c);
    }

    if (ret == ENGINE_SUCCESS) {
        uint64_t cas = c->getCAS();
        ret = bucket_store(c, c->getItem(), &cas, store_op,
                           ntohs(req->message.header.request.vbucket));
        if (ret == ENGINE_SUCCESS) {
            c->setCAS(cas);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        /* Stored */
        if (c->isSupportsMutationExtras()) {
            info.info.nvalue = 1;
            if (!bucket_get_item_info(c, c->getItem(), &info.info)) {
                bucket_release_item(c, c->getItem());
                LOG_WARNING(c, "%u: Failed to get item info", c->getId());
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
                return;
            }
            mutation_descr_t* const extras = (mutation_descr_t*)
                (c->write.buf + sizeof(protocol_binary_response_no_extras));
            extras->vbucket_uuid = htonll(info.info.vbucket_uuid);
            extras->seqno = htonll(info.info.seqno);
            mcbp_write_response(c, extras, sizeof(*extras), 0, sizeof(*extras));
        } else {
            mcbp_write_response(c, NULL, 0, 0, 0);
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }

    if (!c->isEwouldblock()) {
        SLAB_INCR(c, cmd_set, key, nkey);
        /* release the c->item reference */
        bucket_release_item(c, c->getItem());
        c->setItem(nullptr);
    }
}

static void append_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    append_prepend_executor(c, packet, OPERATION_APPEND);
}

static void appendq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    append_prepend_executor(c, packet, OPERATION_APPEND);
}

static void prepend_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    append_prepend_executor(c, packet, OPERATION_PREPEND);
}

static void prependq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    append_prepend_executor(c, packet, OPERATION_PREPEND);
}


static void get_executor(McbpConnection* c, void* packet) {
    (void)packet;

    switch (c->getCmd()) {
    case PROTOCOL_BINARY_CMD_GETQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_GET:
        c->setNoReply(false);
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_GETK:
        c->setNoReply(false);
        break;
    default:
        LOG_WARNING(c,
                    "%u: get_executor: cmd (which is %d) is not a valid GET "
                        "variant - closing connection", c->getCmd());
        c->setState(conn_closing);
        return;
    }

    process_bin_get(c);
}

/**
 * This is a very slow thing that you shouldn't use in production ;-)
 *
 * @param c the connection to return the details for
 */
static void process_bucket_details(McbpConnection* c) {
    cJSON* obj = cJSON_CreateObject();

    cJSON* array = cJSON_CreateArray();
    for (int ii = 0; ii < settings.max_buckets; ++ii) {
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
 * Clear the global and the connected buckets stats
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

static void stat_executor(McbpConnection* c, void*) {
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
        {"reset", {false, stat_reset_executor}},
        {"settings", {false, stat_settings_executor}},
        {"audit", {true, stat_audit_executor}},
        {"bucket_details", {true, stat_bucket_details_executor}},
        {"aggregate", {false, stat_aggregate_executor}},
        {"connections", {false, stat_connections_executor}},
        {"topkeys", {false, stat_topkeys_executor}},
        {"topkeys_json", {false, stat_topkeys_json_executor}},
        {"subdoc_execute", {false, stat_subdoc_execute_executor}}
    };

    // The raw representing the key
    const std::string key(binary_get_key(c), c->binary_header.request.keylen);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
                                    "STATS", key.c_str(), key.size()) != -1) {
            LOG_DEBUG(c, "%s", buffer);
        }
    }

    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        if (key.empty()) {
            /* request all statistics */
            ret = c->getBucketEngine()->get_stats(c->getBucketEngineAsV0(), c->getCookie(),
                                                  nullptr, 0, append_stats);
            if (ret == ENGINE_SUCCESS) {
                ret = server_stats(&append_stats, c);
            }
        } else {
            // Split the key into a command and argument.
            auto index = key.find(' ');
            std::string command;
            std::string argument;

            if (index == key.npos) {
                command = key;
            } else {
                command = key.substr(0, index);
                argument = key.substr(index++);
            }

            auto iter = handlers.find(command);
            if (iter == handlers.end()) {
                // This may be specific to the underlying engine
                ret = c->getBucketEngine()->get_stats(c->getBucketEngineAsV0(),
                                                      c->getCookie(), key.c_str(),
                                                      key.size(),
                                                      append_stats);
            } else {
                if (iter->second.privileged) {
                    if (c->isAdmin()) {
                        ret = iter->second.handler(argument, *c);
                    } else {
                        ret = ENGINE_EACCESS;
                    }
                } else {
                    ret = iter->second.handler(argument, *c);
                }
            }
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        append_stats(NULL, 0, NULL, 0, c->getCookie());
        mcbp_write_and_free(c, &c->getDynamicBuffer());
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void isasl_refresh_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    (void)packet;

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = refresh_cbsasl(c);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_refresh_cbsasl);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void ssl_certs_refresh_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    (void)packet;

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = refresh_ssl_certs(c);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_refresh_ssl_certs);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void verbosity_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_verbosity*>(packet);
    uint32_t level = (uint32_t)ntohl(req->message.body.level);
    if (level > MAX_VERBOSITY_LEVEL) {
        level = MAX_VERBOSITY_LEVEL;
    }
    settings.verbose = (int)level;
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
    mcbp_write_response(c, NULL, 0, 0, 0);
}


static void process_hello_packet_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_hello*>(packet);
    char log_buffer[512];
    int offset = snprintf(log_buffer, sizeof(log_buffer), "HELO ");
    char* key = (char*)packet + sizeof(*req);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t total = (ntohl(req->message.header.request.bodylen) - klen) / 2;
    uint32_t ii;
    char* curr = key + klen;
    uint16_t out[MEMCACHED_TOTAL_HELLO_FEATURES];
    int jj = 0;
    bool tcpdelay_handled = false;
    memset((char*)out, 0, sizeof(out));

    /*
     * Disable all features the hello packet may enable, so that
     * the client can toggle features on/off during a connection
     */
    c->setSupportsDatatype(false);
    c->setSupportsMutationExtras(false);

    if (klen) {
        if (klen > 256) {
            klen = 256;
        }
        log_buffer[offset++] = '[';
        memcpy(log_buffer + offset, key, klen);
        offset += klen;
        log_buffer[offset++] = ']';
        log_buffer[offset++] = ' ';
        log_buffer[offset] = '\0';
    }

    for (ii = 0; ii < total; ++ii) {
        bool added = false;
        uint16_t in;
        /* to avoid alignment */
        memcpy(&in, curr, 2);
        curr += 2;
        in = ntohs(in);

        switch (in) {
        case PROTOCOL_BINARY_FEATURE_TLS:
            /* Not implemented */
            break;
        case PROTOCOL_BINARY_FEATURE_DATATYPE:
            if (settings.datatype && !c->isSupportsDatatype()) {
                c->setSupportsDatatype(true);
                added = true;
            }
            break;

        case PROTOCOL_BINARY_FEATURE_TCPNODELAY:
        case PROTOCOL_BINARY_FEATURE_TCPDELAY:
            if (!tcpdelay_handled) {
                c->setTcpNoDelay(in == PROTOCOL_BINARY_FEATURE_TCPNODELAY);
                tcpdelay_handled = true;
                added = true;
            }
            break;

        case PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO:
            if (!c->isSupportsMutationExtras()) {
                c->setSupportsMutationExtras(true);
                added = true;
            }
            break;
        }

        if (added) {
            out[jj++] = htons(in);
            offset += snprintf(log_buffer + offset,
                               sizeof(log_buffer) - offset,
                               "%s, ",
                               protocol_feature_2_text(in));
        }
    }

    if (jj == 0) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_response_handler(NULL, 0, NULL, 0, out, 2 * jj,
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                              0, c->getCookie());
        mcbp_write_and_free(c, &c->getDynamicBuffer());
    }


    /* Trim off the whitespace and potentially trailing commas */
    --offset;
    while (offset > 0 &&
           (isspace(log_buffer[offset]) || log_buffer[offset] == ',')) {
        log_buffer[offset] = '\0';
        --offset;
    }
    LOG_NOTICE(c, "%u: %s %s", c->getId(), log_buffer,
               c->getDescription().c_str());
}

static void version_executor(McbpConnection* c, void*) {
    mcbp_write_response(c, get_server_version(), 0, 0,
                        (uint32_t)strlen(get_server_version()));
}

static void quit_executor(McbpConnection* c, void*) {
    mcbp_write_response(c, NULL, 0, 0, 0);
    c->setWriteAndGo(conn_closing);
}

static void quitq_executor(McbpConnection* c, void*) {
    c->setState(conn_closing);
}

static void sasl_list_mech_executor(McbpConnection* c, void*) {
    const char* result_string = NULL;
    unsigned int string_length = 0;

    auto ret = cbsasl_listmech(c->getSaslConn(), nullptr, nullptr, " ",
                               nullptr, &result_string, &string_length,
                               nullptr);

    if (ret == CBSASL_OK) {
        mcbp_write_response(c, (char*)result_string, 0, 0, string_length);
    } else  {
        /* Perhaps there's a better error for this... */
        LOG_WARNING(c, "%u: Failed to list SASL mechanisms: %s", c->getId(),
                    cbsasl_strerror(c->getSaslConn(), ret));
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);
        return;
    }
}

static void sasl_auth_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    const char *ptr = reinterpret_cast<char*>(req->bytes) + sizeof(req->bytes);

    std::string mechanism(ptr, nkey);
    std::string challenge(ptr + nkey, vlen);

    LOG_DEBUG(c, "%u: SASL auth with mech: '%s' with %d bytes of data",
              c->getId(), mechanism.c_str(), vlen);

    std::shared_ptr<Task> task;

    if (c->getCmd() == PROTOCOL_BINARY_CMD_SASL_AUTH) {
        task = std::make_shared<StartSaslAuthTask>(c->getCookieObject(), *c, mechanism, challenge);
    } else {
        task = std::make_shared<StepSaslAuthTask>(c->getCookieObject(), *c, mechanism, challenge);
    }
    c->setCommandContext(new SaslCommandContext(task));

    c->setEwouldblock(true);
    c->setState(conn_sasl_auth);
    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task, true);
}

static void noop_executor(McbpConnection* c, void*) {
    mcbp_write_response(c, NULL, 0, 0, 0);
}

static void flush_executor(McbpConnection* c, void*) {
    ENGINE_ERROR_CODE ret;
    if (c->getCmd() == PROTOCOL_BINARY_CMD_FLUSHQ) {
        c->setNoReply(true);
    }

    LOG_NOTICE(c, "%u: flush", c->getId());

    ret = c->getBucketEngine()->flush(c->getBucketEngineAsV0(), c->getCookie(), 0);
    switch (ret) {
    case ENGINE_SUCCESS:
        audit_bucket_flush(c, all_buckets[c->getBucketIndex()].name);
        get_thread_stats(c)->cmd_flush++;
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_flush);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void delete_executor(McbpConnection* c, void*) {
    if (c->getCmd() == PROTOCOL_BINARY_CMD_DELETEQ) {
        c->setNoReply(true);
    }

    ENGINE_ERROR_CODE ret;
    auto* req = reinterpret_cast<protocol_binary_request_delete*>
    (binary_get_request(c));
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    uint64_t cas = ntohll(req->message.header.request.cas);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
                                    "DELETE", key, nkey) != -1) {
            LOG_DEBUG(c, "%s\n", buffer);
        }
    }

    ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    mutation_descr_t mut_info;
    if (ret == ENGINE_SUCCESS) {
        ret = c->getBucketEngine()->remove(c->getBucketEngineAsV0(),
                                           c->getCookie(), key, nkey, &cas,
                                           c->binary_header.request.vbucket,
                                           &mut_info);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        c->setCAS(cas);
        if (c->isSupportsMutationExtras()) {
            /* Response includes vbucket UUID and sequence number */
            mutation_descr_t* const extras = (mutation_descr_t*)
                (c->write.buf + sizeof(protocol_binary_response_delete));

            extras->vbucket_uuid = htonll(mut_info.vbucket_uuid);
            extras->seqno = htonll(mut_info.seqno);
            mcbp_write_response(c, extras, sizeof(*extras), 0, sizeof(*extras));
        } else {
            mcbp_write_response(c, NULL, 0, 0, 0);
        }
        SLAB_INCR(c, delete_hits, key, nkey);
        update_topkeys(key, nkey, c);
        break;
    case ENGINE_KEY_EEXISTS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        break;
    case ENGINE_KEY_ENOENT:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        STATS_INCR(c, delete_misses, key, nkey);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
        break;
    case ENGINE_TMPFAIL:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    default:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
    }
}

static void arithmetic_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_incr*>(binary_get_request(
        c));
    ENGINE_ERROR_CODE ret;
    uint64_t delta;
    uint64_t initial;
    rel_time_t expiration;
    char* key;
    size_t nkey;
    bool incr;
    uint64_t result;

    (void)packet;

    switch (c->getCmd()) {
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_INCREMENT:
        c->setNoReply(false);
        break;
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_DECREMENT:
        c->setNoReply(false);
        break;
    default:
        LOG_WARNING(c,
                    "%u: arithmetic_executor: cmd (which is %d) is not a valid "
                        "ARITHMETIC variant - closing connection", c->getCmd());
        c->setState(conn_closing);
        return;
    }

    if (req->message.header.request.cas != 0) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        return;
    }

    /* fix byteorder in the request */
    delta = ntohll(req->message.body.delta);
    initial = ntohll(req->message.body.initial);
    expiration = ntohl(req->message.body.expiration);
    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;
    incr = (c->getCmd() == PROTOCOL_BINARY_CMD_INCREMENT ||
            c->getCmd() == PROTOCOL_BINARY_CMD_INCREMENTQ);

    if (settings.verbose > 1) {
        char buffer[1024];
        ssize_t nw;
        nw = key_to_printable_buffer(buffer, sizeof(buffer), c->getId(), true,
                                     incr ? "INCR" : "DECR", key, nkey);
        if (nw != -1) {
            if (snprintf(buffer + nw, sizeof(buffer) - nw,
                         " %" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n",
                         delta, initial, (uint64_t)expiration) != -1) {
                LOG_DEBUG(c, "%s", buffer);
            }
        }
    }

    ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);

    item* item = NULL;
    if (ret == ENGINE_SUCCESS) {
        ret = c->getBucketEngine()->arithmetic(c->getBucketEngineAsV0(),
                                               c->getCookie(),
                                               key, (int)nkey, incr,
                                               req->message.body.expiration !=
                                               0xffffffff,
                                               delta, initial, expiration,
                                               &item,
                                               c->binary_header.request.datatype,
                                               &result,
                                               c->binary_header.request.vbucket);
    }

    switch (ret) {
    case ENGINE_SUCCESS: {
        /* Lookup the item's info for necessary metadata. */
        item_info info;
        info.nvalue = 1;
        if (!bucket_get_item_info(c, item, &info)) {
            bucket_release_item(c, item);
            LOG_WARNING(c, "%u: Failed to get item info", c->getId());
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return;
        }
        c->setCAS(info.cas);

        char* body_buf =
            (c->write.buf + sizeof(protocol_binary_response_incr));
        if (c->isSupportsMutationExtras()) {
            /* Response includes vbucket UUID and sequence number (in addition
             * to value) */
            struct mutation_extras_plus_body {
                mutation_descr_t extras;
                uint64_t value;
            };
            auto* body = reinterpret_cast<mutation_extras_plus_body*>(body_buf);
            body->extras.vbucket_uuid = htonll(info.vbucket_uuid);
            body->extras.seqno = htonll(info.seqno);
            body->value = htonll(result);
            mcbp_write_response(c, body, sizeof(body->extras), 0,
                                sizeof(*body));
        } else {
            /* Just send value back. */
            uint64_t* const value_ptr = (uint64_t*)body_buf;
            *value_ptr = htonll(result);
            mcbp_write_response(c, value_ptr, 0, 0, sizeof(*value_ptr));
        }

        /* No further need for item; release it. */
        bucket_release_item(c, item);

        if (incr) {
            STATS_INCR(c, incr_hits, key, nkey);
        } else {
            STATS_INCR(c, decr_hits, key, nkey);
        }
        update_topkeys(key, nkey, c);
        break;
    }
    case ENGINE_KEY_EEXISTS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        break;
    case ENGINE_KEY_ENOENT:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        if ((c->getCmd() == PROTOCOL_BINARY_CMD_INCREMENT) ||
            (c->getCmd() == PROTOCOL_BINARY_CMD_INCREMENTQ)) {
            STATS_INCR(c, incr_misses, key, nkey);
        } else {
            STATS_INCR(c, decr_misses, key, nkey);
        }
        break;
    case ENGINE_ENOMEM:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        break;
    case ENGINE_TMPFAIL:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ETMPFAIL);
        break;
    case ENGINE_EINVAL:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL);
        break;
    case ENGINE_NOT_STORED:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_ENOTSUP:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    default:
        LOG_WARNING(c,
                    "%u: arithmetic_executor: unexpected response (0x%x) "
                        "from engine->arithmetic()", ret);
        c->setState(conn_closing);
        break;
    }
}

static void get_cmd_timer_executor(McbpConnection* c, void* packet) {
    std::string str;
    auto* req = reinterpret_cast<protocol_binary_request_get_cmd_timer*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    int index = c->getBucketIndex();
    std::string bucket(key, keylen);

    if (bucket == "/all/") {
        index = 0;
        keylen = 0;
    }

    if (keylen == 0) {
        if (index == 0 && !cookie_is_admin(c)) {
            // We're not connected to a bucket, and we didn't
            // authenticate to a bucket.. Don't leak the
            // global stats...
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
            return;
        }
        str = all_buckets[index].timings.generate(req->message.body.opcode);
        mcbp_response_handler(NULL, 0, NULL, 0, str.data(),
                              uint32_t(str.length()),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                              0, c->getCookie());
        mcbp_write_and_free(c, &c->getDynamicBuffer());
    } else if (cookie_is_admin(c)) {
        bool found = false;
        for (int ii = 1; ii < settings.max_buckets && !found; ++ii) {
            // Need the lock to get the bucket state and name
            cb_mutex_enter(&all_buckets[ii].mutex);
            if ((all_buckets[ii].state == BucketState::Ready) &&
                (bucket == all_buckets[ii].name)) {
                str = all_buckets[ii].timings.generate(
                    req->message.body.opcode);
                found = true;
            }
            cb_mutex_exit(&all_buckets[ii].mutex);
        }
        if (found) {
            mcbp_response_handler(NULL, 0, NULL, 0, str.data(),
                                  uint32_t(str.length()),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                  0, c->getCookie());
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    } else {
        // non-privileged connections can't specify bucket
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
    }
}

static void set_ctrl_token_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_set_ctrl_token*>(packet);
    uint64_t casval = ntohll(req->message.header.request.cas);
    uint64_t newval = ntohll(req->message.body.new_cas);
    uint64_t value;

    auto ret = session_cas.cas(newval, casval, value);
    mcbp_response_handler(NULL, 0, NULL, 0, NULL, 0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          engine_error_2_mcbp_protocol_error(ret),
                          value, c->getCookie());

    mcbp_write_and_free(c, &c->getDynamicBuffer());
}

static void get_ctrl_token_executor(McbpConnection* c, void*) {
    mcbp_response_handler(NULL, 0, NULL, 0, NULL, 0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          PROTOCOL_BINARY_RESPONSE_SUCCESS,
                          session_cas.getCasValue(), c->getCookie());
    mcbp_write_and_free(c, &c->getDynamicBuffer());
}

static void init_complete_executor(McbpConnection* c, void* packet) {
    auto* init = reinterpret_cast<protocol_binary_request_init_complete*>(packet);
    uint64_t cas = ntohll(init->message.header.request.cas);;

    if (session_cas.increment_session_counter(cas)) {
        set_server_initialized(true);
        session_cas.decrement_session_counter();
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        perform_callbacks(ON_INIT_COMPLETE, nullptr, nullptr);
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    }
}

static void ioctl_get_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t value;

    ENGINE_ERROR_CODE status = ioctl_get_property(key, keylen, &value);

    if (status == ENGINE_SUCCESS) {
        char res_buffer[16];
        size_t length = snprintf(res_buffer, sizeof(res_buffer), "%ld", value);
        if ((length > sizeof(res_buffer) - 1) ||
            mcbp_response_handler(NULL, 0, NULL, 0, res_buffer,
                                  uint32_t(length),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                                  c->getCookie())) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        }
    } else {
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(status));
    }
}

static void ioctl_set_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen);
    const char* value = key + keylen;

    ENGINE_ERROR_CODE status = ioctl_set_property(c, key, keylen, value,
                                                  vallen);

    mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(status));
}

static void config_validate_executor(McbpConnection* c, void* packet) {
    const char* val_ptr = NULL;
    cJSON* errors = NULL;
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);

    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen) - keylen;

    /* Key not yet used, must be zero length. */
    if (keylen != 0) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        return;
    }

    /* must have non-zero length config */
    if (vallen == 0 || vallen > CONFIG_VALIDATE_MAX_LENGTH) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        return;
    }

    val_ptr = (const char*)(req->bytes + sizeof(req->bytes)) + keylen;

    /* null-terminate value, and convert to integer */
    try {
        std::string val_buffer(val_ptr, vallen);
        errors = cJSON_CreateArray();

        if (validate_proposed_config_changes(val_buffer.c_str(), errors)) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        } else {
            /* problem(s). Send the errors back to the client. */
            char* error_string = cJSON_PrintUnformatted(errors);
            if (mcbp_response_handler(NULL, 0, NULL, 0, error_string,
                                      (uint32_t)strlen(error_string), 0,
                                      PROTOCOL_BINARY_RESPONSE_EINVAL, 0,
                                      c->getCookie())) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            }
            cJSON_Free(error_string);
        }

        cJSON_Delete(errors);
    } catch (const std::bad_alloc&) {
        LOG_WARNING(c,
                    "%u: Failed to allocate buffer of size %"
                        PRIu64 " to validate config. Shutting down connection",
                    c->getId(), vallen + 1);
        c->setState(conn_closing);
        return;
    }

}

static void config_reload_executor(McbpConnection* c, void*) {
    reload_config_file();
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

static void audit_config_reload_executor(McbpConnection* c, void*) {
    if (settings.audit_file) {
        if (configure_auditdaemon(get_audit_handle(),
                                  settings.audit_file,
                                  c->getCookie()) == AUDIT_EWOULDBLOCK) {
            c->setEwouldblock(true);
            c->setState(conn_audit_configuring);
        } else {
            LOG_WARNING(NULL,
                        "configuration of audit daemon failed with config "
                            "file: %s",
                        settings.audit_file);
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
        }
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }
}

static void assume_role_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_assume_role*>(packet);
    size_t rlen = ntohs(req->message.header.request.keylen);
    AuthResult err;

    if (rlen > 0) {
        try {
            auto* role_ptr = reinterpret_cast<const char*>(req + 1);
            std::string role(role_ptr, rlen);
            err = c->assumeRole(role.c_str());

        } catch (const std::bad_alloc&) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            return;
        }
    } else {
        err = c->dropRole();
    }

    switch (err) {
    case AuthResult::FAIL:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        return;
    case AuthResult::OK:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        return;
    case AuthResult::STALE:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_STALE);
        return;
    }

    LOG_WARNING(c,
                "%u: assume_role_executor: err (which is %d) is not a valid "
                    "AuthResult - closing connection", err);
    c->setState(conn_closing);
}

static void audit_put_executor(McbpConnection* c, void* packet) {

    auto* req = reinterpret_cast<const protocol_binary_request_audit_put*>(packet);
    const void* payload = req->bytes + sizeof(req->message.header) +
                          req->message.header.request.extlen;

    const size_t payload_length = ntohl(req->message.header.request.bodylen) -
                                  req->message.header.request.extlen;

    if (put_audit_event(get_audit_handle(),
                        ntohl(req->message.body.id),
                        payload,
                        payload_length) == AUDIT_SUCCESS) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
    }
}

/**
 * Override the CreateBucketTask so that we can have our own notification
 * mechanism to kickstart the clients thread
 */
class McbpCreateBucketTask : public Task {
public:
    McbpCreateBucketTask(const std::string& name_,
                         const std::string& config_,
                         const BucketType& type_,
                         McbpConnection& connection_)
        : thread(name_, config_, type_, connection_, this),
          mcbpconnection(connection_) { }

    // start the bucket deletion
    // May throw std::bad_alloc if we're failing to start the thread
    void start() {
        thread.start();
    }

    virtual bool execute() override {
        return true;
    }

    virtual void notifyExecutionComplete() override {
        notify_io_complete(mcbpconnection.getCookie(), thread.getResult());
    }

    CreateBucketThread thread;
    McbpConnection& mcbpconnection;
};

/**
 * The create bucket contains message have the following format:
 *    key: bucket name
 *    body: module\nconfig
 */
static void create_bucket_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        auto* req = reinterpret_cast<protocol_binary_request_create_bucket*>(packet);
        /* decode packet */
        uint16_t klen = ntohs(req->message.header.request.keylen);
        uint32_t blen = ntohl(req->message.header.request.bodylen);
        blen -= klen;

        try {
            std::string name((char*)(req + 1), klen);
            std::string value((char*)(req + 1) + klen, blen);
            std::string config;

            // Check if (optional) config was included after the value.
            auto marker = value.find('\0');
            if (marker != std::string::npos) {
                config.assign(&value[marker + 1]);
            }

            std::string errors;
            BucketType type = module_to_bucket_type(value.c_str());
            std::shared_ptr<Task> task = std::make_shared<McbpCreateBucketTask>(
                name, config, type, *c);
            std::lock_guard<std::mutex> guard(task->getMutex());
            reinterpret_cast<McbpCreateBucketTask*>(task.get())->start();
            ret = ENGINE_EWOULDBLOCK;
            executorPool->schedule(task, false);
        } catch (const std::bad_alloc&) {
            ret = ENGINE_ENOMEM;
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_create_bucket);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void list_bucket_executor(McbpConnection* c, void*) {
    try {
        std::string blob;
        for (auto& bucket : all_buckets) {
            cb_mutex_enter(&bucket.mutex);
            if (bucket.state == BucketState::Ready) {
                blob += bucket.name + std::string(" ");
            }
            cb_mutex_exit(&bucket.mutex);
        }

        if (blob.size() > 0) {
            /* remove trailing " " */
            blob.pop_back();
        }

        if (mcbp_response_handler(NULL, 0, NULL, 0, blob.data(),
                                  uint32_t(blob.size()), 0,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                                  c->getCookie())) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        }
    } catch (const std::bad_alloc&) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
    }
}


static void delete_bucket_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    (void)packet;

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        McbpDestroyBucketTask* task = nullptr;
        try {
            auto* req = reinterpret_cast<protocol_binary_request_delete_bucket*>(packet);
            /* decode packet */
            uint16_t klen = ntohs(req->message.header.request.keylen);
            uint32_t blen = ntohl(req->message.header.request.bodylen);
            blen -= klen;

            std::string name((char*)(req + 1), klen);
            std::string config((char*)(req + 1) + klen, blen);
            bool force = false;

            struct config_item items[2];
            memset(&items, 0, sizeof(items));
            items[0].key = "force";
            items[0].datatype = DT_BOOL;
            items[0].value.dt_bool = &force;
            items[1].key = NULL;

            if (parse_config(config.c_str(), items, stderr) == 0) {
                std::shared_ptr<Task> task = std::make_shared<McbpDestroyBucketTask>(
                    name, force, c);
                std::lock_guard<std::mutex> guard(task->getMutex());
                reinterpret_cast<McbpDestroyBucketTask*>(task.get())->start();
                ret = ENGINE_EWOULDBLOCK;
                executorPool->schedule(task, false);
            } else {
                ret = ENGINE_EINVAL;
            }
        } catch (std::bad_alloc&) {
            ret = ENGINE_ENOMEM;
            delete task;
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_delete_bucket);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void select_bucket_executor(McbpConnection* c, void* packet) {
    /* The validator ensured that we're not doing a buffer overflow */
    char bucketname[1024];
    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
    uint16_t klen = ntohs(req->message.header.request.keylen);
    memcpy(bucketname, req->bytes + (sizeof(*req)), klen);
    bucketname[klen] = '\0';

    if (associate_bucket(c, bucketname)) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    }
}

static void shutdown_executor(McbpConnection* c, void* packet) {
    auto req = reinterpret_cast<protocol_binary_request_shutdown*>(packet);
    uint64_t cas = ntohll(req->message.header.request.cas);

    if (session_cas.increment_session_counter(cas)) {
        shutdown_server();
        session_cas.decrement_session_counter();
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    }
}

std::array<mcbp_package_execute, 0x100>& get_mcbp_executors(void) {
    static std::array<mcbp_package_execute, 0x100> executors;
    std::fill(executors.begin(), executors.end(), nullptr);

    executors[PROTOCOL_BINARY_CMD_DCP_OPEN] = dcp_open_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = dcp_add_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = dcp_close_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = dcp_snapshot_marker_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = tap_checkpoint_end_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = tap_checkpoint_start_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CONNECT] = tap_connect_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_DELETE] = tap_delete_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_FLUSH] = tap_flush_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_MUTATION] = tap_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = tap_opaque_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = tap_vbucket_set_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_DELETION] = dcp_deletion_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = dcp_expiration_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_FLUSH] = dcp_flush_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = dcp_get_failover_log_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_MUTATION] = dcp_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = dcp_set_vbucket_state_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_NOOP] = dcp_noop_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = dcp_buffer_acknowledgement_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CONTROL] = dcp_control_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = dcp_stream_end_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = dcp_stream_req_executor;
    executors[PROTOCOL_BINARY_CMD_ISASL_REFRESH] = isasl_refresh_executor;
    executors[PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH] = ssl_certs_refresh_executor;
    executors[PROTOCOL_BINARY_CMD_VERBOSITY] = verbosity_executor;
    executors[PROTOCOL_BINARY_CMD_HELLO] = process_hello_packet_executor;
    executors[PROTOCOL_BINARY_CMD_VERSION] = version_executor;
    executors[PROTOCOL_BINARY_CMD_QUIT] = quit_executor;
    executors[PROTOCOL_BINARY_CMD_QUITQ] = quitq_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_LIST_MECHS] = sasl_list_mech_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_AUTH] = sasl_auth_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_STEP] = sasl_auth_executor;
    executors[PROTOCOL_BINARY_CMD_NOOP] = noop_executor;
    executors[PROTOCOL_BINARY_CMD_FLUSH] = flush_executor;
    executors[PROTOCOL_BINARY_CMD_FLUSHQ] = flush_executor;
    executors[PROTOCOL_BINARY_CMD_SETQ] = setq_executor;
    executors[PROTOCOL_BINARY_CMD_SET] = set_executor;
    executors[PROTOCOL_BINARY_CMD_ADDQ] = addq_executor;
    executors[PROTOCOL_BINARY_CMD_ADD] = add_executor;
    executors[PROTOCOL_BINARY_CMD_REPLACEQ] = replaceq_executor;
    executors[PROTOCOL_BINARY_CMD_REPLACE] = replace_executor;
    executors[PROTOCOL_BINARY_CMD_APPENDQ] = appendq_executor;
    executors[PROTOCOL_BINARY_CMD_APPEND] = append_executor;
    executors[PROTOCOL_BINARY_CMD_PREPENDQ] = prependq_executor;
    executors[PROTOCOL_BINARY_CMD_PREPEND] = prepend_executor;
    executors[PROTOCOL_BINARY_CMD_GET] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETQ] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETK] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETKQ] = get_executor;
    executors[PROTOCOL_BINARY_CMD_DELETE] = delete_executor;
    executors[PROTOCOL_BINARY_CMD_DELETEQ] = delete_executor;
    executors[PROTOCOL_BINARY_CMD_STAT] = stat_executor;
    executors[PROTOCOL_BINARY_CMD_INCREMENT] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_INCREMENTQ] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_DECREMENT] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_DECREMENTQ] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_GET_CMD_TIMER] = get_cmd_timer_executor;
    executors[PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN] = set_ctrl_token_executor;
    executors[PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN] = get_ctrl_token_executor;
    executors[PROTOCOL_BINARY_CMD_INIT_COMPLETE] = init_complete_executor;
    executors[PROTOCOL_BINARY_CMD_IOCTL_GET] = ioctl_get_executor;
    executors[PROTOCOL_BINARY_CMD_IOCTL_SET] = ioctl_set_executor;
    executors[PROTOCOL_BINARY_CMD_CONFIG_VALIDATE] = config_validate_executor;
    executors[PROTOCOL_BINARY_CMD_CONFIG_RELOAD] = config_reload_executor;
    executors[PROTOCOL_BINARY_CMD_ASSUME_ROLE] = assume_role_executor;
    executors[PROTOCOL_BINARY_CMD_AUDIT_PUT] = audit_put_executor;
    executors[PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD] = audit_config_reload_executor;
    executors[PROTOCOL_BINARY_CMD_SHUTDOWN] = shutdown_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_GET] = subdoc_get_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_EXISTS] = subdoc_exists_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD] = subdoc_dict_add_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT] = subdoc_dict_upsert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DELETE] = subdoc_delete_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_REPLACE] = subdoc_replace_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST] = subdoc_array_push_last_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST] = subdoc_array_push_first_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT] = subdoc_array_insert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE] = subdoc_array_add_unique_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_COUNTER] = subdoc_counter_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP] = subdoc_multi_lookup_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION] = subdoc_multi_mutation_executor;

    executors[PROTOCOL_BINARY_CMD_CREATE_BUCKET] = create_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_LIST_BUCKETS] = list_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_DELETE_BUCKET] = delete_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_SELECT_BUCKET] = select_bucket_executor;

    return executors;
}

static void process_bin_dcp_response(McbpConnection* c) {
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    c->setSupportsDatatype(true);

    if (c->getBucketEngine()->dcp.response_handler != NULL) {
        auto* header = reinterpret_cast<protocol_binary_response_header*>
        (c->read.curr - (c->binary_header.request.bodylen +
                         sizeof(c->binary_header)));
        ret = c->getBucketEngine()->dcp.response_handler
            (c->getBucketEngineAsV0(), c->getCookie(), header);
    }

    if (ret == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        c->setState(conn_ship_log);
    }
}


void initialize_mbcp_lookup_map(void) {
    int ii;
    for (ii = 0; ii < 0x100; ++ii) {
        request_handlers[ii].descriptor = NULL;
        request_handlers[ii].callback = default_unknown_command;
    }

    response_handlers[PROTOCOL_BINARY_CMD_NOOP] = process_bin_noop_response;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_MUTATION] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_DELETE] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_FLUSH] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = process_bin_tap_ack;

    response_handlers[PROTOCOL_BINARY_CMD_DCP_OPEN] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_MUTATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_DELETION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_FLUSH] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_NOOP] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CONTROL] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_RESERVED4] = process_bin_dcp_response;
}

bool conn_setup_tap_stream(McbpConnection* c) {
    process_bin_tap_connect(c);
    return true;
}

static int invalid_datatype(McbpConnection* c) {
    switch (c->binary_header.request.datatype) {
    case PROTOCOL_BINARY_RAW_BYTES:
        return 0;

    case PROTOCOL_BINARY_DATATYPE_JSON:
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
        if (c->isSupportsDatatype()) {
            return 0;
        }
        /* FALLTHROUGH */
    default:
        return 1;
    }
}

static protocol_binary_response_status validate_bin_header(McbpConnection* c) {
    if (c->binary_header.request.bodylen >=
        (c->binary_header.request.keylen + c->binary_header.request.extlen)) {
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    } else {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
}

static void process_bin_packet(McbpConnection* c) {

    char* packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));

    auto opcode = static_cast<protocol_binary_command>(c->binary_header.request.opcode);
    auto executor = executors[opcode];

    AuthResult res = c->checkAccess(opcode);
    switch (res) {
    case AuthResult::FAIL:
        LOG_WARNING(c,
                    "%u %s: no access to command %s",
                    c->getId(), c->getDescription().c_str(),
                    memcached_opcode_2_text(opcode));
        audit_command_access_failed(c);
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
        return;
    case AuthResult::OK: {
        protocol_binary_response_status result = validate_bin_header(c);
        if (result == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            result = c->validateCommand(opcode);
        }

        if (result != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            LOG_WARNING(c,
                        "%u: Invalid format for specified for %s - %d - "
                        "closing connection",
                        c->getId(), memcached_opcode_2_text(opcode), result);
            audit_invalid_packet(c);
            mcbp_write_packet(c, result);
            c->setWriteAndGo(conn_closing);
            return;
        }

        if (executor != NULL) {
            executor(c, packet);
        } else {
            process_bin_unknown_packet(c);
        }
        return;
    }
    case AuthResult::STALE:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_STALE);
        return;
    }

    LOG_WARNING(c,
                "%u: process_bin_packet: res (which is %d) is not a valid "
                    "AuthResult - closing connection", res);
    c->setState(conn_closing);
}


static CB_INLINE bool is_initialized(McbpConnection* c, uint8_t opcode) {
    if (c->isAdmin() || is_server_initialized()) {
        return true;
    }

    switch (opcode) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        return true;
    default:
        return false;
    }
}

static void dispatch_bin_command(McbpConnection* c) {
    uint16_t keylen = c->binary_header.request.keylen;

    /* @trond this should be in the Connection-connect part.. */
    /*        and in the select bucket */
    if (c->getBucketEngine() == NULL) {
        c->setBucketEngine(all_buckets[c->getBucketIndex()].engine);
    }

    if (!is_initialized(c, c->binary_header.request.opcode)) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED);
        c->setWriteAndGo(conn_closing);
        return;
    }

    if (settings.require_sasl && !authenticated(c)) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);
        c->setWriteAndGo(conn_closing);
        return;
    }

    if (invalid_datatype(c)) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        c->setWriteAndGo(conn_closing);
        return;
    }

    if (c->getStart() == 0) {
        c->setStart(gethrtime());
    }

    MEMCACHED_PROCESS_COMMAND_START(c->getId(), c->read.curr, c->read.bytes);

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (keylen > KEY_MAX_LENGTH) {
        handle_binary_protocol_error(c);
        return;
    }

    c->setNoReply(false);

    /*
     * Protect ourself from someone trying to kill us by sending insanely
     * large packets.
     */
    if (c->binary_header.request.bodylen > settings.max_packet_size) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        c->setWriteAndGo(conn_closing);
    } else {
        bin_read_chunk(c, c->binary_header.request.bodylen);
    }
}

void mcbp_complete_nread(McbpConnection* c) {
    if (c->binary_header.request.magic == PROTOCOL_BINARY_RES) {
        RESPONSE_HANDLER handler;
        handler = response_handlers[c->binary_header.request.opcode];
        if (handler) {
            handler(c);
        } else {
            LOG_WARNING(c, "%u: Unsupported response packet received: %u",
                        c->getId(),
                        (unsigned int)c->binary_header.request.opcode);
            c->setState(conn_closing);
        }
    } else {
        process_bin_packet(c);
    }
}

int try_read_mcbp_command(McbpConnection* c) {
    if (c == nullptr) {
        throw std::runtime_error("Internal eror, connection is not mcbp");
    }
    cb_assert(c->read.curr <= (c->read.buf + c->read.size));
    cb_assert(c->read.bytes > 0);

    /* Do we have the complete packet header? */
    if (c->read.bytes < sizeof(c->binary_header)) {
        /* need more data! */
        return 0;
    } else {
#ifdef NEED_ALIGN
        if (((long)(c->read.curr)) % 8 != 0) {
            /* must realign input buffer */
            memmove(c->read.buf, c->read.curr, c->read.bytes);
            c->read.curr = c->read.buf;
            if (settings.verbose > 1) {
                settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                                "%d: Realign input buffer\n", c->sfd);
            }
        }
#endif
        protocol_binary_request_header* req;
        req = (protocol_binary_request_header*)c->read.curr;

        if (settings.verbose > 1) {
            /* Dump the packet before we convert it to host order */
            char buffer[1024];
            ssize_t nw;
            nw = bytes_to_output_string(buffer, sizeof(buffer), c->getId(),
                                        true, "Read binary protocol data:",
                                        (const char*)req->bytes,
                                        sizeof(req->bytes));
            if (nw != -1) {
                LOG_DEBUG(c, "%s", buffer);
            }
        }

        c->binary_header = *req;
        c->binary_header.request.keylen = ntohs(req->request.keylen);
        c->binary_header.request.bodylen = ntohl(req->request.bodylen);
        c->binary_header.request.vbucket = ntohs(req->request.vbucket);
        c->binary_header.request.cas = ntohll(req->request.cas);

        if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ &&
            !(c->binary_header.request.magic == PROTOCOL_BINARY_RES &&
              response_handlers[c->binary_header.request.opcode])) {
            if (c->binary_header.request.magic != PROTOCOL_BINARY_RES) {
                LOG_WARNING(c, "%u: Invalid magic: %x, closing connection",
                            c->getId(), c->binary_header.request.magic);
            } else {
                LOG_WARNING(c,
                            "%u: Unsupported response packet received: %u, "
                                "closing connection",
                            c->getId(),
                            (unsigned int)c->binary_header.request.opcode);

            }
            c->setState(conn_closing);
            return -1;
        }

        if (!c->addMsgHdr(true)) {
            c->setState(conn_closing);
            return -1;
        }

        c->setCmd(c->binary_header.request.opcode);
        /* clear the returned cas value */
        c->setCAS(0);

        dispatch_bin_command(c);

        c->read.bytes -= sizeof(c->binary_header);
        c->read.curr += sizeof(c->binary_header);
    }

    return 1;
}
