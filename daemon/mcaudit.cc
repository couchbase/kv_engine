/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcaudit.h"

#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "debug_helpers.h"
#include "log_macros.h"
#include "logger/logger.h"
#include "memcached.h"
#include "memcached_audit_events.h"
#include "settings.h"

#include <folly/Synchronized.h>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <platform/scope_timer.h>
#include <platform/string_hex.h>
#include <sstream>

/// @returns the singleton audit handle.
folly::Synchronized<cb::audit::UniqueAuditPtr>& getAuditHandle() {
    static folly::Synchronized<cb::audit::UniqueAuditPtr> handle;
    return handle;
}

static std::atomic_bool audit_enabled{false};

static const int First = MEMCACHED_AUDIT_OPENED_DCP_CONNECTION;
static const int Last = MEMCACHED_AUDIT_TENANT_RATE_LIMITED;

static std::array<std::atomic_bool, Last - First + 1> events;

static bool isEnabled(uint32_t id) {
    if (!audit_enabled.load(std::memory_order_consume)) {
        // Global switch is off.. All events are disabled
        return false;
    }

    if (id >= First && id <= Last) {
        // This is one of ours
        return events[id - First].load(std::memory_order_consume);
    }

    // we don't have information about this id... let the underlying event
    // framework deal with it
    return true;
}

void cb::audit::setEnabled(uint32_t id, bool enable) {
    bool expected = !enable;

    if (id == 0) {
        if (audit_enabled.compare_exchange_strong(expected, enable)) {
            LOG_INFO("Audit changed from: {} to: {}",
                     !enable ? "enabled" : "disabled",
                     enable ? "enabled" : "disabled");
        }
    }

    if (id >= First && id <= Last) {
        if (events[id - First].compare_exchange_strong(expected, enable)) {
            LOG_INFO("Audit descriptor {} changed from: {} to: {}",
                     id,
                     expected ? "enabled" : "disabled",
                     enable ? "enabled" : "disabled");
        }
    }
}

/**
 * Create the typical memcached audit object. It consists of a
 * timestamp, the socket endpoints and the creds. Then each audit event
 * may add event-specific content.
 *
 * @param c the connection object
 * @return the json object containing the basic information
 */
static nlohmann::json create_memcached_audit_object(
        const Connection& c,
        const cb::rbac::UserIdent& ui,
        const std::optional<cb::rbac::UserIdent>& euid) {
    nlohmann::json root;

    root["timestamp"] = ISOTime::generatetimestamp();
    root["remote"] = nlohmann::json::parse(c.getPeername());
    root["local"] = nlohmann::json::parse(c.getSockname());
    root["real_userid"] = ui.to_json();

    if (euid) {
        root["effective_userid"] = euid->to_json();
    }

    return root;
}

/**
 * Convert the JSON object to text and send it to the audit framework
 *
 * @param id the audit identifier
 * @param event the payload of the audit description
 * @param warn what to log if we're failing to put the audit event
 */
static void do_audit(Cookie* cookie,
                     uint32_t id,
                     const nlohmann::json& event,
                     const char* warn) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer({cookie, Code::Audit});

    auto text = event.dump();
    getAuditHandle().withRLock([id, warn, &text](auto& handle) {
        if (handle) {
            if (!handle->put_event(id, text)) {
                LOG_WARNING("{}: {}", warn, text);
            }
        }
    });
}

void audit_auth_failure(const Connection& c,
                        const cb::rbac::UserIdent& ui,
                        const char* reason,
                        Cookie* cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_FAILED)) {
        return;
    }
    auto root = create_memcached_audit_object(c, ui, {});
    root["reason"] = reason;

    do_audit(cookie,
             MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
             root,
             "Failed to send AUTH FAILED audit event");
}

void audit_auth_success(const Connection& c, Cookie* cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED)) {
        return;
    }
    auto root = create_memcached_audit_object(c, c.getUser(), {});
    do_audit(cookie,
             MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED,
             root,
             "Failed to send AUTH SUCCESS audit event");
}

void audit_bucket_selection(const Connection& c, Cookie* cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_SELECT_BUCKET)) {
        return;
    }
    const auto& bucket = c.getBucket();
    // Don't audit that we're jumping into the "no bucket"
    if (bucket.type != BucketType::NoBucket) {
        auto root = create_memcached_audit_object(c, c.getUser(), {});
        root["bucket"] = c.getBucket().name;
        do_audit(cookie,
                 MEMCACHED_AUDIT_SELECT_BUCKET,
                 root,
                 "Failed to send SELECT BUCKET audit event");
    }
}

void audit_bucket_flush(Cookie& cookie, const char* bucket) {
    if (!isEnabled(MEMCACHED_AUDIT_EXTERNAL_MEMCACHED_BUCKET_FLUSH)) {
        return;
    }
    auto& c = cookie.getConnection();
    auto root = create_memcached_audit_object(c, c.getUser(), {});
    root["bucket"] = bucket;

    do_audit(&cookie,
             MEMCACHED_AUDIT_EXTERNAL_MEMCACHED_BUCKET_FLUSH,
             root,
             "Failed to send EXTERNAL_MEMCACHED_BUCKET_FLUSH audit event");
}

void audit_dcp_open(Cookie& cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_OPENED_DCP_CONNECTION)) {
        return;
    }
    auto& c = cookie.getConnection();
    if (c.isInternal()) {
        LOG_INFO_RAW("Open DCP stream with admin credentials");
    } else {
        auto root = create_memcached_audit_object(c, c.getUser(), {});
        root["bucket"] = c.getBucket().name;

        do_audit(&cookie,
                 MEMCACHED_AUDIT_OPENED_DCP_CONNECTION,
                 root,
                 "Failed to send DCP open connection "
                 "audit event to audit daemon");
    }
}

void audit_set_privilege_debug_mode(Cookie& cookie, bool enable) {
    if (!isEnabled(MEMCACHED_AUDIT_PRIVILEGE_DEBUG_CONFIGURED)) {
        return;
    }
    auto& c = cookie.getConnection();
    auto root = create_memcached_audit_object(c, c.getUser(), {});
    root["enable"] = enable;
    do_audit(&cookie,
             MEMCACHED_AUDIT_PRIVILEGE_DEBUG_CONFIGURED,
             root,
             "Failed to send modifications in privilege debug state "
             "audit event to audit daemon");
}

void audit_privilege_debug(Cookie& cookie,
                           const std::string& command,
                           const std::string& bucket,
                           const std::string& privilege,
                           const std::string& context) {
    if (!isEnabled(MEMCACHED_AUDIT_PRIVILEGE_DEBUG)) {
        return;
    }
    auto& c = cookie.getConnection();
    auto root = create_memcached_audit_object(c, c.getUser(), {});
    root["command"] = command;
    root["bucket"] = bucket;
    root["privilege"] = privilege;
    root["context"] = context;

    do_audit(&cookie,
             MEMCACHED_AUDIT_PRIVILEGE_DEBUG,
             root,
             "Failed to send privilege debug audit event to audit daemon");
}

void audit_command_access_failed(Cookie& cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE)) {
        return;
    }
    const auto& connection = cookie.getConnection();
    auto root = create_memcached_audit_object(
            connection, connection.getUser(), cookie.getEffectiveUser());
    char buffer[256];
    memset(buffer, 0, sizeof(buffer));
    const auto packet = cookie.getPacket();
    // Deliberately ignore failure of bytes_to_output_string
    // We'll either have a partial string or no string.
    bytes_to_output_string(buffer,
                           sizeof(buffer),
                           connection.getId(),
                           true,
                           "Access to command is not allowed:",
                           reinterpret_cast<const char*>(packet.data()),
                           packet.size());
    root["packet"] = buffer;
    do_audit(&cookie, MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE, root, buffer);
}

void audit_invalid_packet(const Connection& c, cb::const_byte_buffer packet) {
    if (!isEnabled(MEMCACHED_AUDIT_INVALID_PACKET)) {
        return;
    }
    auto root = create_memcached_audit_object(c, c.getUser(), {});
    std::stringstream ss;
    std::string trunc;
    const cb::const_byte_buffer::size_type max_dump_size = 256;
    if (packet.size() > max_dump_size) {
        trunc = " [truncated " + std::to_string(packet.size() - max_dump_size) +
                " bytes]";
        packet = {packet.data(), max_dump_size};
    }
    ss << "Invalid packet: " << cb::to_hex(packet) << trunc;
    const auto message = ss.str();
    root["packet"] = message.c_str() + strlen("Invalid packet: ");
    do_audit(nullptr, MEMCACHED_AUDIT_INVALID_PACKET, root, message.c_str());
}

bool mc_audit_event(Cookie& cookie,
                    uint32_t audit_eventid,
                    cb::const_byte_buffer payload) {
    if (!audit_enabled) {
        return true;
    }

    std::string_view buffer{reinterpret_cast<const char*>(payload.data()),
                            payload.size()};

    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer({cookie, Code::Audit});
    return getAuditHandle().withRLock([audit_eventid, buffer](auto& handle) {
        if (!handle) {
            return false;
        }
        return handle->put_event(audit_eventid, buffer);
    });
}

namespace cb::audit {

void addTenantRateLimited(const Connection& c, Tenant::RateLimit limit) {
    if (!isEnabled(MEMCACHED_AUDIT_TENANT_RATE_LIMITED) ||
        !c.isAuthenticated()) {
        return;
    }
    auto root = create_memcached_audit_object(c, c.getUser(), {});
    root["reason"] = to_string(limit);
    do_audit(nullptr,
             MEMCACHED_AUDIT_TENANT_RATE_LIMITED,
             root,
             "Failed to audit tenant rate limited");
}

void addSessionTerminated(const Connection& c) {
    if (!isEnabled(MEMCACHED_AUDIT_SESSION_TERMINATED) ||
        !c.isAuthenticated()) {
        return;
    }
    auto root = create_memcached_audit_object(c, c.getUser(), {});
    const auto& reason = c.getTerminationReason();
    if (!reason.empty()) {
        root["reason_for_termination"] = reason;
    }

    do_audit(nullptr,
             MEMCACHED_AUDIT_SESSION_TERMINATED,
             root,
             "Failed to audit session terminated");
}

namespace document {

void add(Cookie& cookie, Operation operation) {
    uint32_t id = 0;
    switch (operation) {
    case Operation::Read:
        id = MEMCACHED_AUDIT_DOCUMENT_READ;
        break;
    case Operation::Lock:
        id = MEMCACHED_AUDIT_DOCUMENT_LOCKED;
        break;
    case Operation::Modify:
        id = MEMCACHED_AUDIT_DOCUMENT_MODIFY;
        break;
    case Operation::Delete:
        id = MEMCACHED_AUDIT_DOCUMENT_DELETE;
        break;
    }

    if (id == 0) {
        throw std::invalid_argument(
                "cb::audit::document::add: Invalid operation");
    }

    if (!isEnabled(id)) {
        return;
    }

    const auto& connection = cookie.getConnection();
    auto root = create_memcached_audit_object(
            connection, connection.getUser(), cookie.getEffectiveUser());
    root["bucket"] = connection.getBucket().name;
    root["collection_id"] = cookie.getPrintableRequestCollectionID();
    root["key"] = cookie.getPrintableRequestKey();

    switch (operation) {
    case Operation::Read:
        do_audit(&cookie,
                 MEMCACHED_AUDIT_DOCUMENT_READ,
                 root,
                 "Failed to send document read audit event to audit daemon");
        break;
    case Operation::Lock:
        do_audit(&cookie,
                 MEMCACHED_AUDIT_DOCUMENT_LOCKED,
                 root,
                 "Failed to send document locked audit event to audit daemon");
        break;
    case Operation::Modify:
        do_audit(&cookie,
                 MEMCACHED_AUDIT_DOCUMENT_MODIFY,
                 root,
                 "Failed to send document modify audit event to audit daemon");
        break;
    case Operation::Delete:
        do_audit(&cookie,
                 MEMCACHED_AUDIT_DOCUMENT_DELETE,
                 root,
                 "Failed to send document delete audit event to audit daemon");
        break;
    }
}

} // namespace cb::audit::document
} // namespace cb::audit

static void event_state_listener(uint32_t id, bool enabled) {
    cb::audit::setEnabled(id, enabled);
}

void initialize_audit() {
    /* Start the audit daemon */
    auto audit = cb::audit::create_audit_daemon(
            Settings::instance().getAuditFile(), get_server_api()->cookie);
    if (!audit) {
        FATAL_ERROR(EXIT_FAILURE, "FATAL: Failed to start audit daemon");
    }
    audit->add_event_state_listener(event_state_listener);
    audit->notify_all_event_states();
    *getAuditHandle().wlock() = std::move(audit);
}

void shutdown_audit() {
    getAuditHandle().wlock()->reset();
}

cb::engine_errc reconfigure_audit(Cookie& cookie) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer({cookie, Code::AuditReconfigure});

    return getAuditHandle().withRLock([&cookie](auto& handle) {
        if (!handle) {
            return cb::engine_errc::failed;
        }
        if (handle->configure_auditdaemon(Settings::instance().getAuditFile(),
                                          cookie)) {
            return cb::engine_errc::would_block;
        }
        return cb::engine_errc::failed;
    });
}

void stats_audit(const StatCollector& collector, Cookie* cookie) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch> timer({cookie, Code::AuditStats});
    getAuditHandle().withRLock([&collector](auto& handle) {
        if (handle) {
            handle->stats(collector);
        }
    });
}
