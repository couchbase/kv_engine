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
#include "front_end_thread.h"
#include "log_macros.h"
#include "logger/logger.h"
#include "memcached.h"
#include "settings.h"

#include <auditd/couchbase_audit_events.h>
#include <auditd/src/audit_descriptor_manager.h>
#include <folly/Synchronized.h>
#include <mcbp/protocol/header.h>
#include <memcached/audit_interface.h>
#include <nlohmann/json.hpp>
#include <platform/scope_timer.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <optional>
#include <sstream>

/// @returns the singleton audit handle.
folly::Synchronized<cb::audit::UniqueAuditPtr>& getAuditHandle() {
    static folly::Synchronized<cb::audit::UniqueAuditPtr> handle;
    return handle;
}

static bool isEnabled(uint32_t id,
                      const Connection& connection,
                      const cb::rbac::UserIdent* euid,
                      std::optional<std::string_view> bucket,
                      std::optional<ScopeID> scope,
                      std::optional<CollectionID> collection) {
    auto* filter = connection.getThread().getAuditEventFilter();
    if (!filter || // no filter -> drop all events
        filter->isFilteredOut(
                id, connection.getUser(), euid, bucket, scope, collection)) {
        return false;
    }

    return true;
}

static bool isEnabled(uint32_t id,
                      const Connection& connection,
                      const cb::rbac::UserIdent* euid) {
    auto* filter = connection.getThread().getAuditEventFilter();
    if (!filter || // no filter -> drop all events
        !filter->isEnabled()) {
        return false;
    }

    return isEnabled(id, connection, euid, connection.getBucket().name, {}, {});
}

static bool isEnabled(uint32_t id, const Cookie& cookie) {
    auto* filter = cookie.getConnection().getThread().getAuditEventFilter();
    if (!filter || // no filter -> drop all events
        !filter->isEnabled()) {
        return false;
    }

    const auto& connection = cookie.getConnection();
    auto [sid, cid] = cookie.getScopeAndCollection();
    return isEnabled(id,
                     connection,
                     cookie.getEffectiveUser(),
                     connection.getBucket().name,
                     sid,
                     cid);
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
        uint32_t id,
        const Connection& c,
        const cb::rbac::UserIdent& ui,
        const cb::rbac::UserIdent* euid) {
    nlohmann::json root;

    const auto& descr = AuditDescriptorManager::lookup(id);
    root["id"] = id;
    root["name"] = descr.getName();
    root["description"] = descr.getDescription();
    root["timestamp"] = cb::time::timestamp();
    root["remote"] = c.getPeername();
    root["local"] = c.getSockname();
    root["real_userid"] = ui;

    if (euid) {
        root["effective_userid"] = *euid;
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
    ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(cookie, Code::Audit);

    getAuditHandle().withRLock([id, warn, &event](auto& handle) {
        if (handle) {
            if (!handle->put_event(id, event)) {
                LOG_WARNING_CTX(
                        "put_event failed", {"error", warn}, {"event", event});
            }
        }
    });
}

std::unique_ptr<AuditEventFilter> create_audit_event_filter() {
    try {
        return getAuditHandle().withRLock([](auto& handle) {
            if (handle) {
                return handle->createAuditEventFilter();
            }
            return std::unique_ptr<AuditEventFilter>{};
        });
    } catch (const std::bad_alloc&) {
        return {};
    }
}

void audit_auth_failure(const Connection& c,
                        const cb::rbac::UserIdent& ui,
                        const char* reason,
                        const std::optional<nlohmann::json>& additional,
                        Cookie* cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
                   c,
                   cookie ? cookie->getEffectiveUser() : nullptr)) {
        return;
    }
    auto root = create_memcached_audit_object(
            MEMCACHED_AUDIT_AUTHENTICATION_FAILED, c, ui, {});
    root["reason"] = reason;
    if (additional) {
        root["auth_provider"] = *additional;
    }

    do_audit(cookie,
             MEMCACHED_AUDIT_AUTHENTICATION_FAILED,
             root,
             "Failed to send AUTH FAILED audit event");
}

void audit_auth_success(const Connection& c,
                        const std::optional<nlohmann::json>& additional,
                        Cookie* cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED,
                   c,
                   cookie ? cookie->getEffectiveUser() : nullptr)) {
        return;
    }
    auto root = create_memcached_audit_object(
            MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED, c, c.getUser(), {});
    if (additional) {
        root["auth_provider"] = *additional;
    }
    do_audit(cookie,
             MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED,
             root,
             "Failed to send AUTH SUCCESS audit event");
}

void audit_bucket_selection(const Connection& c, Cookie* cookie) {
    const auto& bucket = c.getBucket();
    // Don't audit that we're jumping into the "no bucket"
    if (bucket.type == BucketType::NoBucket ||
        !isEnabled(MEMCACHED_AUDIT_SELECT_BUCKET,
                   c,
                   cookie ? cookie->getEffectiveUser() : nullptr)) {
        return;
    }
    auto root = create_memcached_audit_object(
            MEMCACHED_AUDIT_SELECT_BUCKET, c, c.getUser(), {});
    root["bucket"] = c.getBucket().name;
    do_audit(cookie,
             MEMCACHED_AUDIT_SELECT_BUCKET,
             root,
             "Failed to send SELECT BUCKET audit event");
}

void audit_bucket_flush(Cookie& cookie, const std::string_view bucket) {
    if (!isEnabled(MEMCACHED_AUDIT_EXTERNAL_MEMCACHED_BUCKET_FLUSH, cookie)) {
        return;
    }
    auto& c = cookie.getConnection();
    auto root = create_memcached_audit_object(
            MEMCACHED_AUDIT_EXTERNAL_MEMCACHED_BUCKET_FLUSH,
            c,
            c.getUser(),
            {});
    root["bucket"] = bucket;

    do_audit(&cookie,
             MEMCACHED_AUDIT_EXTERNAL_MEMCACHED_BUCKET_FLUSH,
             root,
             "Failed to send EXTERNAL_MEMCACHED_BUCKET_FLUSH audit event");
}

void audit_dcp_open(Cookie& cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_OPENED_DCP_CONNECTION, cookie)) {
        return;
    }
    auto& c = cookie.getConnection();
    if (c.isInternal()) {
        LOG_INFO_RAW("Open DCP stream with admin credentials");
    } else {
        auto root = create_memcached_audit_object(
                MEMCACHED_AUDIT_OPENED_DCP_CONNECTION, c, c.getUser(), {});
        root["bucket"] = c.getBucket().name;

        do_audit(&cookie,
                 MEMCACHED_AUDIT_OPENED_DCP_CONNECTION,
                 root,
                 "Failed to send DCP open connection "
                 "audit event to audit daemon");
    }
}

void audit_command_access_failed(Cookie& cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE, cookie)) {
        return;
    }
    const auto& connection = cookie.getConnection();
    auto root = create_memcached_audit_object(
            MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE,
            connection,
            connection.getUser(),
            cookie.getEffectiveUser());
    root["packet"] = cookie.getHeader().to_json(false);
    do_audit(&cookie,
             MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE,
             root,
             "Failed to send command access failed audit event to the audit "
             "daemon");
}

void audit_invalid_packet(const Connection& c, cb::const_byte_buffer packet) {
    if (!isEnabled(MEMCACHED_AUDIT_INVALID_PACKET, c, nullptr)) {
        return;
    }
    auto root = create_memcached_audit_object(
            MEMCACHED_AUDIT_INVALID_PACKET, c, c.getUser(), {});
    std::stringstream ss;
    std::string trunc;
    constexpr cb::const_byte_buffer::size_type max_dump_size = 256;
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

cb::engine_errc mc_audit_event(Cookie& cookie,
                               uint32_t audit_eventid,
                               cb::const_byte_buffer payload) {
    auto& connection = cookie.getConnection();
    auto* filter = connection.getThread().getAuditEventFilter();

    // @todo we should put the bucket name in the kv_enkey!
    if (!filter || !filter->isEnabled(audit_eventid, {})) {
        // No filter, or not enabled. No need to parse the JSON
        return cb::engine_errc::success;
    }

    std::string_view buffer{reinterpret_cast<const char*>(payload.data()),
                            payload.size()};

    const EventDescriptor* descr;
    try {
        descr = &AuditDescriptorManager::lookup(audit_eventid);
    } catch (const std::out_of_range&) {
        LOG_WARNING_CTX("Unknown event id provided with content",
                        {"conn_id", connection.getConnectionId()},
                        {"description", connection.getDescription()},
                        {"event_id", audit_eventid},
                        {"buffer", cb::UserDataView{buffer}});
        cookie.setErrorContext("Unknown event id");
        return cb::engine_errc::invalid_arguments;
    }

    nlohmann::json json;
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;

    {
        ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(cookie,
                                                            Code::JsonParse);
        try {
            json = nlohmann::json::parse(buffer);
        } catch (const std::exception& e) {
            LOG_WARNING_CTX(
                    "Failed to parse provided JSON. Audit event dropped",
                    {"conn_id", connection.getConnectionId()},
                    {"description", connection.getDescription()},
                    {"event_id", audit_eventid},
                    {"error", e.what()},
                    {"buffer", cb::UserDataView{buffer}});
            cookie.setErrorContext("Failed to parse JSON");
            return cb::engine_errc::invalid_arguments;
        }
    }

#ifdef CB_DEVELOPMENT_ASSERTS
    const auto& mandatory = descr->getMandatoryFields();
    if (!mandatory.empty()) {
        ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(
                cookie, Code::AuditValidate);
        nlohmann::json missing = nlohmann::json::array();
        for (auto it = mandatory.begin(); it != mandatory.end(); ++it) {
            if (json.find(it.key()) == json.end()) {
                missing.push_back(std::string{it.key()});
            }
        }
        if (!missing.empty()) {
            LOG_WARNING_CTX(
                    "Audit event is missing mandatory elements and is dropped",
                    {"conn_id", connection.getConnectionId()},
                    {"description", connection.getDescription()},
                    {"event_id", audit_eventid},
                    {"missing_keys", missing});
            cookie.setErrorContext(
                    "Audit event is missing elements specified as "
                    "mandatory");
            cookie.setErrorJsonExtras({{"missing_elements", missing}});
            return cb::engine_errc::invalid_arguments;
        }
    }
#endif

    // find the user identifiers, bucket, scope and collection
    // and call the filter
    auto iter = json.find("real_userid");
    if (iter != json.end()) {
        cb::rbac::UserIdent uid;
        try {
            uid = cb::rbac::UserIdent(*iter);
        } catch (const std::exception& exception) {
            LOG_WARNING_CTX("Audit event: Illegal value for 'real_user'",
                            {"conn_id", connection.getConnectionId()},
                            {"description", connection.getDescription()},
                            {"event_id", audit_eventid},
                            {"real_userid", cb::UserDataView(iter->dump())},
                            {"error", exception.what()});
            cookie.setErrorContext(fmt::format(
                    "Failed to parse 'real_userid': {}", exception.what()));
            return cb::engine_errc::invalid_arguments;
        }

        cb::rbac::UserIdent euid_holder;
        cb::rbac::UserIdent* euid = nullptr;
        try {
            iter = json.find("effective_userid");
            if (iter != json.end()) {
                euid_holder = cb::rbac::UserIdent(*iter);
                euid = &euid_holder;
            }
        } catch (const std::exception& exception) {
            LOG_WARNING_CTX(
                    "Audit event: Illegal value for 'effective_userid'",
                    {"conn_id", connection.getConnectionId()},
                    {"description", connection.getDescription()},
                    {"event_id", audit_eventid},
                    {"effective_userid", cb::UserDataView(iter->dump())},
                    {"error", exception.what()});
            cookie.setErrorContext(
                    fmt::format("Failed to parse 'effective_userid': {}",
                                exception.what()));
            return cb::engine_errc::invalid_arguments;
        }

        try {
            auto bucket = json.value("bucket", std::string{});
            auto sid = json.value("scope_id", std::string{});
            auto cid = json.value("collection_id", std::string{});
            std::optional<std::string_view> buck;
            std::optional<ScopeID> scope;
            std::optional<CollectionID> collection;
            if (!bucket.empty()) {
                buck = bucket;
            }
            if (!sid.empty()) {
                scope = ScopeID(sid);
            }
            if (!cid.empty()) {
                collection = CollectionID(cid);
            }

            if (filter->isFilteredOut(
                        audit_eventid, uid, euid, buck, scope, collection)) {
                return cb::engine_errc::success;
            }
        } catch (const std::exception& e) {
            LOG_WARNING_CTX("Got exception during filtering of audit event id",
                            {"conn_id", connection.getConnectionId()},
                            {"description", connection.getDescription()},
                            {"event_id", audit_eventid},
                            {"error", e.what()});
            throw;
        }
    }
    json["id"] = audit_eventid;
    json["name"] = descr->getName();
    json["description"] = descr->getDescription();
    ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(cookie, Code::Audit);
    return getAuditHandle().withRLock([audit_eventid, &json](auto& handle) {
        if (!handle) {
            return cb::engine_errc::too_busy;
        }
        return handle->put_event(audit_eventid, std::move(json))
                       ? cb::engine_errc::success
                       : cb::engine_errc::too_busy;
    });
}

namespace cb::audit {
std::unordered_set<std::string> getDeksInUse() {
    return getAuditHandle().withRLock(
            [](auto& handle) { return handle->get_deks_in_use(); });
}

void pruneDeks(const std::vector<std::string>& keys) {
    getAuditHandle().withRLock(
            [&keys](auto& handle) { handle->prune_deks(keys); });
}

void addSessionTerminated(const Connection& c) {
    if (!c.isAuthenticated() ||
        !isEnabled(MEMCACHED_AUDIT_SESSION_TERMINATED, c, nullptr)) {
        return;
    }
    auto root = create_memcached_audit_object(
            MEMCACHED_AUDIT_SESSION_TERMINATED, c, c.getUser(), {});
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

void add(Cookie& cookie, Operation operation, const DocKeyView& key) {
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

    if (!isEnabled(id, cookie)) {
        return;
    }

    const auto& connection = cookie.getConnection();
    auto root = create_memcached_audit_object(
            id, connection, connection.getUser(), cookie.getEffectiveUser());
    root["bucket"] = connection.getBucket().name;
    root["key"] = key.makeDocKeyWithoutCollectionID().toPrintableString();
    root["collection_id"] = key.getCollectionID().to_string();
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

void initialize_audit() {
    // Make sure that we initialize the descriptor manager _before_ we
    // start the audit daemon to ensure that it its singleton gets freed
    // after the audit daemon
    AuditDescriptorManager::lookup(AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON);

    /* Start the audit daemon */
    auto audit =
            cb::audit::create_audit_daemon(Settings::instance().getAuditFile());
    if (!audit) {
        FATAL_ERROR(EXIT_FAILURE, "FATAL: Failed to start audit daemon");
    }
    *getAuditHandle().wlock() = std::move(audit);
}

void shutdown_audit() {
    getAuditHandle().wlock()->reset();
}

cb::engine_errc reconfigure_audit(Cookie& cookie) {
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(cookie,
                                                        Code::AuditReconfigure);

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
    ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(cookie,
                                                        Code::AuditStats);
    getAuditHandle().withRLock([&collector](auto& handle) {
        if (handle) {
            handle->stats(collector);
        }
    });
}
