/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
#include "mcaudit.h"
#include "buckets.h"
#include "connection.h"
#include "debug_helpers.h"
#include "log_macros.h"
#include "logger/logger.h"
#include "memcached.h"
#include "memcached_audit_events.h"
#include "runtime.h"
#include <cJSON.h>
#include <logger/logger.h>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>

static cb::audit::UniqueAuditPtr auditHandle;

static std::atomic_bool audit_enabled{false};

static const int First = MEMCACHED_AUDIT_OPENED_DCP_CONNECTION;
static const int Last = MEMCACHED_AUDIT_SELECT_BUCKET;

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

void setEnabled(uint32_t id, bool enable) {
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
 * Create the typical memcached audit object. It constists of a
 * timestamp, the socket endpoints and the creds. Then each audit event
 * may add event-specific content.
 *
 * @param c the connection object
 * @return the cJSON object containing the basic information
 */
static unique_cJSON_ptr create_memcached_audit_object(const Connection* c) {
    cJSON *root = cJSON_CreateObject();

    std::string timestamp = ISOTime::generatetimestamp();
    cJSON_AddStringToObject(root, "timestamp", timestamp.c_str());

    cJSON_AddStringToObject(root, "peername", c->getPeername().c_str());
    cJSON_AddStringToObject(root, "sockname", c->getSockname().c_str());
    cJSON *domain = cJSON_CreateObject();
    cJSON_AddStringToObject(domain, "domain", "memcached");
    cJSON_AddStringToObject(domain, "user", c->getUsername());
    cJSON_AddItemToObject(root, "real_userid", domain);

    return unique_cJSON_ptr(root);
}

/**
 * Convert the JSON object to text and send it to the audit framework
 *
 * @param c the connection object requesting the call
 * @param id the audit identifier
 * @param event the payload of the audit description
 * @param warn what to log if we're failing to put the audit event
 */
static void do_audit(const Connection* c,
                     uint32_t id,
                     unique_cJSON_ptr& event,
                     const char* warn) {
    auto text = to_string(event, false);
    if (!auditHandle->put_event(id, text)) {
        LOG_WARNING("{}: {}", warn, text);
    }
}

void audit_auth_failure(const Connection* c, const char* reason) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_FAILED)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root.get(), "reason", reason);

    do_audit(c, MEMCACHED_AUDIT_AUTHENTICATION_FAILED, root,
             "Failed to send AUTH FAILED audit event");
}

void audit_auth_success(const Connection* c) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    do_audit(c, MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED, root,
             "Failed to send AUTH SUCCESS audit event");
}

void audit_bucket_selection(const Connection& c) {
    if (!isEnabled(MEMCACHED_AUDIT_SELECT_BUCKET)) {
        return;
    }
    const auto& bucket = c.getBucket();
    // Don't audit that we're jumping into the "no bucket"
    if (bucket.type != BucketType::NoBucket) {
        auto root = create_memcached_audit_object(&c);
        cJSON_AddStringToObject(root.get(), "bucket", c.getBucket().name);
        do_audit(&c,
                 MEMCACHED_AUDIT_SELECT_BUCKET,
                 root,
                 "Failed to send SELECT BUCKET audit event");
    }
}

void audit_bucket_flush(const Connection* c, const char* bucket) {
    if (!isEnabled(MEMCACHED_AUDIT_EXTERNAL_MEMCACHED_BUCKET_FLUSH)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root.get(), "bucket", bucket);

    do_audit(c, MEMCACHED_AUDIT_EXTERNAL_MEMCACHED_BUCKET_FLUSH, root,
             "Failed to send EXTERNAL_MEMCACHED_BUCKET_FLUSH audit event");
}

void audit_dcp_open(const Connection* c) {
    if (!isEnabled(MEMCACHED_AUDIT_OPENED_DCP_CONNECTION)) {
        return;
    }
    if (c->isInternal()) {
        LOG_INFO("Open DCP stream with admin credentials");
    } else {
        auto root = create_memcached_audit_object(c);
        cJSON_AddStringToObject(root.get(), "bucket", getBucketName(c));

        do_audit(c, MEMCACHED_AUDIT_OPENED_DCP_CONNECTION, root,
                 "Failed to send DCP open connection "
                 "audit event to audit daemon");
    }
}

void audit_set_privilege_debug_mode(const Connection* c, bool enable) {
    if (!isEnabled(MEMCACHED_AUDIT_PRIVILEGE_DEBUG_CONFIGURED)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    cJSON_AddBoolToObject(root.get(), "enable", enable);
    do_audit(c, MEMCACHED_AUDIT_PRIVILEGE_DEBUG_CONFIGURED, root,
             "Failed to send modifications in privilege debug state "
             "audit event to audit daemon");
}

void audit_privilege_debug(const Connection* c,
                           const std::string& command,
                           const std::string& bucket,
                           const std::string& privilege,
                           const std::string& context) {
    if (!isEnabled(MEMCACHED_AUDIT_PRIVILEGE_DEBUG)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root.get(), "command", command.c_str());
    cJSON_AddStringToObject(root.get(), "bucket", bucket.c_str());
    cJSON_AddStringToObject(root.get(), "privilege", privilege.c_str());
    cJSON_AddStringToObject(root.get(), "context", context.c_str());

    do_audit(c, MEMCACHED_AUDIT_PRIVILEGE_DEBUG, root,
             "Failed to send privilege debug audit event to audit daemon");
}

void audit_command_access_failed(const Cookie& cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE)) {
        return;
    }
    const auto& connection = cookie.getConnection();
    auto root = create_memcached_audit_object(&connection);
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
    cJSON_AddStringToObject(root.get(), "packet", buffer);
    do_audit(&connection, MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE, root, buffer);
}

void audit_invalid_packet(const Cookie& cookie) {
    if (!isEnabled(MEMCACHED_AUDIT_INVALID_PACKET)) {
        return;
    }
    const auto& connection = cookie.getConnection();
    auto root = create_memcached_audit_object(&connection);
    char buffer[256];
    memset(buffer, 0, sizeof(buffer));
    const auto packet = cookie.getPacket();
    // Deliberately ignore failure of bytes_to_output_string
    // We'll either have a partial string or no string.
    bytes_to_output_string(buffer,
                           sizeof(buffer),
                           connection.getId(),
                           true,
                           "Invalid Packet:",
                           reinterpret_cast<const char*>(packet.data()),
                           packet.size());
    cJSON_AddStringToObject(root.get(), "packet", buffer);
    do_audit(&connection, MEMCACHED_AUDIT_INVALID_PACKET, root, buffer);
}

bool mc_audit_event(uint32_t audit_eventid, cb::const_byte_buffer payload) {
    if (!audit_enabled) {
        return true;
    }

    cb::const_char_buffer buffer{reinterpret_cast<const char*>(payload.data()),
                                 payload.size()};
    return auditHandle->put_event(audit_eventid, buffer);
}

namespace cb {
namespace audit {
namespace document {

void add(const Cookie& cookie, Operation operation) {
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
    auto root = create_memcached_audit_object(&connection);
    cJSON_AddStringToObject(root.get(), "bucket", connection.getBucket().name);
    cJSON_AddStringToObject(
            root.get(), "key", cookie.getPrintableRequestKey().c_str());

    switch (operation) {
    case Operation::Read:
        do_audit(&connection,
                 MEMCACHED_AUDIT_DOCUMENT_READ,
                 root,
                 "Failed to send document read audit event to audit daemon");
        break;
    case Operation::Lock:
        do_audit(&connection,
                 MEMCACHED_AUDIT_DOCUMENT_LOCKED,
                 root,
                 "Failed to send document locked audit event to audit daemon");
        break;
    case Operation::Modify:
        do_audit(&connection,
                 MEMCACHED_AUDIT_DOCUMENT_MODIFY,
                 root,
                 "Failed to send document modify audit event to audit daemon");
        break;
    case Operation::Delete:
        do_audit(&connection,
                 MEMCACHED_AUDIT_DOCUMENT_DELETE,
                 root,
                 "Failed to send document delete audit event to audit daemon");
        break;
    }
}

} // namespace documnent
} // namespace audit
} // namespace cb

static void event_state_listener(uint32_t id, bool enabled) {
    setEnabled(id, enabled);
}

void initialize_audit() {
    /* Start the audit daemon */
    auditHandle = cb::audit::create_audit_daemon(settings.getAuditFile(),
                                                 get_server_api()->cookie);
    if (!auditHandle) {
        FATAL_ERROR(EXIT_FAILURE, "FATAL: Failed to start audit daemon");
    }
    auditHandle->add_event_state_listener(event_state_listener);
    auditHandle->notify_all_event_states();
}

void shutdown_audit() {
    auditHandle.reset();
}

ENGINE_ERROR_CODE reconfigure_audit(Cookie& cookie) {
    if (auditHandle->configure_auditdaemon(settings.getAuditFile(),
                                           static_cast<void*>(&cookie))) {
        return ENGINE_EWOULDBLOCK;
    }

    return ENGINE_FAILED;
}

void stats_audit(ADD_STAT add_stats, Cookie& cookie) {
    auditHandle->stats(add_stats, &cookie);
}
