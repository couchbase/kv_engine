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
#include "memcached.h"
#include "mcaudit.h"
#include "memcached_audit_events.h"
#include "buckets.h"
#include "debug_helpers.h"
#include "runtime.h"

#include <memcached/audit_interface.h>
#include <cJSON.h>
#include <memcached/isotime.h>

static std::atomic_bool audit_enabled{false};

static const int First = MEMCACHED_AUDIT_OPENED_DCP_CONNECTION;
static const int Last = MEMCACHED_AUDIT_DOCUMENT_DELETE;

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
            LOG_NOTICE(nullptr,
                       "Audit changed from: %s to: %s",
                       !enable ? "enabled" : "disabled",
                       enable ? "enabled" : "disabled");
        }
    }

    if (id >= First && id <= Last) {
        if (events[id - First].compare_exchange_strong(expected, enable)) {
            LOG_INFO(nullptr,
                     "Audit descriptor %u changed from: %s to: %s",
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
static unique_cJSON_ptr create_memcached_audit_object(const Connection *c) {
    cJSON *root = cJSON_CreateObject();

    std::string timestamp = ISOTime::generatetimestamp();
    cJSON_AddStringToObject(root, "timestamp", timestamp.c_str());

    cJSON_AddStringToObject(root, "peername", c->getPeername().c_str());
    cJSON_AddStringToObject(root, "sockname", c->getSockname().c_str());
    cJSON *source = cJSON_CreateObject();
    cJSON_AddStringToObject(source, "source", "memcached");
    cJSON_AddStringToObject(source, "user", c->getUsername());
    cJSON_AddItemToObject(root, "real_userid", source);

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
    auto status = put_audit_event(get_audit_handle(), id, text.data(),
                                  text.length());

    if (status != AUDIT_SUCCESS) {
        LOG_WARNING(c, "%s: %s", warn, text.c_str());
    }
}

void audit_auth_failure(const Connection *c, const char *reason) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_FAILED)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root.get(), "reason", reason);

    do_audit(c, MEMCACHED_AUDIT_AUTHENTICATION_FAILED, root,
             "Failed to send AUTH FAILED audit event");
}

void audit_auth_success(const Connection *c) {
    if (!isEnabled(MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    do_audit(c, MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED, root,
             "Failed to send AUTH SUCCESS audit event");
}


void audit_bucket_flush(const Connection *c, const char *bucket) {
    if (!isEnabled(MEMCACHED_AUDIT_BUCKET_FLUSH)) {
        return;
    }
    auto root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root.get(), "bucket", bucket);

    do_audit(c, MEMCACHED_AUDIT_BUCKET_FLUSH, root,
             "Failed to send BUCKET_FLUSH audit event");
}


void audit_dcp_open(const Connection *c) {
    if (!isEnabled(MEMCACHED_AUDIT_OPENED_DCP_CONNECTION)) {
        return;
    }
    if (c->isInternal()) {
        LOG_INFO(c, "Open DCP stream with admin credentials");
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

bool mc_audit_event(uint32_t audit_eventid,
                    const void* payload,
                    size_t length) {
    if (!audit_enabled) {
        return true;
    }

    return put_audit_event(get_audit_handle(), audit_eventid,
                           payload, length) == AUDIT_SUCCESS;
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
            root.get(), "key", connection.getPrintableKey().c_str());

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
    AUDIT_EXTENSION_DATA audit_extension_data;
    memset(&audit_extension_data, 0, sizeof(audit_extension_data));
    audit_extension_data.log_extension = settings.extensions.logger;
    audit_extension_data.notify_io_complete = notify_io_complete;
    audit_extension_data.configfile = settings.getAuditFile().c_str();
    Audit* handle = nullptr;
    if (start_auditdaemon(&audit_extension_data, &handle) != AUDIT_SUCCESS) {
        FATAL_ERROR(EXIT_FAILURE, "FATAL: Failed to start audit daemon");
    }
    set_audit_handle(handle);
    cb::audit::add_event_state_listener(handle, event_state_listener);
    cb::audit::notify_all_event_states(handle);
}
