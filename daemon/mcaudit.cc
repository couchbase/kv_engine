/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
    auto root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root.get(), "reason", reason);

    do_audit(c, MEMCACHED_AUDIT_AUTHENTICATION_FAILED, root,
             "Failed to send AUTH FAILED audit event");
}

void audit_auth_success(const Connection *c) {
    auto root = create_memcached_audit_object(c);
    do_audit(c, MEMCACHED_AUDIT_AUTHENTICATION_SUCCEEDED, root,
             "Failed to send AUTH SUCCESS audit event");
}


void audit_bucket_flush(const Connection *c, const char *bucket) {
    auto root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root.get(), "bucket", bucket);

    do_audit(c, MEMCACHED_AUDIT_BUCKET_FLUSH, root,
             "Failed to send BUCKET_FLUSH audit event");
}


void audit_dcp_open(const Connection *c) {
    if (c->isAdmin()) {
        LOG_INFO(c, "Open DCP stream with admin credentials");
    } else {
        auto root = create_memcached_audit_object(c);
        cJSON_AddStringToObject(root.get(), "bucket", getBucketName(c));

        do_audit(c, MEMCACHED_AUDIT_OPENED_DCP_CONNECTION, root,
                 "Failed to send DCP open connection "
                 "audit event to audit daemon");
    }
}

void audit_command_access_failed(const McbpConnection *c) {
    auto root = create_memcached_audit_object(c);
    char buffer[256];
    memset(buffer, 0, sizeof(buffer));
    // Deliberately ignore failure of bytes_to_output_string
    // We'll either have a partial string or no string.
    bytes_to_output_string(buffer, sizeof(buffer), c->getId(), true,
                           "Access to command is not allowed:",
                           reinterpret_cast<const char*>(&c->getBinaryHeader()),
                           sizeof(protocol_binary_request_header));
    cJSON_AddStringToObject(root.get(), "packet", buffer);
    do_audit(c, MEMCACHED_AUDIT_COMMAND_ACCESS_FAILURE, root, buffer);
}

void audit_invalid_packet(const McbpConnection *c) {
    auto root = create_memcached_audit_object(c);
    char buffer[256];
    memset(buffer, 0, sizeof(buffer));
    // Deliberately ignore failure of bytes_to_output_string
    // We'll either have a partial string or no string.
    bytes_to_output_string(buffer, sizeof(buffer), c->getId(), true,
                           "Invalid Packet:",
                           reinterpret_cast<const char*>(&c->getBinaryHeader()),
                           sizeof(protocol_binary_request_header));
    cJSON_AddStringToObject(root.get(), "packet", buffer);
    do_audit(c, MEMCACHED_AUDIT_INVALID_PACKET, root, buffer);
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
}
