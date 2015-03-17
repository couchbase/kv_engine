/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <memcached/audit_interface.h>
#include <cJSON.h>

static const char unknown[] = "unknown";

/**
 * Get the username associated with the given connection
 *
 * @param c the connection object
 * @return the username associated with the given connection or "unknown"
 */
static const char *get_username(const conn *c)
{
    const void *username = unknown;

    if (c->sasl_conn && (cbsasl_getprop(c->sasl_conn,
                                        CBSASL_USERNAME,
                                        &username) != CBSASL_OK)) {
        username = unknown;
    }
    return username;
}

/**
 * Get the bucket the connection is bound to.
 *
 * Currently the bucket name is the same as the authenticated
 * user, but when we're moving to RBAC this will differ
 *
 * @param c the connection object
 * @return the name of the bucket currently connected to
 */
static const char *get_bucketname(const conn *c)
{
    static const char default_bucket[] = "default";
    const void *bucketname = default_bucket;

    if (c->sasl_conn && (cbsasl_getprop(c->sasl_conn,
                                        CBSASL_USERNAME,
                                        &bucketname) != CBSASL_OK)) {
        bucketname = default_bucket;
    }
    return bucketname;
}


/**
 * Create the typical memcached audit object. It constists of a
 * timestamp, the socket endpoints and the creds. Then each audit event
 * may add event-specific content.
 *
 * @param c the connection object
 * @return the cJSON object containing the basic information
 */
static cJSON *create_memcached_audit_object(const conn *c)
{
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "peername",
                            c->peername ? c->peername : unknown);
    cJSON_AddStringToObject(root, "sockname",
                            c->sockname ? c->sockname : unknown);
    cJSON *source = cJSON_CreateObject();
    cJSON_AddStringToObject(source, "source", "memcached");
    cJSON_AddStringToObject(source, "user", get_username(c));
    cJSON_AddItemToObject(root, "real_userid", source);

    return root;
}

/**
 * Convert the JSON object to text and send it to the audit framework
 * and then release all allocated resources
 *
 * @param c the connection object requesting the call
 * @param id the audit identifier
 * @param event the payload of the audit description
 * @param warn what to log if we're failing to put the audit event
 */
static void do_audit(const conn *c, uint32_t id, cJSON *event, const char *warn) {
    if (put_json_audit_event(id, event) != AUDIT_SUCCESS) {
        char *text = cJSON_PrintUnformatted(event);
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c, "%s: %s",
                                        warn, text);
        cJSON_Free(text);
    }
    cJSON_Delete(event);
}

void audit_auth_failure(const conn *c, const char *reason)
{
    cJSON *root = create_memcached_audit_object(c);
    cJSON_AddStringToObject(root, "reason", reason);

    do_audit(c, MEMCACHED_AUDIT_AUTHENTICATION_FAILED, root,
             "Failed to send AUTH FAILED audit event");
}

void audit_dcp_open(const conn *c)
{
    if (c->admin) {
        settings.extensions.logger->log(EXTENSION_LOG_INFO, c,
                                        "Open DCP stream with admin credentials");
    } else {
        cJSON *root = create_memcached_audit_object(c);
        cJSON_AddStringToObject(root, "bucket", get_bucketname(c));

        do_audit(c, MEMCACHED_AUDIT_OPENED_DCP_CONNECTION, root,
                 "Failed to send DCP open connection "
                 "audit event to audit daemon");
    }
}
