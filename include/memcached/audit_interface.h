/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <memcached/extension.h>
#include <memcached/visibility.h>
#include <platform/platform.h>

/**
 * Response codes for audit operations.
 */
typedef enum {
    /** The command executed successfully */
    AUDIT_SUCCESS = 0x00,
    /**
     * A fatal error occurred, and the server should shut down as soon
     * as possible
     */
    AUDIT_FATAL = 0x01,
    /** Generic failure. */
    AUDIT_FAILED = 0x02,
    /** performing configuration would block */
    AUDIT_EWOULDBLOCK = 0x03
} AUDIT_ERROR_CODE;

/**
 * The following structure is passed between the memcached core in order
 * to create the audit subsystem
 */
typedef struct {
    /**
     * The logger instance
     */
    EXTENSION_LOGGER_DESCRIPTOR* log_extension;

    /**
     * The name of the configuration file to use
     */
    const char* configfile;

    /**
     * Pointer to the method to notify that the background task for the
     * requested command represented by the cookie completed
     *
     * @param cookie the cookie representing the command
     * @param status the return code for the operation
     */
    void (* notify_io_complete)(const void* cookie,
                                ENGINE_ERROR_CODE status);
} AUDIT_EXTENSION_DATA;

/**
 * Forward declaration of the AuditHandle which holds the instance data
 */
class Audit;

/**
 * Start the audit daemon
 *
 * @todo refactor to return the handle to the newly created audit daemon
 *
 * @param extension_data the default configuration data to be used
 * @param handle where to store the audit handle
 * @return AUDIT_SUCCESS for success, AUDIT_FAILED if an error occurred
 */
MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE start_auditdaemon(const AUDIT_EXTENSION_DATA* extension_data,
                                   Audit **handle);

/**
 * Update the audit daemon with the specified configuration file
 *
 * @param handle the handle to the audit instance to use
 * @param config the configuration file to use.
 * @param cookie the command cookie to notify when we're done
 * @return AUDIT_EWOULDBLOCK the configuration is to be scheduled (cookie
 *                           will be signalled when it is done)
 *         AUDIT_FAILED if we failed to update the configuration
 */
MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE configure_auditdaemon(Audit* handle,
                                       const char* config,
                                       const void* cookie);

/**
 * Put an audit event into the audit trail
 *
 * @param audit_eventid The identifier for the event to insert
 * @param payload the JSON encoded payload to insert to the audit trail
 * @param length the number of bytes in the payload
 * @return AUDIT_SUCCESS if the event was successfully added to the audit
 *                       queue (may be dropped at a later time)
 *         AUDIT_FAILED if an error occured while trying to insert the
 *                      event to the audit queue.
 */
MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE put_audit_event(Audit* handle,
                                 const uint32_t audit_eventid,
                                 const void* payload,
                                 const size_t length);

/**
 * Shut down the audit daemon
 *
 * @param handle the audit daemon handle
 *
 */
MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE shutdown_auditdaemon(Audit* handle);

/**
 * method called from the core to collect statistics information from
 * the audit subsystem
 *
 * @param add_stats a callback function to add information to the response
 * @param cookie the cookie representing the command
 */
MEMCACHED_PUBLIC_API
void process_auditd_stats(Audit* handle,
                          ADD_STAT add_stats,
                          const void* cookie);
