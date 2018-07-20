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

#include <memcached/engine.h>
#include <platform/platform.h>

/**
 * The following structure is passed between the memcached core in order
 * to create the audit subsystem
 */
typedef struct {
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
    void (*notify_io_complete)(gsl::not_null<const void*> cookie,
                               ENGINE_ERROR_CODE status);
} AUDIT_EXTENSION_DATA;

/**
 * Forward declaration of the AuditHandle which holds the instance data
 */
class Audit;

class AuditDeleter {
public:
    void operator()(Audit* audit);
};

/**
 * Start the audit daemon
 *
 * @param extension_data the default configuration data to be used
 * @return The newly created audit daemon handle upon success
 */
std::unique_ptr<Audit, AuditDeleter> start_auditdaemon(
        const AUDIT_EXTENSION_DATA* extension_data);

/**
 * Update the audit daemon with the specified configuration file
 *
 * @param handle the handle to the audit instance to use
 * @param config the configuration file to use.
 * @param cookie the command cookie to notify when we're done
 * @return true if success (and the cookie will be signalled when reconfigure
 *              is complete)
 */
bool configure_auditdaemon(Audit& handle,
                           const std::string& config,
                           gsl::not_null<const void*> cookie);

/**
 * Put an audit event into the audit trail
 *
 * @param audit_eventid The identifier for the event to insert
 * @param payload the JSON encoded payload to insert to the audit trail
 * @return true if the event was successfully added to the audit
 *              queue (may be dropped at a later time)
 *         false if an error occurred while trying to insert the
 *               event to the audit queue.
 */
bool put_audit_event(Audit& handle,
                     uint32_t audit_eventid,
                     cb::const_char_buffer payload);

/**
 * method called from the core to collect statistics information from
 * the audit subsystem
 *
 * @param add_stats a callback function to add information to the response
 * @param cookie the cookie representing the command
 */
void process_auditd_stats(Audit& handle,
                          ADD_STAT add_stats,
                          gsl::not_null<const void*> cookie);

namespace cb {
namespace audit {

/**
 * The event state listener is a callback function being called
 * every time the state for an event id change state (enabled / disabled)
 */
typedef void (*EventStateListener)(uint32_t id, bool enabled);

/**
 * Add a listener to be called whenever an event identifier change state
 *
 * The callback is called with the event id and the new state for the event,
 * and the _global_ on/off switch is signalled by passing event id 0
 *
 * This information may be used to cache which events are enabled or
 * disabled in memcached (to avoid building up audit events in the frontend
 * threads which will be dropped by the audit daemon later on).
 */
void add_event_state_listener(Audit& handle, EventStateListener listener);

/**
 * Fire the event state listener for _ALL_ of the events.
 *
 * The use case for this is for clients who wants to know the state of
 * all events (and initialize their cache)
 *
 * NOTE: This method should _ONLY_ be called before the memcached
 *       server is accepting audit events as it operates on one of
 *       the internal datastructures without locking it (the event
 *       descriptor array).
 */
void notify_all_event_states(Audit& handle);
}
}
