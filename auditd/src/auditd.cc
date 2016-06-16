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

#include <algorithm>
#include <cJSON.h>
#include <cstring>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>
#include <platform/strerror.h>
#include <sstream>

#include "audit.h"
#include "auditd.h"
#include "auditd_audit_events.h"
#include "event.h"

/**
 * In order to test the audit daemon we allow clients to get notifications
 * every time the audit daemon is done processing a scheduled event.
 *
 * The listener may be set by calling audit_set_audit_processed_listener
 */
static void (* audit_processed_listener)() = nullptr;

/**
 * The entry point for the thread used to drain the generated audit events
 *
 * @param arg not used
 * @todo refactor the method to get the audit daemon handle from arg
 */
static void consume_events(void* arg) {
    if (arg == nullptr) {
        throw std::invalid_argument(
            "consume_events: arg should be the audit instance");
    }
    Audit& audit = *reinterpret_cast<Audit*>(arg);

    cb_mutex_enter(&audit.producer_consumer_lock);
    while (!audit.terminate_audit_daemon) {
        if (audit.filleventqueue->empty()) {
            cb_cond_timedwait(&audit.events_arrived,
                              &audit.producer_consumer_lock,
                              audit.auditfile.get_seconds_to_rotation() * 1000);
            if (audit.filleventqueue->empty()) {
                // We timed out, so just rotate the files
                audit.auditfile.maybe_rotate_files();
            }
        }
        /* now have producer_consumer lock!
         * event(s) have arrived or shutdown requested
         */
        swap(audit.processeventqueue, audit.filleventqueue);
        cb_mutex_exit(&audit.producer_consumer_lock);
        // Now outside of the producer_consumer_lock

        while (!audit.processeventqueue->empty()) {
            Event* event = audit.processeventqueue->front();
            if (!event->process(audit)) {
                audit.dropped_events++;
            }
            audit.processeventqueue->pop();
            delete event;
            if (audit_processed_listener) {
                audit_processed_listener();
            }
        }
        audit.auditfile.flush();
        cb_mutex_enter(&audit.producer_consumer_lock);
    }
    cb_mutex_exit(&audit.producer_consumer_lock);

    // close the auditfile
    audit.auditfile.close();
}

static std::string gethostname(void) {
    char host[128];
    if (gethostname(host, sizeof(host)) != 0) {
        throw std::runtime_error("gethostname() failed: " + cb_strerror());
    }

    return std::string(host);
}

MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE start_auditdaemon(const AUDIT_EXTENSION_DATA* extension_data,
                                   Audit** handle) {
    if (handle == nullptr) {
        throw std::invalid_argument("start_auditdaemon: handle can't be null");
    }
    if (extension_data == nullptr) {
        throw std::invalid_argument(
            "start_auditdaemon: extension_data can't be null");
    }
    if (extension_data->log_extension == nullptr) {
        throw std::invalid_argument("start_auditdaemon: logger can't be null");
    }

    std::unique_ptr<Audit> holder;

    try {
        holder.reset(new Audit);

        Audit* audit = holder.get();
        audit->logger = extension_data->log_extension;
        audit->notify_io_complete = extension_data->notify_io_complete;
        audit->hostname = gethostname();
        if (extension_data->configfile != nullptr) {
            audit->configfile.assign(extension_data->configfile);
        }

        if (!audit->configfile.empty() && !audit->configure()) {
            return AUDIT_FAILED;
        }

        if (cb_create_named_thread(&audit->consumer_tid, consume_events,
                                   audit, 0, "mc:auditd") != 0) {
            audit->logger->log(EXTENSION_LOG_WARNING, nullptr,
                               "Failed to create audit thread");
            return AUDIT_FAILED;
        }
        audit->consumer_thread_running.store(true);
    } catch (std::runtime_error& err) {
        extension_data->log_extension->log(EXTENSION_LOG_WARNING, nullptr, "%s",
                                           err.what());
        return AUDIT_FAILED;
    } catch (std::bad_alloc&) {
        extension_data->log_extension->log(EXTENSION_LOG_WARNING, nullptr,
                                           "Out of memory");
        return AUDIT_FAILED;
    }

    *handle = holder.release();
    return AUDIT_SUCCESS;
}

MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE configure_auditdaemon(Audit* handle,
                                       const char* config,
                                       const void* cookie) {
    if (handle == nullptr) {
        throw std::invalid_argument(
            "configure_auditdaemon: handle can't be nullptr");
    }
    if (cookie == nullptr) {
        throw std::invalid_argument(
            "configure_auditdaemon: cookie can't be nullptr");
    }
    if (handle->add_reconfigure_event(config, cookie)) {
        return AUDIT_EWOULDBLOCK;
    } else {
        return AUDIT_FAILED;
    }
}

MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE put_audit_event(Audit* handle,
                                 const uint32_t audit_eventid,
                                 const void* payload,
                                 const size_t length) {
    if (handle == nullptr) {
        throw std::invalid_argument("put_audit_event: handle can't be nullptr");
    }
    if (handle->config.is_auditd_enabled()) {
        if (!handle->add_to_filleventqueue(audit_eventid,
                                           (const char*)payload,
                                           length)) {
            return AUDIT_FAILED;
        }
    }
    return AUDIT_SUCCESS;
}

MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE put_json_audit_event(Audit* handle,
                                      uint32_t id,
                                      cJSON* event) {
    cJSON* ts = cJSON_GetObjectItem(event, "timestamp");
    if (ts == nullptr) {
        std::string timestamp = ISOTime::generatetimestamp();
        cJSON_AddStringToObject(event, "timestamp", timestamp.c_str());
    }

    auto text = to_string(event, false);
    return put_audit_event(handle, id, text.data(), text.length());
}

MEMCACHED_PUBLIC_API
AUDIT_ERROR_CODE shutdown_auditdaemon(Audit* handle) {
    if (handle == nullptr) {
        throw std::invalid_argument(
            "shutdown_auditdaemon: handle can't be nullptr");
    }

    // Put the handle in a unique_ptr to ensure that it is deleted in
    // all return paths
    std::unique_ptr<Audit> holder(handle);

    if (handle->config.is_auditd_enabled()) {
        // send event to say we are shutting down the audit daemon
        unique_cJSON_ptr payload(cJSON_CreateObject());
        if ((payload.get() == nullptr) ||
            !handle->create_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                                        payload.get()) ||
            !handle->add_to_filleventqueue(
                AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                to_string(payload, false))) {
            handle->clean_up();
            return AUDIT_FAILED;
        }
    }

    if (handle->clean_up()) {
        return AUDIT_SUCCESS;
    }

    return AUDIT_FAILED;
}

MEMCACHED_PUBLIC_API
void process_auditd_stats(Audit* handle,
                          ADD_STAT add_stats,
                          const void* cookie) {
    if (handle == nullptr) {
        throw std::invalid_argument(
            "process_auditd_stats: handle can't be nullptr");
    }
    const char* enabled;
    enabled = handle->config.is_auditd_enabled() ? "true" : "false";
    add_stats("enabled", (uint16_t)strlen("enabled"),
              enabled, (uint32_t)strlen(enabled), cookie);
    std::stringstream num_of_dropped_events;
    num_of_dropped_events << handle->dropped_events;
    add_stats("dropped_events", (uint16_t)strlen("dropped_events"),
              num_of_dropped_events.str().c_str(),
              (uint32_t)num_of_dropped_events.str().length(), cookie);
}

MEMCACHED_PUBLIC_API
void audit_set_audit_processed_listener(void (* listener)()) {
    audit_processed_listener = listener;
}
