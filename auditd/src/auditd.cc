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

#include <cJSON.h>
#include <logger/logger.h>
#include <memcached/audit_interface.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <platform/strerror.h>
#include <algorithm>
#include <cstring>
#include <sstream>

#include "audit.h"
#include "auditd.h"
#include "auditd_audit_events.h"
#include "event.h"

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
                if (audit.auditfile.maybe_rotate_files()) {
                    // If the file was rotated then we need to open a new
                    // audit.log file.
                    audit.auditfile.ensure_open();
                }
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
        }
        audit.auditfile.flush();
        cb_mutex_enter(&audit.producer_consumer_lock);
    }
    cb_mutex_exit(&audit.producer_consumer_lock);

    // close the auditfile
    audit.auditfile.close();
}

static std::string gethostname() {
    char host[128];
    if (gethostname(host, sizeof(host)) != 0) {
        throw std::runtime_error("gethostname() failed: " + cb_strerror());
    }

    return std::string(host);
}

std::unique_ptr<Audit, AuditDeleter> start_auditdaemon(
        const std::string& config_file, SERVER_COOKIE_API* server_cookie_api) {
    if (!cb::logger::isInitialized()) {
        throw std::invalid_argument(
                "start_auditdaemon: logger must have been created");
    }

    std::unique_ptr<Audit, AuditDeleter> holder;

    try {
        holder.reset(new Audit(config_file, server_cookie_api, gethostname()));
        if (!holder->configfile.empty() && !holder->configure()) {
            return {};
        }

        if (cb_create_named_thread(&holder->consumer_tid,
                                   consume_events,
                                   holder.get(),
                                   0,
                                   "mc:auditd") != 0) {
            LOG_WARNING("Failed to create audit thread");
            return {};
        }
        holder->consumer_thread_running.store(true);
    } catch (std::runtime_error& err) {
        LOG_WARNING("{}", err.what());
        return {};
    } catch (std::bad_alloc&) {
        LOG_WARNING("Failed to start audit: Out of memory");
        return {};
    }

    return holder;
}

bool configure_auditdaemon(Audit& handle,
                           const std::string& config,
                           gsl::not_null<const void*> cookie) {
    return handle.add_reconfigure_event(config.c_str(), cookie.get());
}

bool put_audit_event(Audit& handle,
                     uint32_t audit_eventid,
                     cb::const_char_buffer payload) {
    if (handle.config.is_auditd_enabled()) {
        if (!handle.add_to_filleventqueue(audit_eventid, payload)) {
            return false;
        }
    }
    return true;
}

void AuditDeleter::operator()(Audit* handle) {
    if (handle->config.is_auditd_enabled()) {
        // send event to say we are shutting down the audit daemon
        nlohmann::json payload;
        handle->create_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                                   payload);
        handle->add_to_filleventqueue(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                                      payload.dump());
    }

    handle->clean_up();
    delete handle;
}

void process_auditd_stats(Audit& handle,
                          ADD_STAT add_stats,
                          gsl::not_null<const void*> cookie) {
    const char* enabled;
    enabled = handle.config.is_auditd_enabled() ? "true" : "false";
    add_stats("enabled",
              (uint16_t)strlen("enabled"),
              enabled,
              (uint32_t)strlen(enabled),
              cookie.get());
    std::stringstream num_of_dropped_events;
    num_of_dropped_events << handle.dropped_events;
    add_stats("dropped_events",
              (uint16_t)strlen("dropped_events"),
              num_of_dropped_events.str().c_str(),
              (uint32_t)num_of_dropped_events.str().length(),
              cookie.get());
}

namespace cb {
namespace audit {

void add_event_state_listener(Audit& handle, EventStateListener listener) {
    handle.add_event_state_listener(listener);
}

void notify_all_event_states(Audit& handle) {
    handle.notify_all_event_states();
}

} // namespace audit
} // namespace cb
