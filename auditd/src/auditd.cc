/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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

#include <algorithm>
#include <cstring>
#include <sstream>
#include <cJSON.h>
#include <platform/dirutils.h>
#include <memcached/isotime.h>
#include "auditd.h"
#include "audit.h"
#include "config.h"
#include "auditd_audit_events.h"
#include "event.h"

Audit audit;

void process_auditd_stats(ADD_STAT add_stats, void *c) {
    const char *enabled;
    enabled = audit.config.is_auditd_enabled() ? "true" : "false";
    add_stats("enabled", (uint16_t)strlen("enabled"),
              enabled, (uint32_t)strlen(enabled), c);
    std::stringstream num_of_dropped_events;
    num_of_dropped_events << audit.dropped_events;
    add_stats("dropped_events", (uint16_t)strlen("dropped_events"),
              num_of_dropped_events.str().c_str(),
              (uint32_t)num_of_dropped_events.str().length(), c);

}

static void (*audit_processed_listener)(void) = NULL;
void audit_set_audit_processed_listener(void (*listener)(void)) {
    audit_processed_listener = listener;
}


static void consume_events(void *arg) {
    cb_mutex_enter(&audit.producer_consumer_lock);
    while (!audit.terminate_audit_daemon) {
        assert(audit.filleventqueue != NULL);
        if (audit.filleventqueue->empty()) {
            // wait up after 10 secs no matter what
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

        assert(audit.processeventqueue != NULL);
        while (!audit.processeventqueue->empty()) {
            Event *event = audit.processeventqueue->front();
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


AUDIT_ERROR_CODE start_auditdaemon(const AUDIT_EXTENSION_DATA *extension_data) {
    Audit::logger = extension_data->log_extension;
    char host[128];
    gethostname(host, sizeof(host));
    Audit::hostname = std::string(host);
    Audit::notify_io_complete = extension_data->notify_io_complete;

    if (extension_data->version != 1) {
        Audit::log_error(AuditErrorCode::AUDIT_EXTENSION_DATA_ERROR, NULL);
        return AUDIT_FAILED;
    }
    AuditConfig::min_file_rotation_time = extension_data->min_file_rotation_time;
    AuditConfig::max_file_rotation_time = extension_data->max_file_rotation_time;

    if (cb_create_named_thread(&audit.consumer_tid, consume_events, NULL, 0,
                               "mc:auditd") != 0) {
        Audit::log_error(AuditErrorCode::CB_CREATE_THREAD_ERROR, NULL);
        return AUDIT_FAILED;
    }
    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE configure_auditdaemon(const char *config, const void *cookie) {
    AUDIT_ERROR_CODE res;
    audit.configfile = std::string(config);
    if (cookie == NULL) {
        res = audit.configure() ? AUDIT_SUCCESS : AUDIT_FAILED;
    } else {
        res = audit.add_reconfigure_event(cookie) ? AUDIT_EWOULDBLOCK : AUDIT_FAILED;
    }
    return res;
}


AUDIT_ERROR_CODE put_audit_event(const uint32_t audit_eventid,
                                 const void *payload, size_t length) {
    if (audit.config.is_auditd_enabled()) {
        if (!audit.add_to_filleventqueue(audit_eventid, (char *)payload, length)) {
            return AUDIT_FAILED;
        }
    }
    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE put_json_audit_event(uint32_t id, cJSON *event) {
    cJSON *ts = cJSON_GetObjectItem(event, "timestamp");
    if (ts == NULL) {
        std::string timestamp = ISOTime::generatetimestamp();
        cJSON_AddStringToObject(event, "timestamp", timestamp.c_str());
    }

    char *text = cJSON_PrintUnformatted(event);
    AUDIT_ERROR_CODE ret = put_audit_event(id, text, strlen(text));
    cJSON_Free(text);

    return ret;
}


AUDIT_ERROR_CODE shutdown_auditdaemon(const char *config) {
    if (config != NULL && audit.config.is_auditd_enabled()) {
        // send event to say we are shutting down the audit daemon
        cJSON *payload = cJSON_CreateObject();
        assert(payload != NULL);
        if (!audit.create_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                                      payload)) {
            cJSON_Delete(payload);
            audit.clean_up();
            return AUDIT_FAILED;
        }
        char *content = cJSON_Print(payload);
        assert(content != NULL);
        cJSON_Delete(payload);

        if (!audit.add_to_filleventqueue(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON,
                                         content, strlen(content))) {
            cJSON_Free(content);
            audit.clean_up();
            return AUDIT_FAILED;
        }
        cJSON_Free(content);
    }
    cb_mutex_enter(&audit.producer_consumer_lock);
    audit.terminate_audit_daemon = true;
    cb_mutex_exit(&audit.producer_consumer_lock);

    /* consumer thread maybe waiting for an event to arrive so need
     * to send it a broadcast so it can exit cleanly
     */
    cb_mutex_enter(&audit.producer_consumer_lock);
    cb_cond_broadcast(&audit.events_arrived);
    cb_mutex_exit(&audit.producer_consumer_lock);
    if (cb_join_thread(audit.consumer_tid) != 0) {
        audit.clean_up();
        return AUDIT_FAILED;
    }
    audit.clean_up();
    return AUDIT_SUCCESS;
}

static std::atomic<time_t> auditd_test_timetravel_offset;

time_t auditd_time(time_t *tloc) {
    time_t now;
    time(&now);
    now += auditd_test_timetravel_offset.load(std::memory_order_acquire);

    if (tloc != NULL) {
        *tloc = now;
    }
    return now;
}

MEMCACHED_PUBLIC_API
void audit_test_timetravel(time_t offset) {
    auditd_test_timetravel_offset += offset;
}
