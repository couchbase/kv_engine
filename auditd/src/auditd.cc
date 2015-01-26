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
#include "auditd.h"
#include "audit.h"
#include "config.h"

Audit audit;

static void consume_events(void *arg) {
    cb_mutex_enter(&audit.producer_consumer_lock);
    while (!audit.terminate_audit_daemon) {
        bool drop_events = false;
        assert(audit.filleventqueue != NULL);
        if (audit.filleventqueue->empty()) {
            cb_cond_wait(&audit.events_arrived, &audit.producer_consumer_lock);
        }
        /* now have producer_consumer lock!
         * event(s) have arrived or shutdown requested
         */
        swap(audit.processeventqueue, audit.filleventqueue);
        cb_mutex_exit(&audit.producer_consumer_lock);
        // Now outside of the producer_consumer_lock
        if (audit.auditfile.af.is_open() &&
            audit.auditfile.time_to_rotate_log(audit.config.rotate_interval)) {
            audit.auditfile.close_and_rotate_log(audit.config.log_path,
                                                 audit.config.archive_path);
        }
        assert(audit.processeventqueue != NULL);
        if (!audit.processeventqueue->empty() && !audit.auditfile.af.is_open()) {
            if (!audit.auditfile.open(audit.config.log_path)) {
                drop_events = true;
            }
        }
        while (!audit.processeventqueue->empty()) {
            if (drop_events) {
                Audit::log_error(DROPPING_EVENT_ERROR,
                                 audit.processeventqueue->front().payload.c_str());
            } else if (!audit.process_event(audit.processeventqueue->front())) {
                Audit::log_error(EVENT_PROCESSING_ERROR, NULL);
            }
            audit.processeventqueue->pop();
        }

        cb_mutex_enter(&audit.producer_consumer_lock);
        if (audit.reloading_config_file) {
            cb_cond_wait(&audit.reload_finished, &audit.producer_consumer_lock);
        }
    }
    cb_mutex_exit(&audit.producer_consumer_lock);
    if (audit.auditfile.af.is_open()) {
      audit.auditfile.close_and_rotate_log(audit.config.log_path,
                                           audit.config.archive_path);
    }
}


AUDIT_ERROR_CODE initialize_auditdaemon(const char *config,
                                        const AUDIT_EXTENSION_DATA *extension_data) {
    Audit::logger = extension_data->log_extension;
    char host[128];
    gethostname(host, sizeof(host));
    Audit::hostname = std::string(host);

    if (extension_data->version != 1) {
        Audit::log_error(AUDIT_EXTENSION_DATA_ERROR, NULL);
        audit.clean_up();
        return AUDIT_FAILED;
    }
    AuditConfig::min_file_rotation_time = extension_data->min_file_rotation_time;
    AuditConfig::max_file_rotation_time = extension_data->max_file_rotation_time;

    std::string configuration = audit.load_file(config);
    if (configuration.empty()) {
        audit.clean_up();
        return AUDIT_FAILED;
    }
    if (!audit.config.initialize_config(configuration)) {
        audit.clean_up();
        return AUDIT_FAILED;
    }

    if (!audit.auditfile.cleanup_old_logfile(audit.config.log_path,
                                            audit.config.archive_path)) {
        audit.clean_up();
        return AUDIT_FAILED;
    }

    std::stringstream audit_events_file;
    audit_events_file << CouchbaseDirectoryUtilities::dirname(std::string(config))
                      << DIRECTORY_SEPARATOR_CHARACTER << "audit_events.json";
    std::string str = audit.load_file(audit_events_file.str().c_str());
    if (str.empty()) {
        audit.clean_up();
        return AUDIT_FAILED;
    }
    cJSON *json_ptr = cJSON_Parse(str.c_str());
    if (json_ptr == NULL) {
        Audit::log_error(JSON_PARSING_ERROR, str.c_str());
        audit.clean_up();
        return AUDIT_FAILED;
    }
    if (!audit.process_module_descriptor(json_ptr->child)) {
        assert(json_ptr != NULL);
        cJSON_Delete(json_ptr);
        audit.clean_up();
        return AUDIT_FAILED;
    }

    assert(json_ptr != NULL);
    cJSON_Delete(json_ptr);

    if (cb_create_thread(&audit.consumer_tid, consume_events, NULL, 0) != 0) {
        Audit::log_error(CB_CREATE_THREAD_ERROR, NULL);
        audit.clean_up();
        return AUDIT_FAILED;
    }

    // create an event stating that the audit daemon has been started
    cJSON *payload = cJSON_CreateObject();
    if (!audit.create_audit_event(0x1000, payload)) {
        assert(payload != NULL);
        cJSON_Delete(payload);
        audit.clean_up();
        return AUDIT_FAILED;
    }
    char *content = cJSON_Print(payload);
    assert(payload != NULL);
    cJSON_Delete(payload);

    if (!audit.add_to_filleventqueue(0x1000, content, strlen(content))) {
        assert(content != NULL);
        free(content);
        audit.clean_up();
        return AUDIT_FAILED;
    }
    free(content);

    // create an event stating that the audit daemon has been enabled/disabled
    payload = cJSON_CreateObject();
    uint32_t event_id = (audit.config.auditd_enabled) ? 0x1003 : 0x1004;
    if (!audit.create_audit_event(event_id, payload)) {
        assert(payload != NULL);
        cJSON_Delete(payload);
        audit.clean_up();
        return AUDIT_FAILED;
    }
    content = cJSON_Print(payload);
    assert(payload != NULL);
    cJSON_Delete(payload);

    if (!audit.add_to_filleventqueue(event_id, content, strlen(content))) {
        assert(content != NULL);
        free(content);
        audit.clean_up();
        return AUDIT_FAILED;
    }
    free(content);

    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE put_audit_event(const uint32_t audit_eventid,
                                 const void *payload, size_t length) {
    if (audit.config.auditd_enabled) {
        if (!audit.add_to_filleventqueue(audit_eventid, (char *)payload, length)) {
            return AUDIT_FAILED;
        }
    }
    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE reload_auditdaemon_config(const char *config) {
    bool initial_auditd_enabled = audit.config.auditd_enabled;
    cb_mutex_enter(&audit.producer_consumer_lock);
    audit.reloading_config_file = true;
    cb_mutex_exit(&audit.producer_consumer_lock);

    std::string configuration = audit.load_file(config);
    if (configuration.empty()) {
        return AUDIT_FAILED;
    }
    if (!audit.config.initialize_config(configuration)) {
        return AUDIT_FAILED;
    }
    // iterate through the events map and update the sync and enabled flags
    typedef std::map<uint32_t, EventData*>::iterator it_type;
    cb_mutex_enter(&audit.producer_consumer_lock);
    for(it_type iterator = audit.events.begin(); iterator != audit.events.end(); iterator++) {
        iterator->second->sync = (std::find(audit.config.sync.begin(),
                                               audit.config.sync.end(), iterator->first)
                                     != audit.config.sync.end()) ? true : false;
        iterator->second->enabled = (std::find(audit.config.enabled.begin(),
                                               audit.config.enabled.end(), iterator->first)
                                     != audit.config.enabled.end()) ? true : false;
    }
    cb_mutex_exit(&audit.producer_consumer_lock);

    // send event to state we have reconfigured
    cJSON *payload = cJSON_CreateObject();
    if (!audit.create_audit_event(0x1002, payload)) {
        assert(payload != NULL);
        cJSON_Delete(payload);
        return AUDIT_FAILED;
    }
    char *content = cJSON_Print(payload);
    assert(payload != NULL);
    cJSON_Delete(payload);

    if (!audit.add_to_filleventqueue(0x1002, content, strlen(content))) {
        assert(content != NULL);
        free(content);
        return AUDIT_FAILED;
    }
    assert(content != NULL);
    free(content);

    if (!initial_auditd_enabled && audit.config.auditd_enabled) {
        // send event to say we are enabling the audit daemon
        cJSON *payload = cJSON_CreateObject();
        if (!audit.create_audit_event(0x1003, payload)) {
            assert(payload != NULL);
            cJSON_Delete(payload);
            return AUDIT_FAILED;
        }
        char *content = cJSON_Print(payload);
        assert(payload != NULL);
        cJSON_Delete(payload);

        if (!audit.add_to_filleventqueue(0x1003, content, strlen(content))) {
            assert(content != NULL);
            free(content);
            return AUDIT_FAILED;
        }
        assert(content != NULL);
        free(content);

    } else if (initial_auditd_enabled && !audit.config.auditd_enabled) {
        // send event to say we are disabling the audit daemon
        cJSON *payload = cJSON_CreateObject();
        if (!audit.create_audit_event(0x1004, payload)) {
            assert(payload != NULL);
            cJSON_Delete(payload);
            return AUDIT_FAILED;
        }
        char *content = cJSON_Print(payload);
        assert(payload != NULL);
        cJSON_Delete(payload);

        if (!audit.add_to_filleventqueue(0x1004, content, strlen(content))) {
            assert(content != NULL);
            free(content);
            return AUDIT_FAILED;
        }
        assert(content != NULL);
        free(content);
    }

    // notify finished doing the reload and that events have been sent
    cb_mutex_enter(&audit.producer_consumer_lock);
    audit.reloading_config_file = false;
    cb_cond_broadcast(&audit.reload_finished);
    cb_cond_broadcast(&audit.events_arrived);
    cb_mutex_exit(&audit.producer_consumer_lock);

    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE shutdown_auditdaemon(void) {
    cJSON *payload = cJSON_CreateObject();
    if (!audit.create_audit_event(0x1001, payload)) {
        assert(payload != NULL);
        cJSON_Delete(payload);
        audit.clean_up();
        return AUDIT_FAILED;
    }
    char *content = cJSON_Print(payload);
    assert(payload != NULL);
    cJSON_Delete(payload);

    if (!audit.add_to_filleventqueue(0x1001, content, strlen(content))) {
        assert(content != NULL);
        free(content);
        audit.clean_up();
        return AUDIT_FAILED;
    }
    free(content);

    cb_mutex_enter(&audit.producer_consumer_lock);
    audit.terminate_audit_daemon = true;
    cb_mutex_exit(&audit.producer_consumer_lock);

    /* consumer thread(s) maybe waiting for an event to arrive so need
     * to send it a broadcast so they can exit cleanly
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
