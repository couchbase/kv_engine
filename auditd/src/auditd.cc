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

void process_auditd_stats(ADD_STAT add_stats, void *c) {
    const char *enabled;
    enabled = audit.config.auditd_enabled ? "true" : "false";
    add_stats("enabled", (uint16_t)strlen("enabled"),
              enabled, (uint32_t)strlen(enabled), c);
    std::stringstream num_of_dropped_events;
    num_of_dropped_events << audit.dropped_events;
    add_stats("dropped_events", (uint16_t)strlen("dropped_events"),
              num_of_dropped_events.str().c_str(),
              num_of_dropped_events.str().length(), c);

}


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
                Audit::log_error(OPEN_AUDITFILE_ERROR, NULL);
                drop_events = true;
            }
        }
        while (!audit.processeventqueue->empty()) {
            if (drop_events) {
                audit.dropped_events++;
            } else if (!audit.process_event(audit.processeventqueue->front())) {
                audit.dropped_events++;
                Audit::log_error(EVENT_PROCESSING_ERROR, NULL);
            }
            audit.processeventqueue->pop();
        }

        cb_mutex_enter(&audit.producer_consumer_lock);
        if (audit.configuring) {
            cb_cond_broadcast(&audit.processeventqueue_empty);
        }
    }
    cb_mutex_exit(&audit.producer_consumer_lock);
    if (audit.auditfile.af.is_open()) {
      audit.auditfile.close_and_rotate_log(audit.config.log_path,
                                           audit.config.archive_path);
    }
}



AUDIT_ERROR_CODE start_auditdaemon(const AUDIT_EXTENSION_DATA *extension_data) {
    Audit::logger = extension_data->log_extension;
    char host[128];
    gethostname(host, sizeof(host));
    Audit::hostname = std::string(host);

    if (extension_data->version != 1) {
        Audit::log_error(AUDIT_EXTENSION_DATA_ERROR, NULL);
        return AUDIT_FAILED;
    }
    AuditConfig::min_file_rotation_time = extension_data->min_file_rotation_time;
    AuditConfig::max_file_rotation_time = extension_data->max_file_rotation_time;

    if (cb_create_thread(&audit.consumer_tid, consume_events, NULL, 0) != 0) {
        Audit::log_error(CB_CREATE_THREAD_ERROR, NULL);
        return AUDIT_FAILED;
    }
    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE configure_auditdaemon(const char *config) {
    cb_mutex_enter(&audit.producer_consumer_lock);
    audit.configuring = true;
    while (!audit.processeventqueue->empty()) {
        cb_cond_wait(&audit.processeventqueue_empty, &audit.producer_consumer_lock);
    }
    cb_mutex_exit(&audit.producer_consumer_lock);
    std::string configuration = audit.load_file(config);
    if (configuration.empty()) {
        return AUDIT_FAILED;
    }
    if (!audit.config.initialize_config(configuration)) {
        return AUDIT_FAILED;
    }
    if (!audit.auditfile.af.is_open()) {
        if (!audit.auditfile.cleanup_old_logfile(audit.config.log_path,
                                                audit.config.archive_path)) {
            return AUDIT_FAILED;
        }
    }

    std::stringstream audit_events_file;
    // @todo check during loading of configfile that descriptors_path is defined
    std::stringstream tmp;
    tmp  << audit.config.descriptors_path << DIRECTORY_SEPARATOR_CHARACTER << "audit_events.json";
    if (audit.config.descriptors_path.empty() || (!AuditFile::file_exists(tmp.str()))) {
        audit_events_file << CouchbaseDirectoryUtilities::dirname(std::string(config));
    } else {
        audit_events_file << audit.config.descriptors_path;
    }
    audit_events_file  << DIRECTORY_SEPARATOR_CHARACTER << "audit_events.json";
    std::string str = audit.load_file(audit_events_file.str().c_str());
    if (str.empty()) {
        return AUDIT_FAILED;
    }
    cJSON *json_ptr = cJSON_Parse(str.c_str());
    if (json_ptr == NULL) {
        Audit::log_error(JSON_PARSING_ERROR, str.c_str());
        return AUDIT_FAILED;
    }
    if (!audit.process_module_descriptor(json_ptr->child)) {
        cJSON_Delete(json_ptr);
        return AUDIT_FAILED;
    }
    cJSON_Delete(json_ptr);

    // iterate through the events map and update the sync and enabled flags
    typedef std::map<uint32_t, EventData*>::iterator it_type;
    for(it_type iterator = audit.events.begin(); iterator != audit.events.end(); iterator++) {
        iterator->second->sync = (std::find(audit.config.sync.begin(),
                                            audit.config.sync.end(), iterator->first)
                                  != audit.config.sync.end()) ? true : false;
        iterator->second->enabled = (std::find(audit.config.disabled.begin(),
                                               audit.config.disabled.end(), iterator->first)
                                     != audit.config.disabled.end()) ? false : true;
    }

    // notify finished doing the configuration
    cb_mutex_enter(&audit.producer_consumer_lock);
    audit.configuring = false;
    cb_mutex_exit(&audit.producer_consumer_lock);

    // send event to state we have configured the audit daemon
    cJSON *payload = cJSON_CreateObject();
    assert(payload != NULL);
    if (!audit.create_audit_event(0x1000, payload)) {
        cJSON_Delete(payload);
        return AUDIT_FAILED;
    }
    char *content = cJSON_Print(payload);
    assert(content != NULL);
    cJSON_Delete(payload);

    if (!audit.add_to_filleventqueue(0x1000, content, strlen(content))) {
        free(content);
        return AUDIT_FAILED;
    }
    free(content);

    if (audit.config.auditd_enabled) {
        // send event to say we have enabled the audit daemon
        cJSON *payload = cJSON_CreateObject();
        if (!audit.create_audit_event(0x1001, payload)) {
            cJSON_Delete(payload);
            return AUDIT_FAILED;
        }
        char *content = cJSON_Print(payload);
        assert(content != NULL);
        cJSON_Delete(payload);

        if (!audit.add_to_filleventqueue(0x1001, content, strlen(content))) {
            free(content);
            return AUDIT_FAILED;
        }
        free(content);

    } else {
        // send event to say we have disabled the audit daemon
        cJSON *payload = cJSON_CreateObject();
        assert(payload != NULL);
        if (!audit.create_audit_event(0x1002, payload)) {
            cJSON_Delete(payload);
            return AUDIT_FAILED;
        }
        char *content = cJSON_Print(payload);
        assert(content != NULL);
        cJSON_Delete(payload);

        if (!audit.add_to_filleventqueue(0x1002, content, strlen(content))) {
            free(content);
            return AUDIT_FAILED;
        }
        free(content);
    }

    // notify that events have been sent
    cb_mutex_enter(&audit.producer_consumer_lock);
    cb_cond_broadcast(&audit.events_arrived);
    cb_mutex_exit(&audit.producer_consumer_lock);

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


AUDIT_ERROR_CODE shutdown_auditdaemon(const char *config) {
    if (config != NULL && audit.config.auditd_enabled) {
        // send event to say we are shutting down the audit daemon
        cJSON *payload = cJSON_CreateObject();
        assert(payload != NULL);
        if (!audit.create_audit_event(0x1003, payload)) {
            cJSON_Delete(payload);
            audit.clean_up();
            return AUDIT_FAILED;
        }
        char *content = cJSON_Print(payload);
        assert(content != NULL);
        cJSON_Delete(payload);

        if (!audit.add_to_filleventqueue(0x1003, content, strlen(content))) {
            free(content);
            audit.clean_up();
            return AUDIT_FAILED;
        }
        free(content);
    }
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


const char* generatetimestamp(void) {
    return Audit::generatetimestamp().c_str();
}
