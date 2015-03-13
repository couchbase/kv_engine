/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#ifndef AUDIT_H
#define AUDIT_H

#include <inttypes.h>
#include <map>
#include <queue>
#include <atomic>
#include <cJSON.h>
#include "memcached/audit_interface.h"
#include "memcached/types.h"
#include "auditconfig.h"
#include "auditfile.h"
#include "eventdata.h"
#include "auditd.h"

class Event;

class Audit {
public:
    AuditConfig config;
    std::map<uint32_t,EventData*> events;
    std::queue<Event*> eventqueue1;
    std::queue<Event*> eventqueue2;
    std::queue<Event*> *filleventqueue;
    std::queue<Event*> *processeventqueue;
    bool terminate_audit_daemon;
    std::string auditfile_open_time_string;
    std::string configfile;
    cb_thread_t consumer_tid;
    cb_cond_t processeventqueue_empty;
    cb_cond_t events_arrived;
    cb_mutex_t producer_consumer_lock;
    static EXTENSION_LOGGER_DESCRIPTOR *logger;
    static std::string hostname;
    static void (*notify_io_complete)(const void *cookie,
                                      ENGINE_ERROR_CODE status);
    AuditFile auditfile;
    std::atomic<uint32_t> dropped_events;

    Audit(void) : dropped_events(0), max_audit_queue(50000) {
        processeventqueue = &eventqueue1;
        filleventqueue = &eventqueue2;
        cb_cond_initialize(&processeventqueue_empty);
        cb_cond_initialize(&events_arrived);
        cb_mutex_initialize(&producer_consumer_lock);
    }

    ~Audit(void) {
        clean_up();
        cb_cond_destroy(&processeventqueue_empty);
        cb_cond_destroy(&events_arrived);
        cb_mutex_destroy(&producer_consumer_lock);
    }

    bool initialize_event_data_structures(cJSON *event_ptr);
    bool process_module_data_structures(cJSON *module);
    bool process_module_descriptor(cJSON *module_descriptor);
    bool configure(void);
    bool process_event(const Event* event);
    bool add_to_filleventqueue(const uint32_t event_id,
                               const char *payload,
                               const size_t length);
    bool add_reconfigure_event(const void *cookie);
    bool create_audit_event(uint32_t event_id, cJSON *payload);
    void clear_events_map(void);
    void clear_events_queues(void);
    void clean_up(void);

    static void log_error(const ErrorCode return_code, const char *string);
    static std::string load_file(const char *file);
    static bool is_timestamp_format_correct (std::string& str);

private:
    size_t max_audit_queue;
};

#endif
