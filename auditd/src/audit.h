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
#include <memory>
#include <queue>
#include <atomic>

#include <cJSON.h>
#include "memcached/audit_interface.h"
#include "memcached/types.h"
#include "auditconfig.h"
#include "auditfile.h"
#include "auditd.h"
#include "eventdescriptor.h"

class Event;

class Audit {
public:
    AuditConfig config;
    std::map<uint32_t,EventDescriptor*> events;

    // We maintain two Event queues. At any one time one will be used to accept
    // new events, and the other will be processed. The two queues are swapped
    // periodically.
    std::unique_ptr<std::queue<Event*>> processeventqueue;
    std::unique_ptr<std::queue<Event*>> filleventqueue;

    bool terminate_audit_daemon;
    std::string configfile;
    cb_thread_t consumer_tid;
    std::atomic_bool consumer_thread_running;
    cb_cond_t processeventqueue_empty;
    cb_cond_t events_arrived;
    cb_mutex_t producer_consumer_lock;
    static EXTENSION_LOGGER_DESCRIPTOR *logger;
    static std::string hostname;
    static void (*notify_io_complete)(const void *cookie,
                                      ENGINE_ERROR_CODE status);
    AuditFile auditfile;
    std::atomic<uint32_t> dropped_events;

    Audit()
        : processeventqueue(new std::queue<Event*>()),
          filleventqueue(new std::queue<Event*>()),
          terminate_audit_daemon(false),
          dropped_events(0),
          max_audit_queue(50000) {
        consumer_thread_running.store(false);
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
    bool add_to_filleventqueue(const uint32_t event_id,
                               const char *payload,
                               const size_t length);
    bool add_to_filleventqueue(const uint32_t event_id,
                               const std::string& payload) {
        return add_to_filleventqueue(event_id, payload.data(),
                                     payload.length());
    }

    bool add_reconfigure_event(const char *configfile, const void *cookie);
    bool create_audit_event(uint32_t event_id, cJSON *payload);
    bool terminate_consumer_thread(void);
    void clear_events_map(void);
    void clear_events_queues(void);
    bool clean_up(void);

    static void log_error(const AuditErrorCode return_code,
                          const std::string& string = "");
    static std::string load_file(const char *file);

private:
    size_t max_audit_queue;
};

#endif
