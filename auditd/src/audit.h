/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "auditconfig.h"
#include "auditd.h"
#include "auditfile.h"

#include <memcached/audit_interface.h>

#include <atomic>
#include <cinttypes>
#include <map>
#include <memory>
#include <queue>

class Event;
class EventDescriptor;
struct cJSON;

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
    static std::string hostname;
    AuditFile auditfile;
    std::atomic<uint32_t> dropped_events;

    explicit Audit(std::string config_file,
                   SERVER_COOKIE_API* sapi,
                   const std::string& host);
    ~Audit();

    bool initialize_event_data_structures(cJSON *event_ptr);
    bool process_module_data_structures(cJSON *module);
    bool process_module_descriptor(cJSON *module_descriptor);
    bool configure();
    bool add_to_filleventqueue(const uint32_t event_id,
                               const char *payload,
                               const size_t length);
    bool add_to_filleventqueue(const uint32_t event_id,
                               const std::string& payload);

    bool add_reconfigure_event(const char *configfile, const void *cookie);
    bool create_audit_event(uint32_t event_id, nlohmann::json& payload);
    bool terminate_consumer_thread();
    void clear_events_map();
    void clear_events_queues();
    bool clean_up();

    static void log_error(const AuditErrorCode return_code,
                          const std::string& string = "");

    /**
     * Add a listener to notify state changes for individual events.
     *
     * @param listener the callback function
     */
    void add_event_state_listener(cb::audit::EventStateListener listener);

    void notify_all_event_states();

    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status);

protected:
    void notify_event_state_changed(uint32_t id, bool enabled) const;
    struct {
        mutable std::mutex mutex;
        std::vector<cb::audit::EventStateListener> clients;
    } event_state_listener;

    SERVER_COOKIE_API* cookie_api;

private:
    const size_t max_audit_queue = 50000;
};

