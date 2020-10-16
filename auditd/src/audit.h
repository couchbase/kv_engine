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
#include "auditfile.h"
#include "event.h"
#include "eventdescriptor.h"

#include <memcached/audit_interface.h>
#include <platform/platform_thread.h>

#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

class AuditImpl : public cb::audit::Audit {
public:
    // Implementation of the public API
    bool put_event(uint32_t event_id, std::string_view payload) override;
    void add_event_state_listener(
            cb::audit::EventStateListener listener) override;
    void notify_all_event_states() override;
    void stats(const StatCollector& collector) override;
    bool configure_auditdaemon(const std::string& config,
                               gsl::not_null<const void*> cookie) override;
    // End public API

    explicit AuditImpl(std::string config_file,
                       ServerCookieIface* sapi,
                       const std::string& host);
    ~AuditImpl() override;

    /**
     * Set the configuration file to the named file and reconfigure the system
     *
     * @param the file to read the configuration from
     * @return true if success, false otherwise
     */
    bool reconfigure(std::string file);

    /**
     * Read the current configuration and update the internal settings
     * to reflect the configuration
     *
     * @return true if success, false otherwise
     */
    bool configure();

    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status);

    /**
     * The entry point for the thread used to drain the generated audit events
     */
    void consume_events();

protected:
    /// The event class needs access to the configuration object
    /// and the event descriptors as part of filtering events (it'll be
    /// fixed in a later commit)
    friend class Event;

    /// The AuditDaemonFilteringTest tries to add it's own event descriptor
    /// to use in the unit tests
    friend class AuditDaemonFilteringTest;

    /**
     * Add a new event descriptor to the list of defined audit descriptors
     *
     * @param event_ptr the definition of the descriptor in JSON
     * @return true if the descriptor was successfully added
     */
    bool add_event_descriptor(const nlohmann::json& json);

    bool process_module_data_structures(const nlohmann::json& json);
    bool process_module_descriptor(const nlohmann::json& json);

    /**
     * Create an internal audit event structure
     *
     * @param event_id the event identifier to use
     * @param payload the json payload to populate with the mandatory fields
     */
    void create_audit_event(uint32_t event_id, nlohmann::json& payload);

    void notify_event_state_changed(uint32_t id, bool enabled) const;
    struct {
        mutable std::mutex mutex;
        std::vector<cb::audit::EventStateListener> clients;
    } event_state_listener;

    /// The current configuration used by the daemon
    AuditConfig config;

    /// The map of known audit events to process
    std::unordered_map<uint32_t, std::unique_ptr<EventDescriptor>> events;

    /// The current audit log file we're using
    AuditFile auditfile;

    /// The name of the configuration file currently in use
    std::string configfile;

    /// The thread id of the consumer thread
    cb_thread_t consumer_tid = {};

    /// The consumer should run until this flag is set to true
    bool stop_audit_consumer = {false};

    // We maintain two Event queues. At any one time one will be used to accept
    // new events, and the other will be processed. The two queues are swapped
    // periodically.
    std::queue<std::unique_ptr<Event>> processeventqueue;
    std::queue<std::unique_ptr<Event>> filleventqueue;
    std::condition_variable events_arrived;
    std::mutex producer_consumer_lock;

    /// The number of events currently dropped.
    std::atomic<uint32_t> dropped_events = {0};

    ServerCookieIface* cookie_api;

    /// The hostname we want to inject to the audit events
    const std::string hostname;

private:
    const size_t max_audit_queue = 50000;
};
