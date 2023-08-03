/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "auditconfig.h"
#include "auditfile.h"
#include "event.h"
#include "eventdescriptor.h"

#include <folly/SynchronizedPtr.h>
#include <memcached/audit_interface.h>
#include <platform/platform_thread.h>
#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

class AuditEventFilter;

class AuditImpl : public cb::audit::Audit {
public:
    // Implementation of the public API
    bool put_event(uint32_t event_id, nlohmann::json payload) override;
    void stats(const StatCollector& collector) override;
    bool configure_auditdaemon(std::string config,
                               CookieIface& cookie) override;
    // End public API

    explicit AuditImpl(std::string config_file, std::string host);
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

    /**
     * The entry point for the thread used to drain the generated audit events
     */
    void consume_events();

    std::unique_ptr<AuditEventFilter> createAuditEventFilter() override;

    /// A simple generation counter which change every time the current
    /// configuration change.
    static std::atomic<uint64_t> generation;

    /// Write the provided json to the audit trail
    bool write_to_audit_trail(const nlohmann::json& json);

protected:
    /**
     * Create an internal audit event structure
     *
     * @param event_id the event identifier to use
     * @param payload the json payload to populate with the mandatory fields
     */
    void create_audit_event(uint32_t event_id, nlohmann::json& payload);

    /// The global on-off switch if audit is enabled or not. It is an
    /// atomic variable as it is used from the front-end threads
    /// as part of requesting stats; and whenever we try to submit
    /// audit events.
    std::atomic_bool enabled{false};

    /// The current configuration used by the daemon.
    AuditConfig config;

    /// The current audit log file we're using
    AuditFile auditfile;

    /// The name of the configuration file currently in use
    std::string configfile;

    /// The thread id of the consumer thread
    std::thread consumer;

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

    /// The hostname we want to inject to the audit events
    const std::string hostname;

    /// The event filter currently in use by the audit daemon
    folly::SynchronizedPtr<std::unique_ptr<AuditEventFilter>> event_filter;

private:
    const size_t max_audit_queue = 50000;
};
