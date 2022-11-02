/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "audit.h"
#include "audit_descriptor_manager.h"
#include "audit_event_filter.h"
#include "configureevent.h"
#include "event.h"
#include "eventdescriptor.h"

#include <auditd/couchbase_audit_events.h>
#include <logger/logger.h>
#include <memcached/isotime.h>
#include <memcached/server_cookie_iface.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/dirutils.h>
#include <statistics/collector.h>
#include <statistics/definitions.h>
#include <utilities/logtags.h>

#include <algorithm>
#include <chrono>
#include <queue>
#include <string>

using namespace std::string_view_literals;

std::atomic<uint64_t> AuditImpl::generation = {0};

AuditImpl::AuditImpl(std::string config_file, std::string host)
    : Audit(),
      auditfile(host),
      configfile(std::move(config_file)),
      hostname(std::move(host)) {
    if (configfile.empty()) {
        // Create an event filter where everything gets filtered out
        // (disable everything).
        // Note: This is only used via unit tests
        auto filter = AuditEventFilter::create(generation + 1, {});
        event_filter.withWLockPointer(
                [&filter](auto& f) { f = std::move(filter); });
        ++generation;
    } else if (!configure()) {
        throw std::runtime_error(
                "Audit::Audit(): Failed to configure audit daemon");
    }

    std::unique_lock<std::mutex> lock(producer_consumer_lock);
    consumer = create_thread([this]() { consume_events(); }, "mc:auditd");
    events_arrived.wait(lock);
}

AuditImpl::~AuditImpl() {
    nlohmann::json payload;
    create_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON, payload);
    put_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON, payload);

    {
        // Set the flag to request the audit consumer to stop
        std::lock_guard<std::mutex> guard(producer_consumer_lock);
        stop_audit_consumer = true;
        // The consume thread may be waiting on the condition variable
        // so we should kick it in the but as well to make sure that
        // it is let loose
        events_arrived.notify_all();
    }

    // Wait for the consumer thread to stop
    consumer.join();
}

void AuditImpl::create_audit_event(uint32_t event_id, nlohmann::json& payload) {
    // Add common fields to the audit event
    payload["timestamp"] = ISOTime::generatetimestamp();
    const cb::rbac::UserIdent real_userid("@memcached",
                                          cb::rbac::Domain::Local);
    payload["real_userid"] = real_userid.to_json();
    const auto& evt = AuditDescriptorManager::lookup(event_id);
    payload["id"] = event_id;
    payload["name"] = evt.getName();
    payload["description"] = evt.getDescription();

    switch (event_id) {
        case AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON:
            payload["auditd_enabled"] = enabled.load();
            payload["hostname"] = hostname;
            payload["log_path"] = config.get_log_directory();
            payload["rotate_interval"] = config.get_rotate_interval();
            payload["version"] = config.get_version();
            payload["uuid"] = config.get_uuid();
            return;

        case AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON:
            return;
    }

    throw std::logic_error(
            "Audit::create_audit_event: Invalid event identifier specified");
}

bool AuditImpl::reconfigure(std::string file) {
    configfile = std::move(file);
    return configure();
}

bool AuditImpl::configure() {
    bool is_enabled_before_reconfig = enabled;
    std::string file_content;
    try {
        file_content = cb::io::loadFile(configfile, std::chrono::seconds{5});
        if (file_content.empty()) {
            LOG_WARNING(R"(Audit::configure: No data in "{}")", configfile);
            return false;
        }
    } catch (const std::exception& exception) {
        LOG_WARNING(R"(Audit::configure: Failed to load "{}": {})",
                    configfile,
                    exception.what());
        return false;
    }

    nlohmann::json config_json;
    try {
        config_json = nlohmann::json::parse(file_content);
    } catch (const nlohmann::json::exception&) {
        LOG_WARNING(
                R"(Audit::configure: JSON parsing error of "{}" with content: "{}")",
                configfile,
                cb::UserDataView(file_content));
        return false;
    }

    try {
        config = AuditConfig(config_json);
    } catch (const nlohmann::json::exception& e) {
        LOG_WARNING(
                R"(Audit::configure:: Configuration error in "{}". Error: {}.)"
                R"(Content: {})",
                cb::UserDataView(configfile),
                cb::UserDataView(e.what()),
                cb::UserDataView(file_content));
        return false;
    } catch (const std::exception& exception) {
        LOG_WARNING("Audit::configure: Initialization failed: {}",
                    exception.what());
        return false;
    }

    // Until we have the server providing the new type of configuration
    // we need to convert the old style configuration to the new
    // type of configuration. Don't hold the lock while doing this
    auto filter = AuditEventFilter::create(generation + 1,
                                           config.get_audit_event_filter());
    event_filter.withWLockPointer(
            [&filter](auto& f) { f = std::move(filter); });
    ++generation;

    LOG_DEBUG("Using audit Event filter: {}",
              event_filter.rlock()->to_json().dump());
    if (!auditfile.is_open()) {
        try {
            auditfile.cleanup_old_logfile(config.get_log_directory());
        } catch (const std::exception& exception) {
            LOG_WARNING(
                    "Audit::configure(): Failed to clean up old log files: {}",
                    exception.what());
            return false;
        }
    }

    auditfile.reconfigure(config);
    enabled = config.is_auditd_enabled();

    // create event to say done reconfiguration
    if (is_enabled_before_reconfig || enabled) {
        nlohmann::json payload;
        try {
            create_audit_event(AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON, payload);
            if (!(auditfile.ensure_open() &&
                  auditfile.write_event_to_disk(payload))) {
                dropped_events++;
            }
        } catch (const std::exception& exception) {
            dropped_events++;
            LOG_WARNING(
                    "Audit::configure(): Failed to add audit event for "
                    "audit configure: {}",
                    exception.what());
        }
    }

    if (enabled) {
        // If the write_event_to_disk function returns false then it is
        // possible the audit file has been closed.  Therefore ensure
        // the file is open.
        auditfile.ensure_open();
    } else {
        // Audit is disabled, ensure that the audit file is closed
        auditfile.close();
    }

    return true;
}

bool AuditImpl::put_event(uint32_t event_id, nlohmann::json payload) {
    if (!enabled) {
        // Audit is disabled
        return true;
    }

    try {
        auto new_event = std::make_unique<Event>(event_id, std::move(payload));
        std::lock_guard<std::mutex> guard(producer_consumer_lock);
        if (filleventqueue.size() < max_audit_queue) {
            filleventqueue.push(std::move(new_event));
            events_arrived.notify_all();
            return true;
        }
    } catch (const std::bad_alloc&) {
    }

    dropped_events++;
    LOG_WARNING("Audit: Dropping audit event {}", event_id);
    return false;
}

bool AuditImpl::configure_auditdaemon(std::string file, CookieIface& cookie) {
    auto new_event = std::make_unique<ConfigureEvent>(std::move(file), cookie);
    std::lock_guard<std::mutex> guard(producer_consumer_lock);
    filleventqueue.push(std::move(new_event));
    events_arrived.notify_all();
    return true;
}

void AuditImpl::stats(const StatCollector& collector) {
    using namespace cb::stats;
    collector.addStat(Key::audit_enabled, enabled.load());
    collector.addStat(Key::audit_dropped_events, dropped_events);
}

void AuditImpl::consume_events() {
    std::unique_lock<std::mutex> lock(producer_consumer_lock);
    // Tell the main thread that we're up and running
    events_arrived.notify_one();

    while (!stop_audit_consumer) {
        if (filleventqueue.empty()) {
            events_arrived.wait_for(
                    lock,
                    std::chrono::seconds(auditfile.get_seconds_to_rotation()));
            if (filleventqueue.empty()) {
                // We timed out, so just rotate the files
                if (auditfile.maybe_rotate_files()) {
                    // If the file was rotated then we need to open a new
                    // audit.log file.
                    auditfile.ensure_open();
                }
            }
        }
        /* now have producer_consumer lock!
         * event(s) have arrived or shutdown requested
         */
        processeventqueue.swap(filleventqueue);
        lock.unlock();
        // Now outside of the producer_consumer_lock

        while (!processeventqueue.empty()) {
            auto& event = processeventqueue.front();
            try {
                if (!event->process(*this)) {
                    dropped_events++;
                }
            } catch (const std::exception& e) {
                LOG_WARNING(
                        "AuditImpl::consume_events(): Got exception while "
                        "processing event: {} ({}): {}",
                        event->id,
                        cb::tagUserData(event->payload.dump()),
                        e.what());
                dropped_events++;
            }
            processeventqueue.pop();
        }
        auditfile.flush();
        lock.lock();
    }

    // close the auditfile
    auditfile.close();
}

std::unique_ptr<AuditEventFilter> AuditImpl::createAuditEventFilter() {
    return event_filter.rlock()->clone();
}
