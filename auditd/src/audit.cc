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

#include <auditd/couchbase_audit_events.h>
#include <cbcrypto/file_utilities.h>
#include <dek/manager.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/timeutils.h>
#include <statistics/collector.h>
#include <statistics/definitions.h>
#include <utilities/logtags.h>
#include <chrono>
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
    try {
        nlohmann::json payload;
        create_audit_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON, payload);
        put_event(AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON, payload);
    } catch (const std::exception& exception) {
        LOG_WARNING_CTX("AuditImpl::~AuditImpl(): Failed to add audit event",
                        {"error", exception.what()});
    }

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
    payload["timestamp"] = cb::time::timestamp();
    const cb::rbac::UserIdent real_userid("@memcached",
                                          cb::rbac::Domain::Local);
    payload["real_userid"] = real_userid;
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
        file_content = cb::dek::Manager::instance().load(
                cb::dek::Entity::Config, configfile, std::chrono::seconds{5});
        if (file_content.empty()) {
            LOG_WARNING_CTX("Audit::configure: No data in file",
                            {"path", configfile});
            return false;
        }
    } catch (const std::exception& exception) {
        LOG_WARNING_CTX("Audit::configure: Failed to load",
                        {"path", configfile},
                        {"error", exception.what()});
        return false;
    }

    nlohmann::json config_json;
    try {
        config_json = nlohmann::json::parse(file_content);
    } catch (const nlohmann::json::exception&) {
        LOG_WARNING_CTX("Audit::configure: JSON parsing error",
                        {"path", configfile},
                        {"content", cb::UserDataView(file_content)});
        return false;
    }

    try {
        config = AuditConfig(config_json);
    } catch (const nlohmann::json::exception& e) {
        LOG_WARNING_CTX("Audit::configure:: Configuration error",
                        {"path", cb::UserDataView(configfile)},
                        {"error", cb::UserDataView(e.what())},
                        {"content", cb::UserDataView(file_content)});
        return false;
    } catch (const std::exception& exception) {
        LOG_WARNING_CTX("Audit::configure: Initialization failed",
                        {"error", exception.what()});
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

    if (!auditfile.is_open()) {
        auditfile.remove_audit_link(config.get_log_directory());
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
            LOG_WARNING_CTX(
                    "Audit::configure(): Failed to add audit event for "
                    "audit configure",
                    {"error", exception.what()});
        }
    }

    log_directory = auditfile.get_log_directory();

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

std::unordered_set<std::string> AuditImpl::get_deks_in_use() const {
    bool unencrypted = false;
    auto deks = log_directory.withLock([&unencrypted](auto& dir) {
        return cb::crypto::findDeksInUse(
                dir,
                [&unencrypted](const auto& path) {
                    const auto filename = path.string();
                    if (filename.find("-audit.log") != std::string::npos) {
                        unencrypted = true;
                        return false;
                    }

                    return filename.find("-audit.cef") != std::string::npos;
                },
                [](auto message, const auto& ctx) {
                    LOG_WARNING_CTX(message, ctx);
                });
    });
    if (unencrypted) {
        deks.insert(cb::crypto::DataEncryptionKey::UnencryptedKeyId);
    }

    // Add the "current" key as it is always supposed to be "in use"
    auto& manager = cb::dek::Manager::instance();
    auto key = manager.lookup(cb::dek::Entity::Audit);
    if (key) {
        deks.insert(std::string{key->getId()});
    }

    return deks;
}

void AuditImpl::prune_deks(const std::vector<std::string>& keys) const {
    auto directory = log_directory.withLock([](auto& dir) { return dir; });
    auto active_key =
            cb::dek::Manager::instance().lookup(cb::dek::Entity::Audit);

    maybeRewriteFiles(
            directory,
            [this, &keys](const auto& path, auto id) {
                const auto filename = path.filename().string();
                if (id.empty() && filename.ends_with("-audit.log")) {
                    return std::ranges::find(keys, "unencrypted") != keys.end();
                }

                if (filename.ends_with("-audit.cef")) {
                    return std::ranges::find(keys, id) != keys.end();
                }

                return false;
            },
            active_key,
            [](auto id) {
                return cb::dek::Manager::instance().lookup(
                        cb::dek::Entity::Audit, id);
            },
            [](std::string_view message, const nlohmann::json& ctx) {
                LOG_WARNING_CTX(message, ctx);
            },
            ".log");
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
    LOG_WARNING_CTX("Audit: Dropping audit event", {"id", event_id});
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
            events_arrived.wait_for(lock, auditfile.get_sleep_time());
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
                // If a previous event disabled auditing, drop any events which
                // aren't changing the audit config again (i.e events others
                // than configureevent events).
                if (enabled || !event->drop_if_audit_disabled()) {
                    if (!event->process(*this)) {
                        dropped_events++;
                    }
                }
            } catch (const std::exception& e) {
                LOG_WARNING_CTX(
                        "AuditImpl::consume_events(): Got exception while "
                        "processing event",
                        {"id", event->id},
                        {"content", cb::tagUserData(event->payload.dump())},
                        {"error", e.what()});
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

bool AuditImpl::write_to_audit_trail(const nlohmann::json& json) {
    if (!auditfile.ensure_open()) {
        LOG_WARNING_CTX("Audit: error opening audit file. Dropping event",
                        {"content", cb::UserDataView(json.dump())});
        return false;
    }

    if (auditfile.write_event_to_disk(json)) {
        return true;
    }

    LOG_WARNING_CTX("Audit: error writing event to disk. Dropping event",
                    {"content", cb::UserDataView(json.dump())});

    // If the write_event_to_disk function returns false then it is
    // possible the audit file has been closed. Therefore, ensure
    // the file is open.
    auditfile.ensure_open();
    return false;
}
