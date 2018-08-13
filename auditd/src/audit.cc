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
#include "config.h"

#include "audit.h"
#include "auditd.h"
#include "auditd_audit_events.h"
#include "configureevent.h"
#include "event.h"
#include "eventdescriptor.h"

#include <cJSON.h>
#include <logger/logger.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <utilities/logtags.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <queue>
#include <sstream>
#include <string>

std::string Audit::hostname;

Audit::Audit(std::string config_file,
             SERVER_COOKIE_API* sapi,
             const std::string& host)
    : configfile(std::move(config_file)), cookie_api(sapi) {
    hostname.assign(host);
}

Audit::~Audit() {
    clean_up();
}

void Audit::log_error(const AuditErrorCode return_code,
                      const std::string& string) {
    switch (return_code) {
    case AuditErrorCode::AUDIT_EXTENSION_DATA_ERROR:
        LOG_WARNING("Audit: audit extension data error");
        break;
    case AuditErrorCode::FILE_OPEN_ERROR:
        LOG_WARNING(
                "Audit: open error on file {}: {}", string, strerror(errno));
        break;
    case AuditErrorCode::FILE_RENAME_ERROR:
        LOG_WARNING(
                "Audit: rename error on file {}: {}", string, strerror(errno));
        break;
    case AuditErrorCode::FILE_REMOVE_ERROR:
        LOG_WARNING(
                "Audit: remove error on file {}: {}", string, strerror(errno));
        break;
    case AuditErrorCode::MEMORY_ALLOCATION_ERROR:
        LOG_WARNING("Audit: memory allocation error: {}", string);
        break;
    case AuditErrorCode::JSON_PARSING_ERROR:
        LOG_WARNING(R"(Audit: JSON parsing error on string "{}")", string);
        break;
    case AuditErrorCode::JSON_MISSING_DATA_ERROR:
        LOG_WARNING("Audit: JSON missing data error");
        break;
    case AuditErrorCode::JSON_MISSING_OBJECT_ERROR:
        LOG_WARNING("Audit: JSON missing object error");
        break;
    case AuditErrorCode::JSON_KEY_ERROR:
        LOG_WARNING(R"(Audit: JSON key "{}" error)", string);
        break;
    case AuditErrorCode::JSON_ID_ERROR:
        LOG_WARNING("Audit: JSON eventid error");
        break;
    case AuditErrorCode::JSON_UNKNOWN_FIELD_ERROR:
        LOG_WARNING("Audit: JSON unknown field error");
        break;
    case AuditErrorCode::CB_CREATE_THREAD_ERROR:
        LOG_WARNING("Audit: cb create thread error");
        break;
    case AuditErrorCode::EVENT_PROCESSING_ERROR:
        LOG_WARNING("Audit: event processing error");
        break;
    case AuditErrorCode::PROCESSING_EVENT_FIELDS_ERROR:
        LOG_WARNING("Audit: processing events field error");
        break;
    case AuditErrorCode::TIMESTAMP_MISSING_ERROR:
        LOG_WARNING("Audit: timestamp missing error");
        break;
    case AuditErrorCode::TIMESTAMP_FORMAT_ERROR:
        LOG_WARNING(
                R"(Audit: timestamp format error on string "{}")", string);
        break;
    case AuditErrorCode::EVENT_ID_ERROR:
        LOG_WARNING("Audit: eventid error");
        break;
    case AuditErrorCode::VERSION_ERROR:
        LOG_WARNING("Audit: audit version error");
        break;
    case AuditErrorCode::VALIDATE_PATH_ERROR:
        LOG_WARNING(R"(Audit: validate path "{}" error)", string);
        break;
    case AuditErrorCode::ROTATE_INTERVAL_BELOW_MIN_ERROR:
        LOG_WARNING("Audit: rotate_interval below minimum error");
        break;
    case AuditErrorCode::ROTATE_INTERVAL_EXCEEDS_MAX_ERROR:
        LOG_WARNING("Audit: rotate_interval exceeds maximum error");
        break;
    case AuditErrorCode::OPEN_AUDITFILE_ERROR:
        LOG_WARNING("Audit: error opening audit file. Dropping event: {}",
                    cb::UserDataView(string));
        break;
    case AuditErrorCode::SETTING_AUDITFILE_OPEN_TIME_ERROR:
        LOG_WARNING("Audit: error: setting auditfile open time = {}", string);
        break;
    case AuditErrorCode::WRITING_TO_DISK_ERROR:
        LOG_WARNING("Audit: writing to disk error: {}", string);
        break;
    case AuditErrorCode::WRITE_EVENT_TO_DISK_ERROR:
        LOG_WARNING("Audit: error writing event to disk. Dropping event: {}",
                    cb::UserDataView(string));
        break;
    case AuditErrorCode::UNKNOWN_EVENT_ERROR:
        LOG_WARNING("Audit: error: unknown event {}", string);
        break;
    case AuditErrorCode::CONFIG_INPUT_ERROR:
        if (!string.empty()) {
            LOG_WARNING("Audit: error reading config: {}", string);
        } else {
            LOG_WARNING("Audit: error reading config");
        }
        break;
    case AuditErrorCode::CONFIGURATION_ERROR:
        LOG_WARNING("Audit: error performing configuration");
        break;
    case AuditErrorCode::MISSING_AUDIT_EVENTS_FILE_ERROR:
        LOG_WARNING(
                R"(Audit: error: missing audit_event.json from "{}")", string);
        break;
    case AuditErrorCode::ROTATE_INTERVAL_SIZE_TOO_BIG:
        LOG_WARNING("Audit: error: rotation_size too big: {}", string);
        break;
    case AuditErrorCode::AUDIT_DIRECTORY_DONT_EXIST:
        LOG_WARNING("Audit: error: %s does not exists", string);
        break;
    case AuditErrorCode::INITIALIZATION_ERROR:
        LOG_WARNING("Audit: error during initialization: {}", string);
        break;
    default:
        LOG_WARNING("Audit: unknown error code:{} with string:{}",
                    int(return_code),
                    string);
        break;
    }
}

bool Audit::create_audit_event(uint32_t event_id, nlohmann::json& payload) {
    // Add common fields to the audit event
    payload["timestamp"] = ISOTime::generatetimestamp();
    nlohmann::json real_userid;
    real_userid["domain"] = "internal";
    real_userid["user"] = "couchbase";
    payload["real_userid"] = real_userid;

    switch (event_id) {
        case AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON:
            payload["auditd_enabled"] = config.is_auditd_enabled();
            payload["descriptors_path"] = config.get_descriptors_path();
            payload["hostname"] = hostname;
            payload["log_path"] = config.get_log_directory();
            payload["rotate_interval"] = config.get_rotate_interval();
            payload["version"] = config.get_version();
            payload["uuid"] = config.get_uuid();
            break;

        case AUDITD_AUDIT_SHUTTING_DOWN_AUDIT_DAEMON:
            break;

        default:
            log_error(AuditErrorCode::EVENT_ID_ERROR);
            return false;
    }
    return true;
}


bool Audit::initialize_event_data_structures(cJSON *event_ptr) {
    if (event_ptr == nullptr) {
        log_error(AuditErrorCode::JSON_MISSING_DATA_ERROR);
        return false;
    }

    try {
        auto *entry = new EventDescriptor(event_ptr);
        events.insert(std::pair<uint32_t, EventDescriptor*>(entry->getId(), entry));
    } catch (std::bad_alloc& ba) {
        log_error(AuditErrorCode::MEMORY_ALLOCATION_ERROR, ba.what());
        return false;
    } catch (std::logic_error& le) {
        log_error(AuditErrorCode::JSON_KEY_ERROR, le.what());
    }

    return true;
}


bool Audit::process_module_data_structures(cJSON *module) {
    if (module == NULL) {
        log_error(AuditErrorCode::JSON_MISSING_OBJECT_ERROR);
        return false;
    }
    while (module != NULL) {
        cJSON *mod_ptr = module->child;
        if (mod_ptr == NULL) {
            log_error(AuditErrorCode::JSON_MISSING_DATA_ERROR);
            return false;
        }
        while (mod_ptr != NULL) {
            cJSON *event_ptr;
            switch (mod_ptr->type) {
                case cJSON_Number:
                case cJSON_String:
                    break;
                case cJSON_Array:
                    event_ptr = mod_ptr->child;
                    while (event_ptr != NULL) {
                        if (!initialize_event_data_structures(event_ptr)) {
                            return false;
                        }
                        event_ptr = event_ptr->next;
                    }
                    break;
                default:
                    log_error(AuditErrorCode::JSON_UNKNOWN_FIELD_ERROR);
                    return false;
            }
            mod_ptr = mod_ptr->next;
        }
        module = module->next;
    }
    return true;
}


bool Audit::process_module_descriptor(cJSON *module_descriptor) {
    clear_events_map();
    while(module_descriptor != NULL) {
        switch (module_descriptor->type) {
            case cJSON_Number:
                break;
            case cJSON_Array:
                if (!process_module_data_structures(module_descriptor->child)) {
                    return false;
                }
                break;
            default:
                log_error(AuditErrorCode::JSON_UNKNOWN_FIELD_ERROR);
                return false;
        }
        module_descriptor = module_descriptor->next;
    }
    return true;
}

bool Audit::configure() {
    bool is_enabled_before_reconfig = config.is_auditd_enabled();
    const auto configuration = cb::io::loadFile(configfile);
    if (configuration.empty()) {
        return false;
    }

    nlohmann::json config_json;
    try {
        config_json = nlohmann::json::parse(configuration);
    } catch (const nlohmann::json::exception&) {
        log_error(AuditErrorCode::JSON_PARSING_ERROR, configuration);
        return false;
    }

    bool failure = false;
    try {
        config.initialize_config(config_json);
    } catch (std::pair<AuditErrorCode, char*>& exc) {
        log_error(exc.first, exc.second);
        failure = true;
    } catch (std::pair<AuditErrorCode, const char *>& exc) {
        log_error(exc.first, exc.second);
        failure = true;
    } catch (std::string &msg) {
        log_error(AuditErrorCode::CONFIG_INPUT_ERROR, msg);
        failure = true;
    } catch (...) {
        log_error(AuditErrorCode::CONFIG_INPUT_ERROR);
        failure = true;
    }
    if (failure) {
        return false;
    }

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
    auto audit_events_file =
            config.get_descriptors_path() + "/audit_events.json";
    cb::io::sanitizePath(audit_events_file);
    const auto str = cb::io::loadFile(audit_events_file);
    if (str.empty()) {
        return false;
    }
    cJSON *json_ptr = cJSON_Parse(str.c_str());
    if (json_ptr == NULL) {
        Audit::log_error(AuditErrorCode::JSON_PARSING_ERROR, str);
        return false;
    }
    if (!process_module_descriptor(json_ptr->child)) {
        cJSON_Delete(json_ptr);
        return false;
    }
    cJSON_Delete(json_ptr);

    auditfile.reconfigure(config);

    // iterate through the events map and update the sync and enabled flags
    for (const auto& event : events) {
        event.second->setSync(config.is_event_sync(event.first));
        // If the event has a state defined then use that
        AuditConfig::EventState state = config.get_event_state(event.first);
        switch (state) {
        case AuditConfig::EventState::enabled:
            event.second->setEnabled(true);
            break;
        case AuditConfig::EventState::disabled:
            event.second->setEnabled(false);
            break;
        case AuditConfig::EventState::undefined:
            // No state defined for the event so don't do anything
            break;
        }
    }

    /*
     * We need to notify if the audit daemon is turned on or off during a
     * reconfigure.  It is also possible that particular audit events may
     * have been enabled or disabled.  Therefore we want to notify all of the
     * current event states whenever we do a reconfigure.
     */
    notify_all_event_states();

    // create event to say done reconfiguration
    if (is_enabled_before_reconfig || config.is_auditd_enabled()) {
        auto evt = events.find(AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON);
        if (evt == events.end()) {
            std::ostringstream convert;
            convert << AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON;
            Audit::log_error(AuditErrorCode::UNKNOWN_EVENT_ERROR,
                             convert.str().c_str());
        } else {
            if (evt->second->isEnabled()) {
                nlohmann::json payload;
                if (create_audit_event(AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON,
                                       payload)) {
                    payload["id"] = AUDITD_AUDIT_CONFIGURED_AUDIT_DAEMON;
                    payload["name"] = evt->second->getName();
                    payload["description"] = evt->second->getDescription();

                    if (!(auditfile.ensure_open() && auditfile.write_event_to_disk(payload))) {
                        dropped_events++;
                    }
                } else {
                    dropped_events++;
                }
            }
        }
    }

    if (config.is_auditd_enabled()) {
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

bool Audit::add_to_filleventqueue(uint32_t event_id,
                                  cb::const_char_buffer payload) {
    // @todo I think we should do full validation of the content
    //       in debug mode to ensure that developers actually fill
    //       in the correct fields.. if not we should add an
    //       event to the audit trail saying it is one in an illegal
    //       format (or missing fields)
    try {
        auto* new_event = new Event(event_id, payload);
        std::lock_guard<std::mutex> guard(producer_consumer_lock);
        if (filleventqueue->size() < max_audit_queue) {
            filleventqueue->push(new_event);
            events_arrived.notify_all();
            return true;
        }
        delete new_event;
    } catch (const std::bad_alloc&) {
    }

    dropped_events++;
    LOG_WARNING("Audit: Dropping audit event {}: {}",
                event_id,
                cb::UserDataView(payload));
    return false;
}

bool Audit::add_reconfigure_event(const char* configfile, const void *cookie) {
    ConfigureEvent* new_event = new ConfigureEvent(configfile, cookie);
    std::lock_guard<std::mutex> guard(producer_consumer_lock);
    filleventqueue->push(new_event);
    events_arrived.notify_all();
    return true;
}

void Audit::clear_events_map() {
    typedef std::map<uint32_t, EventDescriptor*>::iterator it_type;
    for(it_type iterator = events.begin(); iterator != events.end(); iterator++) {
        delete iterator->second;
    }
    events.clear();
}

void Audit::clear_events_queues() {
    while(!processeventqueue->empty()) {
        Event *event = processeventqueue->front();
        processeventqueue->pop();
        delete event;
    }
    while(!filleventqueue->empty()) {
        Event *event = filleventqueue->front();
        filleventqueue->pop();
        delete event;
    }
}

bool Audit::terminate_consumer_thread() {
    {
        std::lock_guard<std::mutex> guard(producer_consumer_lock);
        terminate_audit_daemon = true;
    }

    /* The consumer thread maybe waiting for an event
     * to arrive so we need to send it a broadcast so
     * it can exit cleanly.
     */
    {
        std::lock_guard<std::mutex> guard(producer_consumer_lock);
        events_arrived.notify_all();
    }
    if (consumer_thread_running.load()) {
        if (cb_join_thread(consumer_tid) == 0) {
            consumer_thread_running.store(false);
            return true;
        }
        return false;
    } else {
        return false;
    }
}

bool Audit::clean_up() {
    /* clean_up is called from shutdown_auditdaemon
     * which in turn is called from memcached when
     * performing a graceful shutdown.
     *
     * However clean_up is also called from ~Audit().
     * This is required because it possible for the
     * destructor to be invoked without going through
     * the memcached graceful shutdown.
     *
     * We therefore first check to see if the
     * terminate_audit_daemon flag has not been set
     * before trying to terminate the consumer thread.
     */
    if (!terminate_audit_daemon) {
        if (terminate_consumer_thread()) {
            consumer_tid = cb_thread_self();
        } else {
            return false;
        }
        clear_events_map();
        clear_events_queues();
    }
    return true;
}

void Audit::notify_all_event_states() {
    notify_event_state_changed(0, config.is_auditd_enabled());
    for (const auto& event : events) {
        notify_event_state_changed(event.first, event.second->isEnabled());
    }
}

void Audit::add_event_state_listener(cb::audit::EventStateListener listener) {
    std::lock_guard<std::mutex> guard(event_state_listener.mutex);
    event_state_listener.clients.push_back(listener);
}

void Audit::notify_event_state_changed(uint32_t id, bool enabled) const {
    std::lock_guard<std::mutex> guard(event_state_listener.mutex);
    for (const auto& func : event_state_listener.clients) {
        func(id, enabled);
    }
}

void Audit::notify_io_complete(gsl::not_null<const void*> cookie,
                               ENGINE_ERROR_CODE status) {
    cookie_api->notify_io_complete(cookie, status);
}

void Audit::stats(ADD_STAT add_stats, gsl::not_null<const void*> cookie) {
    const auto* enabled = config.is_auditd_enabled() ? "true" : "false";
    add_stats("enabled",
              (uint16_t)strlen("enabled"),
              enabled,
              (uint32_t)strlen(enabled),
              cookie.get());
    const auto num_of_dropped_events = std::to_string(dropped_events);
    add_stats("dropped_events",
              (uint16_t)strlen("dropped_events"),
              num_of_dropped_events.data(),
              (uint32_t)num_of_dropped_events.length(),
              cookie.get());
}

void Audit::consume_events() {
    std::unique_lock<std::mutex> lock(producer_consumer_lock);
    while (!terminate_audit_daemon) {
        if (filleventqueue->empty()) {
            events_arrived.wait_for(
                    lock,
                    std::chrono::seconds(auditfile.get_seconds_to_rotation()));
            if (filleventqueue->empty()) {
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
        swap(processeventqueue, filleventqueue);
        lock.unlock();
        // Now outside of the producer_consumer_lock

        while (!processeventqueue->empty()) {
            Event* event = processeventqueue->front();
            if (!event->process(*this)) {
                dropped_events++;
            }
            processeventqueue->pop();
            delete event;
        }
        auditfile.flush();
        lock.lock();
    }

    // close the auditfile
    auditfile.close();
}
