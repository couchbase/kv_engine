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

#include <cJSON.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
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

#include "auditd.h"
#include "audit.h"
#include "event.h"
#include "configureevent.h"
#include "auditd_audit_events.h"
#include "eventdescriptor.h"

std::string Audit::hostname;
void (*Audit::notify_io_complete)(gsl::not_null<const void*> cookie,
                                  ENGINE_ERROR_CODE status);

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
                    cb::logtags::tagUserData(string));
        break;
    case AuditErrorCode::SETTING_AUDITFILE_OPEN_TIME_ERROR:
        LOG_WARNING("Audit: error: setting auditfile open time = {}", string);
        break;
    case AuditErrorCode::WRITING_TO_DISK_ERROR:
        LOG_WARNING("Audit: writing to disk error: {}", string);
        break;
    case AuditErrorCode::WRITE_EVENT_TO_DISK_ERROR:
        LOG_WARNING("Audit: error writing event to disk. Dropping event: {}",
                    cb::logtags::tagUserData(string));
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


std::string Audit::load_file(const char *file) {
    std::ifstream myfile(file, std::ios::in | std::ios::binary);
    if (myfile.is_open()) {
        std::string str((std::istreambuf_iterator<char>(myfile)),
                        std::istreambuf_iterator<char>());
        myfile.close();
        return str;
    } else {
        Audit::log_error(AuditErrorCode::FILE_OPEN_ERROR, file);
        std::string str;
        return str;
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


bool Audit::configure(void) {
    bool is_enabled_before_reconfig = config.is_auditd_enabled();
    std::string configuration = load_file(configfile.c_str());
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
        } catch (std::string &str) {
            // @todo We shouldn't be throwing strings..
            LOG_WARNING("{}", str);
            return false;
        }
    }
    std::stringstream audit_events_file;
    audit_events_file << config.get_descriptors_path();
    audit_events_file << DIRECTORY_SEPARATOR_CHARACTER << "audit_events.json";
    std::string str = load_file(audit_events_file.str().c_str());
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


bool Audit::add_to_filleventqueue(const uint32_t event_id,
                                  const char *payload,
                                  const size_t length) {
    // @todo I think we should do full validation of the content
    //       in debug mode to ensure that developers actually fill
    //       in the correct fields.. if not we should add an
    //       event to the audit trail saying it is one in an illegal
    //       format (or missing fields)
    bool res;
    Event* new_event = new Event(event_id, payload, length);
    cb_mutex_enter(&producer_consumer_lock);
    if (filleventqueue->size() < max_audit_queue) {
        filleventqueue->push(new_event);
        cb_cond_broadcast(&events_arrived);
        res = true;
    } else {
        LOG_WARNING("Audit: Dropping audit event {}: {}",
                    new_event->id,
                    new_event->payload);
        dropped_events++;
        delete new_event;
        res = false;
    }
    cb_mutex_exit(&producer_consumer_lock);
    return res;
}


bool Audit::add_reconfigure_event(const char* configfile, const void *cookie) {
    ConfigureEvent* new_event = new ConfigureEvent(configfile, cookie);
    cb_mutex_enter(&producer_consumer_lock);
    filleventqueue->push(new_event);
    cb_cond_broadcast(&events_arrived);
    cb_mutex_exit(&producer_consumer_lock);
    return true;
}


void Audit::clear_events_map(void) {
    typedef std::map<uint32_t, EventDescriptor*>::iterator it_type;
    for(it_type iterator = events.begin(); iterator != events.end(); iterator++) {
        delete iterator->second;
    }
    events.clear();
}


void Audit::clear_events_queues(void) {
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

bool Audit::terminate_consumer_thread(void)
{
    cb_mutex_enter(&producer_consumer_lock);
    terminate_audit_daemon = true;
    cb_mutex_exit(&producer_consumer_lock);

    /* The consumer thread maybe waiting for an event
     * to arrive so we need to send it a broadcast so
     * it can exit cleanly.
     */
    cb_mutex_enter(&producer_consumer_lock);
    cb_cond_broadcast(&events_arrived);
    cb_mutex_exit(&producer_consumer_lock);
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

bool Audit::clean_up(void) {
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
