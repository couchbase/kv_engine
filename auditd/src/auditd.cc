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

#include "config.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <queue>
#include <string>
#include <iomanip>
#include <map>
#include <new>
#include <limits.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <string.h>
#include <cJSON.h>
#include <platform/platform.h>
#include <platform/dirutils.h>
#include "memcached/audit_interface.h"
#include "auditd.h"
#include "audit.h"
#include "eventdata.h"

Audit audit;

// @toto move the following variables into the Audit class
std::map<uint32_t,EventData*> events;
std::queue<Event> eventqueue1;
std::queue<Event> eventqueue2;
std::queue<Event> *filleventqueue;
std::queue<Event> *processeventqueue;
cb_thread_t consumer_tid;
cb_mutex_t producer_consumer_lock;
cb_cond_t events_arrived;
bool terminate_audit_daemon;
EXTENSION_LOGGER_DESCRIPTOR *logger;
std::ofstream auditfile;
time_t auditfile_open_time;
std::string auditfile_open_time_string;
bool set_auditfile_open_time;
char hostname[128];

static void log_error(const ErrorCode return_code, const char *string) {
    switch (return_code) {
        case AUDIT_EXTENSION_DATA_ERROR:
            assert(string != NULL);
            logger->log(EXTENSION_LOG_WARNING, NULL, "audit extension data error");
            break;
        case FILE_ATTRIBUTES_ERROR:
            assert(string != NULL);
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "attributes error on file %s: %s", string, strerror(errno));
            break;
        case FILE_OPEN_ERROR:
            assert(string != NULL);
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "open error on file %s: %s", string, strerror(errno));
            break;
        case FILE_RENAME_ERROR:
            assert(string != NULL);
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "rename error on file %s: %s", string, strerror(errno));
            break;
        case FILE_REMOVE_ERROR:
            assert(string != NULL);
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "remove error on file %s: %s", string, strerror(errno));
            break;
        case MEMORY_ALLOCATION_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "memory allocation error: %s", string);
            break;
        case JSON_PARSING_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "JSON parsing error on string \"%s\"", string);
            break;
        case JSON_MISSING_DATA_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "JSON missing data error");
            break;
        case JSON_MISSING_OBJECT_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "JSON missing object error");
            break;
        case JSON_KEY_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "JSON key \"%s\" error", string);
            break;
        case JSON_ID_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "JSON eventid error");
            break;
        case JSON_UNKNOWN_FIELD_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "JSON unknown field error");
            break;
        case CB_CREATE_THREAD_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "cb create thread error");
            break;
        case EVENT_PROCESSING_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "event processing error");
            break;
        case PROCESSING_EVENT_FIELDS_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "processing events field error");
            break;
        case TIMESTAMP_FORMAT_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "timestamp format error on string \"%s\"", string);
            break;
        case EVENT_ID_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "eventid error");
            break;
        default:
            assert(false);
    }
}

static void clear_events_map(void) {
    typedef std::map<uint32_t, EventData*>::iterator it_type;
    for(it_type iterator = events.begin(); iterator != events.end(); iterator++) {
        assert(iterator->second != NULL);
        delete iterator->second;
    }
    events.clear();
}


static std::string load_file(const char *file) {
    std::ifstream myfile(file, std::ios::in | std::ios::binary);
    if (myfile.is_open()) {
        std::string str((std::istreambuf_iterator<char>(myfile)),
                        std::istreambuf_iterator<char>());
        myfile.close();
        return str;
    } else {
        log_error(FILE_OPEN_ERROR, file);
        std::string str;
        return str;
    }
}


static int8_t initialize_event_data_structures(cJSON *event_ptr) {
    if (event_ptr == NULL) {
        log_error(JSON_MISSING_DATA_ERROR, NULL);
        return -1;
    }
    uint32_t eventid;
    bool set_eventid = false;
    EventData* eventdata;
    cJSON* values_ptr = event_ptr->child;
    if (values_ptr == NULL) {
        log_error(JSON_MISSING_DATA_ERROR, NULL);
        return -1;
    }
    try {
        eventdata = new EventData;
    } catch (std::bad_alloc& ba) {
        log_error(MEMORY_ALLOCATION_ERROR, ba.what());
        return -1;
    }
    while (values_ptr != NULL) {
        switch (values_ptr->type) {
            case cJSON_Number:
                if (strcmp(values_ptr->string, "id") == 0) {
                    eventid = values_ptr->valueint;
                    assert(eventid != 0);
                    set_eventid = true;
                } else {
                    log_error(JSON_KEY_ERROR,values_ptr->string);
                    return -1;
                }
                break;
            case cJSON_String:
                if (strcmp(values_ptr->string, "name") == 0) {
                    eventdata->name = values_ptr->valuestring;
                } else if (strcmp(values_ptr->string, "description") == 0) {
                    eventdata->description = values_ptr->valuestring;
                } else {
                    log_error(JSON_KEY_ERROR, event_ptr->string);
                    return -1;
                }
                break;
            case cJSON_True:
            case cJSON_False:
                if ((strcmp(values_ptr->string, "sync") != 0) &&
                    (strcmp(values_ptr->string, "enabled") != 0)) {
                    log_error(JSON_KEY_ERROR,values_ptr->string);
                    return -1;
                }
                break;
            case cJSON_Object:
                break;
            default:
                log_error(JSON_UNKNOWN_FIELD_ERROR, NULL);
                return -1;
        }
        values_ptr = values_ptr->next;
    }
    if (set_eventid) {
        eventdata->sync = (std::find(audit.config.sync.begin(),
                                     audit.config.sync.end(), eventid) !=
                           audit.config.sync.end()) ? true : false;
        eventdata->enabled = (std::find(audit.config.enabled.begin(),
                                        audit.config.enabled.end(), eventid)
                              != audit.config.enabled.end()) ? true : false;
        events.insert(std::pair<uint32_t, EventData*>(eventid, eventdata));
    } else {
        log_error(JSON_ID_ERROR, NULL);
        return -1;
    }
    return 0;
}


static int8_t process_module_data_structures(cJSON *module) {
    if (module == NULL) {
        log_error(JSON_MISSING_OBJECT_ERROR, NULL);
        return -1;
    }
    cJSON *mod_ptr = module->child;
    if (mod_ptr == NULL) {
        log_error(JSON_MISSING_DATA_ERROR, NULL);
        return -1;
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
                    if (initialize_event_data_structures(event_ptr) != 0) {
                        return -1;
                    }
                    event_ptr = event_ptr->next;
                }
                break;
            default:
                log_error(JSON_UNKNOWN_FIELD_ERROR, NULL);
                return -1;
        }
        mod_ptr = mod_ptr->next;
    }
    return 0;
}


static int8_t process_module_descriptor(cJSON *module_descriptor) {
    while(module_descriptor != NULL) {
        switch (module_descriptor->type) {
            case cJSON_Number:
                break;
            case cJSON_Array:
                if (process_module_data_structures(module_descriptor->child) != 0) {
                    return -1;
                }
                break;
            default:
                log_error(JSON_UNKNOWN_FIELD_ERROR, NULL);
                return -1;
        }
        module_descriptor = module_descriptor->next;
    }
    return 0;
}

std::string generatetimestamp(void) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::chrono::system_clock::duration seconds_since_epoch =
    std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
    // Construct time_t using 'seconds_since_epoch' rather than 'now' since it is
    // implementation-defined whether the value is rounded or truncated.
    time_t now_t(std::chrono::system_clock::to_time_t(
                      std::chrono::system_clock::time_point(seconds_since_epoch)));
    std::chrono::milliseconds frac_of_second (
                          std::chrono::duration_cast<std::chrono::milliseconds>(
                                    now.time_since_epoch() - seconds_since_epoch));
    struct tm *utc_time = gmtime(&now_t) ;
    struct tm *local_time = localtime(&now_t);
    time_t utc = mktime(utc_time);
    time_t local = mktime(local_time);
    double total_seconds_diff = difftime(local, utc);
    double total_minutes_diff = total_seconds_diff / 60;
    int32_t hours = (int32_t)(total_minutes_diff / 60);
    int32_t minutes = (int32_t)(total_minutes_diff) % 60;

    std::stringstream timestamp;
    timestamp << std::setw(4) << std::setfill('0') <<
    local_time->tm_year + 1900 << "-" <<
    std::setw(2) << std::setfill('0') << local_time->tm_mon+1 << "-" <<
    std::setw(2) << std::setfill('0') << local_time->tm_mday << "T" <<
    std::setw(2) << std::setfill('0') << local_time->tm_hour << ":" <<
    std::setw(2) << std::setfill('0') << local_time->tm_min << ":" <<
    std::setw(2) << std::setfill('0') << local_time->tm_sec << "." <<
    std::setw(3) << std::setfill('0') << std::setprecision(3) <<
    frac_of_second.count();

    if (total_seconds_diff == 0.0) {
        timestamp << "Z";
    } else if (total_seconds_diff < 0.0) {
        timestamp << "-" <<
        std::setw(2) << std::setfill('0') << abs(hours) <<
        std::setw(2) << std::setfill('0') << abs(minutes);
    } else {
        timestamp << "+" <<
        std::setw(2) << std::setfill('0') << hours <<
        std::setw(2) << std::setfill('0') << minutes;
    }
    return timestamp.str();
}


static bool file_exists(const std::string& name) {
#ifdef WIN32
    DWORD dwAttrib = GetFileAttributes(name.c_str());
    if (dwAttrib == INVALID_FILE_ATTRIBUTES) {
        return false;
    }
    return (!(dwAttrib & FILE_ATTRIBUTE_DIRECTORY));
#else
    struct stat buffer;
    if (stat (name.c_str(), &buffer) != 0) {
        return false;
    }
    return (!S_ISDIR(buffer.st_mode));
#endif
}


static int64_t file_size(const std::string& name) {
#ifdef WIN32
    WIN32_FILE_ATTRIBUTE_DATA fad;
    if (!GetFileAttributesEx(name.c_str(), GetFileExInfoStandard,
                             &fad) == 0) {
        log_error(FILE_ATTRIBUTES_ERROR, name.c_str());
        return -1;
    }
    LARGE_INTEGER size;
    size.HighPart = fad.nFileSizeHigh;
    size.LowPart = fad.nFileSizeLow;
    return size.QuadPart;
#else
    struct stat buffer;
    if (stat (name.c_str(), &buffer) != 0) {
        log_error(FILE_ATTRIBUTES_ERROR, name.c_str());
        return -1;
    }
    return (int64_t)buffer.st_size;
#endif
}


static int8_t open_auditfile(void) {
    assert(!auditfile.is_open());
    std::stringstream file;
    file << audit.config.log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    auditfile.open(file.str().c_str(), std::ios::out | std::ios::binary);
    if (!auditfile.is_open()) {
        log_error(FILE_OPEN_ERROR, file.str().c_str());
        return -1;
    }
    return 0;
}


static int8_t process_fields_recursive(cJSON *json_payload,
                                       std::stringstream& output) {
    std::map<std::string, cJSON *> fields_map;
    cJSON *fields = json_payload->child;
    while (fields != NULL) {
        std::string name = fields->string;
        fields_map[name] = fields;
        fields = fields->next;
    }
    // iterate through the map
    for (std::map<std::string, cJSON *>::iterator
         it=fields_map.begin(); it!=fields_map.end(); ++it) {
        if (it != fields_map.begin()) {
            output << ", ";
        }
        switch (it->second->type) {
            case cJSON_String:
                output << "\"" << it->first << "\" : \"" << it->second->valuestring;
                output << "\"";
                break;
            case cJSON_Number:
                output << "\"" << it->first << "\" : " << std::dec << it->second->valueint;
                break;
            case cJSON_Object:
                output << "\"" << it->first << "\" : {";
                if (process_fields_recursive(it->second, output) != 0) {
                    log_error(PROCESSING_EVENT_FIELDS_ERROR, NULL);
                    return -1;
                }
                output << "}";
                break;
            case cJSON_True:
            case cJSON_False:
                output << "\"" << it->first << "\" : \"";
                output << ((it->second->type == cJSON_True) ? "true" : "false");
                output << "\"";
                break;
            default:
                log_error(JSON_UNKNOWN_FIELD_ERROR, NULL);
                return -1;
        }
    }
    return 0;
}


static bool is_timestamp_format_correct (std::string& str) {
    if (isdigit(str.c_str()[0]) && isdigit(str.c_str()[1]) &&
        isdigit(str.c_str()[2]) && isdigit(str.c_str()[3]) &&
        str.c_str()[4] == '-' &&
        isdigit(str.c_str()[5]) && isdigit(str.c_str()[6]) &&
        str.c_str()[7] == '-' &&
        isdigit(str.c_str()[8]) && isdigit(str.c_str()[9]) &&
        str.c_str()[10] == 'T' &&
        isdigit(str.c_str()[11]) && isdigit(str.c_str()[12]) &&
        str.c_str()[13] == ':' &&
        isdigit(str.c_str()[14]) && isdigit(str.c_str()[15]) &&
        str.c_str()[16] == ':' &&
        isdigit(str.c_str()[17]) && isdigit(str.c_str()[18])) {
            return true;
        }
    return false;
}

static int8_t process_event(Event& event) {
    // convert the event.payload into JSON
    cJSON *json_payload = cJSON_Parse(event.payload.c_str());
    if (json_payload == NULL) {
        log_error(JSON_PARSING_ERROR, event.payload.c_str());
        return -1;
    }
    std::map<std::string, cJSON *> fields_map;
    cJSON *fields = json_payload->child;
    while (fields != NULL) {
        std::string name = fields->string;
        fields_map[name] = fields;
        fields = fields->next;
    }
    if (!set_auditfile_open_time) {
        auditfile_open_time_string = fields_map["timestamp"]->valuestring;
        assert(!auditfile_open_time_string.empty());
        if (!is_timestamp_format_correct(auditfile_open_time_string)) {
            log_error(TIMESTAMP_FORMAT_ERROR, auditfile_open_time_string.c_str());
        }
        std::string year = auditfile_open_time_string.substr(0,4);
        std::string month = auditfile_open_time_string.substr(5,2);
        std::string day = auditfile_open_time_string.substr(8,2);
        std::string hour = auditfile_open_time_string.substr(11,2);
        std::string min = auditfile_open_time_string.substr(14,2);
        std::string sec = auditfile_open_time_string.substr(17,2);

        struct tm time_str;
        time_str.tm_year = atoi(year.c_str()) - 1900;
        time_str.tm_mon = atoi(month.c_str()) - 1;
        time_str.tm_mday = atoi(day.c_str());
        time_str.tm_hour = atoi(hour.c_str());
        time_str.tm_min = atoi(min.c_str());
        time_str.tm_sec = atoi(sec.c_str());
        auditfile_open_time = mktime(&time_str);
        set_auditfile_open_time = true;
    }

    // write the event out to the audit log
    std::stringstream output;

    output << "{ \"ts\" : \"" << fields_map["timestamp"]->valuestring << "\", ";
    output << "\"id\" : " << event.id << ", ";

    // write out the name & description
    output << "\"name\" : \"" << events[event.id]->name << "\", ";
    output << "\"desc\" : \"" << events[event.id]->description << "\"";

    // now iterate through the fields map
    for (std::map<std::string, cJSON *>::iterator
         it=fields_map.begin(); it!=fields_map.end(); ++it) {
        switch (it->second->type) {
            case cJSON_String:
                if (it->first.compare("timestamp") != 0) {
                    output << ", \"" << it->first << "\" : \"" << it->second->valuestring;
                    output << "\"";
                }
                break;
            case cJSON_Number:
                output << ", \"" << it->first << "\" : " << std::dec << it->second->valueint;
                break;
            case cJSON_Object:
                output << ", \"" << it->first << "\" : {";
                if (process_fields_recursive(it->second, output) != 0) {
                    log_error(PROCESSING_EVENT_FIELDS_ERROR, NULL);
                    return -1;
                }
                output << "}";
                break;
            case cJSON_True:
            case cJSON_False:
                output << ", \"" << it->first << "\" : \"";
                output << ((it->second->type == cJSON_True) ? "true" : "false");
                output << "\"";
                break;
            default:
                log_error(JSON_UNKNOWN_FIELD_ERROR, NULL);
                return -1;
        }
    }
    output << " }" << std::endl;
    auditfile << output.rdbuf();
    auditfile.flush();
    return 0;
}


static void close_and_rotate_log(void) {
    assert(auditfile.is_open());
    auditfile.close();
    //cp the file to archive path and rename using auditfile_open_time_string
    std::stringstream audit_file;
    std::stringstream archive_file;
    audit_file << audit.config.log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";

    // form the archive filename
    std::string archive_filename = hostname;
    assert(!auditfile_open_time_string.empty());
    if (!is_timestamp_format_correct(auditfile_open_time_string)) {
        log_error(TIMESTAMP_FORMAT_ERROR, auditfile_open_time_string.c_str());
    }
    std::string ts = auditfile_open_time_string.substr(0,19);
    std::replace(ts.begin(), ts.end(), ':', '-');
    archive_filename += "-" + ts + "-audit.log";
    // move the audit_log to the archive.
    archive_file << audit.config.archive_path << DIRECTORY_SEPARATOR_CHARACTER << archive_filename;

    // check if archive file already exists if so delete
    if (file_exists(archive_file.str().c_str())) {
        if (remove(archive_file.str().c_str()) != 0) {
            log_error(FILE_REMOVE_ERROR, archive_file.str().c_str());
        }
    }
    if (rename (audit_file.str().c_str(), archive_file.str().c_str()) != 0) {
        log_error(FILE_RENAME_ERROR, audit_file.str().c_str());
    }
    set_auditfile_open_time = false;
}


static bool time_to_rotate_log(void) {
    if (set_auditfile_open_time) {
        time_t now;
        time(&now);
        if (difftime(now, auditfile_open_time) > audit.config.rotate_interval) {
            return true;
        }
    }
    return false;
}


static void consume_events(void *arg) {
    cb_mutex_enter(&producer_consumer_lock);
    while (!terminate_audit_daemon) {
        bool drop_events = false;
        assert(filleventqueue != NULL);
        if (filleventqueue->empty()) {
            cb_cond_wait(&events_arrived, &producer_consumer_lock);
        }
        /* now have producer_consumer lock!
         * event(s) have arrived or shutdown requested
         */
        swap(processeventqueue, filleventqueue);
        cb_mutex_exit(&producer_consumer_lock);
        if (auditfile.is_open() &&
            (time_to_rotate_log() || !audit.config.cbauditd_enabled)) {
            close_and_rotate_log();
        }
        assert(processeventqueue != NULL);
        if (!processeventqueue->empty() && !auditfile.is_open()) {
            if (open_auditfile() != 0) {
                drop_events = true;
            }
        }
        while (!processeventqueue->empty()) {
            if (!drop_events) {
                /* process the event, i.e. write to disk */
                if (process_event(processeventqueue->front()) != 0) {
                    log_error(EVENT_PROCESSING_ERROR, NULL);
                }
            }
            processeventqueue->pop();
        }
        cb_mutex_enter(&producer_consumer_lock);
    }
    cb_mutex_exit(&producer_consumer_lock);
    assert(terminate_audit_daemon);
    close_and_rotate_log();
}


static int8_t create_consumer_thread(void) {
    cb_cond_initialize(&events_arrived);
    cb_mutex_initialize(&producer_consumer_lock);
    if (cb_create_thread(&consumer_tid, consume_events, NULL, 0) != 0) {
        log_error(CB_CREATE_THREAD_ERROR, NULL);
        cb_cond_destroy(&events_arrived);
        cb_mutex_destroy(&producer_consumer_lock);
        return -1;
    }
    return 0;
}


static int8_t create_audit_event(uint32_t event_id, cJSON *payload) {
    switch (event_id) {
        case 0x1000:
        case 0x1001: {
            cJSON_AddStringToObject(payload, "timestamp", generatetimestamp().c_str());
            cJSON *real_userid = cJSON_CreateObject();
            cJSON_AddStringToObject(real_userid, "source", "internal");
            cJSON_AddStringToObject(real_userid, "user", "_admin");
            cJSON_AddItemReferenceToObject(payload, "real_userid", real_userid);
            cJSON_AddStringToObject(payload, "hostname", hostname);
            cJSON_AddNumberToObject(payload, "version", 1.0);
            cJSON_AddStringToObject(payload, "log_path", audit.config.log_path.c_str());
            cJSON_AddStringToObject(payload, "archive_path", audit.config.archive_path.c_str());
            }
            break;
        default:
            log_error(EVENT_ID_ERROR, NULL);
            return -1;
    }
    return 0;
}


static bool add_to_filleventqueue(uint32_t event_id,
                                  const char *payload,
                                  size_t length) {
    // @todo we might need to protect the access to the map if we'd
    //       like to be able to modifications to the map itself.
    auto evt = events.find(event_id);
    if (evt == events.end()) {
        // This is an unknown id. drop it!
        // We should change this to return false at some point because
        // people should know that they're sending an unknown identifier
        return true;
    }

    if (evt->second->enabled) {
        // @todo I think we should do full validation of the content
        //       in debug mode to ensure that developers actually fill
        //       in the correct fields.. if not we should add an
        //       event to the audit trail saying it is one in an illegal
        //       format (or missing fields)
        Event new_event = Event();
        new_event.id = event_id;
        new_event.payload.assign(payload, length);
        assert(filleventqueue != NULL);
        filleventqueue->push(new_event);
        cb_mutex_enter(&producer_consumer_lock);
        cb_cond_broadcast(&events_arrived);
        cb_mutex_exit(&producer_consumer_lock);
    }

    return true;
}


static int8_t cleanup_old_logfile() {
    std::stringstream file;
    file << audit.config.log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    if (file_exists(file.str())) {
        if (file_size(file.str()) == 0) {
            // the file is empty so just remove
            if (remove(file.str().c_str()) != 0 ) {
                log_error(FILE_REMOVE_ERROR, file.str().c_str());
                return -1;
            }
            return 0;
        } else {
            // open the audit.log that needs archiving
            std::string str = load_file(file.str().c_str());
            if (str.empty()) {
                return -1;
            }
            // extract the first event
            std::size_t found = str.find_first_of("\n");
            str.erase(found+1, std::string::npos);
            cJSON *json_ptr = cJSON_Parse(str.c_str());
            if (json_ptr == NULL) {
                log_error(JSON_PARSING_ERROR, str.c_str());
                return -1;
            }
            // extract the timestamp
            std::string ts;
            cJSON *fields = json_ptr->child;
            while (fields != NULL) {
                std::string name = fields->string;
                if (name.compare("ts") == 0) {
                    ts = fields->valuestring;
                    break;
                }
                fields = fields->next;
            }
            assert(!ts.empty());
            if (!is_timestamp_format_correct(ts)) {
                log_error(TIMESTAMP_FORMAT_ERROR, ts.c_str());
                return -1;
            }
            ts = ts.substr(0,19);
            std::replace(ts.begin(), ts.end(), ':', '-');
            // form the archive filename
            std::string archive_filename = hostname;
            archive_filename += "-" + ts + "-audit.log";
            // move the audit_log to the archive.
            std::stringstream archive_file;
            archive_file << audit.config.archive_path << DIRECTORY_SEPARATOR_CHARACTER
            << archive_filename;
            if (rename (file.str().c_str(), archive_file.str().c_str()) != 0) {
                log_error(FILE_RENAME_ERROR, file.str().c_str());
                return -1;
            }
        }
    }
    return 0;
}


AUDIT_ERROR_CODE initialize_auditdaemon(const char *config,
                                        const AUDIT_EXTENSION_DATA *extension_data) {
    if (extension_data->version != 1) {
        log_error(AUDIT_EXTENSION_DATA_ERROR, NULL);
        return AUDIT_FAILED;
    }
    gethostname(hostname, sizeof(hostname));
    logger=extension_data->log_extension;
    AuditConfig::min_file_rotation_time = extension_data->min_file_rotation_time;
    AuditConfig::max_file_rotation_time = extension_data->max_file_rotation_time;
    AuditConfig::logger = logger;

    std::string configuration = load_file(config);
    if (configuration.empty()) {
        return AUDIT_FAILED;
    }
    if (!audit.config.initialize_config(configuration)) {
        return AUDIT_FAILED;
    }

    cleanup_old_logfile();

    std::stringstream audit_events_file;
    audit_events_file << CouchbaseDirectoryUtilities::dirname(std::string(config))
                      << DIRECTORY_SEPARATOR_CHARACTER << "audit_events.json";
    std::string str = load_file(audit_events_file.str().c_str());
    if (str.empty()) {
        return AUDIT_FAILED;
    }
    cJSON *json_ptr = cJSON_Parse(str.c_str());
    if (json_ptr == NULL) {
        log_error(JSON_PARSING_ERROR, str.c_str());
        return AUDIT_FAILED;
    }
    if (process_module_descriptor(json_ptr->child) != 0) {
        clear_events_map();
        assert(json_ptr != NULL);
        cJSON_Delete(json_ptr);
        return AUDIT_FAILED;
    }

    // set variables before spawning consumer thread
    set_auditfile_open_time = false;
    processeventqueue = &eventqueue1;
    filleventqueue = &eventqueue2;
    terminate_audit_daemon = (audit.config.cbauditd_enabled)? false : true;

    if (create_consumer_thread() != 0) {
        clear_events_map();
        assert(json_ptr != NULL);
        cJSON_Delete(json_ptr);
        return AUDIT_FAILED;
    }

    if (audit.config.cbauditd_enabled) {
        // create an event stating that the audit daemon has been started
        cJSON *payload = cJSON_CreateObject();
        if (create_audit_event(0x1000, payload) != 0) {
            return AUDIT_FAILED;
        }
        char *content = cJSON_Print(payload);
        if (!add_to_filleventqueue(0x1000, content, strlen(content))) {
            return AUDIT_FAILED;
        }
        free(content);
        assert(payload != NULL);
        cJSON_Delete(payload);
     }
    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE put_audit_event(const uint32_t audit_eventid,
                                 const void *payload, size_t length) {
    if (audit.config.cbauditd_enabled) {
        if (!add_to_filleventqueue(audit_eventid, (char *)payload, length)) {
            return AUDIT_FAILED;
        }
    }
    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE reload_auditdaemon_config(const char *config) {
    bool cbauditd_enabled_before_reload = audit.config.cbauditd_enabled;
    std::string configuration = load_file(config);
    if (configuration.empty()) {
        return AUDIT_FAILED;
    }
    if (!audit.config.initialize_config(configuration)) {
         return AUDIT_FAILED;
    }
    if (cbauditd_enabled_before_reload && !audit.config.cbauditd_enabled) {
        /* the new configuration is disabling the audit daemon.
         * consumer thread(s) maybe waiting for an event to arrive so need
         * to send it a broadcast to close the audit_log file.
         */
        cb_mutex_enter(&producer_consumer_lock);
        cb_cond_broadcast(&events_arrived);
        cb_mutex_exit(&producer_consumer_lock);
    }
    return AUDIT_SUCCESS;
}


AUDIT_ERROR_CODE shutdown_auditdaemon(void) {
    cJSON *payload = cJSON_CreateObject();
    if (create_audit_event(0x1001, payload) != 0) {
        return AUDIT_FAILED;
    }
    char *content = cJSON_Print(payload);
    if (!add_to_filleventqueue(0x1001, content, strlen(content))) {
        return AUDIT_FAILED;
    }
    free(content);
    assert(payload != NULL);
    cJSON_Delete(payload);

    terminate_audit_daemon = true;
    /* consumer thread(s) maybe waiting for an event to arrive so need
     * to send it a broadcast so they can exit cleanly
     */
    cb_mutex_enter(&producer_consumer_lock);
    cb_cond_broadcast(&events_arrived);
    cb_mutex_exit(&producer_consumer_lock);
    if (cb_join_thread(consumer_tid) != 0) {
        cb_cond_destroy(&events_arrived);
        cb_mutex_destroy(&producer_consumer_lock);
        clear_events_map();
        return AUDIT_FAILED;
    }
    cb_cond_destroy(&events_arrived);
    cb_mutex_destroy(&producer_consumer_lock);
    clear_events_map();
    return AUDIT_SUCCESS;
}
