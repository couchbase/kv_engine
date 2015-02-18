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

#include <cstring>
#include <sstream>
#include <cJSON.h>
#include <platform/dirutils.h>
#include "auditd.h"
#include "audit.h"
#include "auditconfig.h"

uint32_t AuditConfig::min_file_rotation_time = 0;
uint32_t AuditConfig::max_file_rotation_time = 0;

// The maximum file size before rotation is 500MB
#define MAX_ROTATE_FILE_SIZE (500 * 1024 * 1024)

bool AuditConfig::initialize_config(const std::string& str) {
    try {
            clean_up();
            cJSON *json_ptr = cJSON_Parse(str.c_str());
            if (json_ptr == NULL) {
                throw std::make_pair(JSON_PARSING_ERROR, str.c_str());
            }
            cJSON *config_json = json_ptr->child;
            while (config_json != NULL) {
                switch (config_json->type) {
                    case cJSON_Number:
                        if (strcmp(config_json->string, "version") == 0) {
                            if (config_json->valueint != 1) {
                                throw std::make_pair(VERSION_ERROR, "");
                            }
                        } else if (strcmp(config_json->string, "rotate_interval") == 0) {
                            rotate_interval = config_json->valueint;
                            if (rotate_interval < min_file_rotation_time) {
                                throw std::make_pair(ROTATE_INTERVAL_BELOW_MIN_ERROR, "");
                            } else if (rotate_interval > max_file_rotation_time) {
                                throw std::make_pair(ROTATE_INTERVAL_EXCEEDS_MAX_ERROR, "");
                            }
                        } else if (strcmp(config_json->string, "rotate_size") == 0) {
                            size_t size = (size_t)config_json->valueint;
                            if (size > MAX_ROTATE_FILE_SIZE) {
                                std::stringstream ss;
                                ss << size << " > " << MAX_ROTATE_FILE_SIZE;
                                throw std::make_pair(ROTATE_INTERVAL_SIZE_TOO_BIG, ss.str().c_str());
                            }
                            rotate_size = size;
                        } else {
                            throw std::make_pair(JSON_KEY_ERROR, config_json->string);
                        }
                        break;
                    case cJSON_String:
                        if ((strcmp(config_json->string, "log_path") == 0) ||
                            (strcmp(config_json->string, "descriptors_path") == 0)) {
                            using namespace CouchbaseDirectoryUtilities;

                            if (!isDirectory(config_json->valuestring)) {
                                throw std::make_pair(VALIDATE_PATH_ERROR,
                                                     config_json->valuestring);
                            } else if (strcmp(config_json->string, "log_path") == 0) {
                                log_path = std::string(config_json->valuestring);
                            } else {
                                descriptors_path = std::string(config_json->valuestring);
                                // check the descriptors path contains audit_events.json
                                std::stringstream tmp;
                                tmp << descriptors_path << DIRECTORY_SEPARATOR_CHARACTER;
                                tmp << "audit_events.json";
                                if (!AuditFile::file_exists(tmp.str())) {
                                    throw std::make_pair(MISSING_AUDIT_EVENTS_FILE_ERROR,
                                                         descriptors_path);
                                }
                            }
                        } else if (strcmp(config_json->string, "archive_path") == 0) {
                            // legacy - possibly defined but no longer used - do nothing
                            // @todo remove check for archive_path when removed from
                            // ns_server
                        } else {
                            throw std::make_pair(JSON_KEY_ERROR, config_json->string);
                        }
                        break;
                    case cJSON_Array:
                        if (strcmp(config_json->string, "sync") == 0) {
                            // @todo add code when support synchronous events
                        } else if (strcmp(config_json->string, "disabled") == 0) {
                            cJSON *disabled_events = config_json->child;
                            while (disabled_events != NULL) {
                                disabled.push_back(disabled_events->valueint);
                                disabled_events = disabled_events->next;
                            }
                        } else {
                            throw std::make_pair(JSON_KEY_ERROR, config_json->string);
                        }
                        break;
                    case cJSON_True:
                    case cJSON_False:
                        if (strcmp(config_json->string, "auditd_enabled") == 0) {
                            auditd_enabled = (config_json->type == cJSON_True) ? true : false;
                        } else {
                            throw std::make_pair(JSON_KEY_ERROR, config_json->string);
                        }
                        break;
                    default:
                        throw std::make_pair(JSON_UNKNOWN_FIELD_ERROR, "");
                }
                config_json = config_json->next;
            }
        assert(json_ptr != NULL);
        cJSON_Delete(json_ptr);
    } catch (std::pair<ErrorCode, char*>& exc) {
        Audit::log_error(exc.first, exc.second);
        return false;
    } catch (std::pair<ErrorCode, const char *>& exc) {
        Audit::log_error(exc.first, exc.second);
        return false;
    } catch (...) {
        Audit::log_error(CONFIGURATION_ERROR, NULL);
        return false;
    }
    return true;
}
