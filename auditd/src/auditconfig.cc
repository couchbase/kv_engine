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

#include <string>
#include <cstring>
#include <cJSON.h>
#include <sys/stat.h>
#include <platform/platform.h>
#include <platform/dirutils.h>
#include "auditconfig.h"


EXTENSION_LOGGER_DESCRIPTOR* AuditConfig::logger = NULL;
uint32_t AuditConfig::min_file_rotation_time = 0;
uint32_t AuditConfig::max_file_rotation_time = 0;

void AuditConfig::log_error(const ConfigErrorCode error_code, const std::string& str) {
    switch (error_code) {
        case CONFIG_JSON_PARSING_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "config JSON parsing error on "
                        "string \"%s\"", str.c_str());
            break;
        case CONFIG_JSON_KEY_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "config JSON key \"%s\" error", str.c_str());
            break;
        case CONFIG_JSON_UNKNOWN_FIELD_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "config JSON unknown field error");
            break;
        case CONFIG_ROTATE_INTERVAL_BELOW_MIN_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "config rotate_interval below minimum error");
            break;
        case CONFIG_ROTATE_INTERVAL_EXCEEDS_MAX_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "config rotate_interval exceeds maximum error");
            break;
        case CONFIG_VALIDATE_PATH_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "config validate path \"%s\" error", str.c_str());
            break;
        case CONFIG_VERSION_ERROR:
            logger->log(EXTENSION_LOG_WARNING, NULL, "config audit version error");
            break;
        default:
            assert(false);
    }
}


bool AuditConfig::initialize_config(const std::string& str) {
    try {
            cJSON *json_ptr = cJSON_Parse(str.c_str());
            if (json_ptr == NULL) {
                throw std::make_pair(CONFIG_JSON_PARSING_ERROR, str);
            }
            cJSON *config_json = json_ptr->child;
            while (config_json != NULL) {
                switch (config_json->type) {
                    case cJSON_Number:
                        if (strcmp(config_json->string, "version") == 0) {
                            if (config_json->valueint != 1) {
                                throw std::make_pair(CONFIG_VERSION_ERROR, std::string(""));
                            }
                        } else if (strcmp(config_json->string, "rotate_interval") == 0) {
                            rotate_interval = config_json->valueint;
                            if (rotate_interval < min_file_rotation_time) {
                                throw std::make_pair(CONFIG_ROTATE_INTERVAL_BELOW_MIN_ERROR,
                                                     std::string(""));
                            } else if (rotate_interval > max_file_rotation_time) {
                                throw std::make_pair(CONFIG_ROTATE_INTERVAL_EXCEEDS_MAX_ERROR,
                                                     std::string(""));
                            }
                        } else {
                            throw std::make_pair(CONFIG_JSON_KEY_ERROR, std::string(config_json->string));
                        }
                        break;
                    case cJSON_String:
                        if ((strcmp(config_json->string, "log_path") == 0) ||
                            (strcmp(config_json->string, "archive_path") == 0)) {
                            using namespace CouchbaseDirectoryUtilities;

                            if (!isDirectory(config_json->valuestring)) {
                                throw std::make_pair(CONFIG_VALIDATE_PATH_ERROR,
                                                     std::string(config_json->valuestring));
                            } else if (strcmp(config_json->string, "log_path") == 0) {
                                log_path = config_json->valuestring;
                            } else {
                                archive_path = config_json->valuestring;
                            }
                        } else {
                            throw std::make_pair(CONFIG_JSON_KEY_ERROR, std::string(config_json->string));
                        }
                        break;
                    case cJSON_Array:
                        if (strcmp(config_json->string, "sync") == 0) {
                            // @todo add code when support synchronous events
                        } else if (strcmp(config_json->string, "enabled") == 0) {
                            cJSON *enabled_events = config_json->child;
                            while (enabled_events != NULL) {
                                enabled.push_back(enabled_events->valueint);
                                enabled_events = enabled_events->next;
                            }
                        } else {
                            throw std::make_pair(CONFIG_JSON_KEY_ERROR, std::string(config_json->string));
                        }
                        break;
                    case cJSON_True:
                    case cJSON_False:
                        if (strcmp(config_json->string, "cbauditd_enabled") == 0) {
                            cbauditd_enabled = (config_json->type == cJSON_True) ? true : false;
                        } else {
                            throw std::make_pair(CONFIG_JSON_KEY_ERROR, std::string(config_json->string));
                        }
                        break;
                    default:
                        throw std::make_pair(CONFIG_JSON_UNKNOWN_FIELD_ERROR, std::string(""));
                }
                config_json = config_json->next;
            }
        assert(json_ptr != NULL);
        cJSON_Delete(json_ptr);
    } catch (std::pair<ConfigErrorCode, const std::string>& exc) {
        log_error(exc.first, exc.second);
        return false;
    }
    return true;
}
