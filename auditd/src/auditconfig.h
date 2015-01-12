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
#ifndef AUDITCONFIG_H
#define AUDITCONFIG_H

#include <string>
#include <vector>
#include <inttypes.h>
#include "memcached/audit_interface.h"


typedef enum {
    CONFIG_JSON_PARSING_ERROR,
    CONFIG_JSON_KEY_ERROR,
    CONFIG_JSON_UNKNOWN_FIELD_ERROR,
    CONFIG_ROTATE_INTERVAL_BELOW_MIN_ERROR,
    CONFIG_ROTATE_INTERVAL_EXCEEDS_MAX_ERROR,
    CONFIG_VERSION_ERROR,
    CONFIG_VALIDATE_PATH_ERROR
  } ConfigErrorCode;


class AuditConfig {
public:
    uint32_t rotate_interval;
    bool cbauditd_enabled;
    std::string log_path;
    std::string archive_path;
    std::vector<uint32_t> enabled;
    std::vector<uint32_t> sync;
    static EXTENSION_LOGGER_DESCRIPTOR *logger;
    static uint32_t min_file_rotation_time;
    static uint32_t max_file_rotation_time;

    static void log_error(const ConfigErrorCode error_code, const std::string& str);
    bool initialize_config(const std::string& str);
};

#endif
