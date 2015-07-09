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
#ifndef AUDITD_H
#define AUDITD_H

enum class AuditErrorCode {
    AUDIT_EXTENSION_DATA_ERROR,
    FILE_OPEN_ERROR,
    FILE_RENAME_ERROR,
    FILE_REMOVE_ERROR,
    MEMORY_ALLOCATION_ERROR,
    JSON_PARSING_ERROR,
    JSON_MISSING_DATA_ERROR,
    JSON_MISSING_OBJECT_ERROR,
    JSON_KEY_ERROR,
    JSON_ID_ERROR,
    JSON_UNKNOWN_FIELD_ERROR,
    CB_CREATE_THREAD_ERROR,
    EVENT_PROCESSING_ERROR,
    PROCESSING_EVENT_FIELDS_ERROR,
    TIMESTAMP_MISSING_ERROR,
    TIMESTAMP_FORMAT_ERROR,
    EVENT_ID_ERROR,
    VERSION_ERROR,
    VALIDATE_PATH_ERROR,
    ROTATE_INTERVAL_BELOW_MIN_ERROR,
    ROTATE_INTERVAL_EXCEEDS_MAX_ERROR,
    OPEN_AUDITFILE_ERROR,
    SETTING_AUDITFILE_OPEN_TIME_ERROR,
    WRITING_TO_DISK_ERROR,
    WRITE_EVENT_TO_DISK_ERROR,
    UNKNOWN_EVENT_ERROR,
    CONFIG_INPUT_ERROR,
    CONFIGURATION_ERROR,
    MISSING_AUDIT_EVENTS_FILE_ERROR,
    ROTATE_INTERVAL_SIZE_TOO_BIG,
    AUDIT_DIRECTORY_DONT_EXIST
};

extern time_t auditd_time(time_t *tloc);

#endif
