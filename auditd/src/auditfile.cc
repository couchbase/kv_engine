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

#include <algorithm>
#include <sstream>
#include <cJSON.h>
#include <sys/stat.h>
#include "auditd.h"
#include "audit.h"
#include "auditfile.h"

int64_t AuditFile::file_size(const std::string& name) {
#ifdef WIN32
    WIN32_FILE_ATTRIBUTE_DATA fad;
    if (!GetFileAttributesEx(name.c_str(), GetFileExInfoStandard,
                             &fad) == 0) {
        Audit::log_error(FILE_ATTRIBUTES_ERROR, name.c_str());
        return -1;
    }
    LARGE_INTEGER size;
    size.HighPart = fad.nFileSizeHigh;
    size.LowPart = fad.nFileSizeLow;
    return size.QuadPart;
#else
    struct stat buffer;
    if (stat (name.c_str(), &buffer) != 0) {
        Audit::log_error(FILE_ATTRIBUTES_ERROR, name.c_str());
        return -1;
    }
    return (int64_t)buffer.st_size;
#endif
}


bool AuditFile::file_exists(const std::string& name) {
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


bool AuditFile::time_to_rotate_log(uint32_t rotate_interval) {
    if (set_open_time) {
        time_t now;
        time(&now);
        if (difftime(now, open_time) > rotate_interval) {
            return true;
        }
    }
    return false;
}


int8_t AuditFile::open(std::string& log_path) {
    assert(!af.is_open());
    std::stringstream file;
    file << log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    af.open(file.str().c_str(), std::ios::out | std::ios::binary);
    if (!af.is_open()) {
        Audit::log_error(FILE_OPEN_ERROR, file.str().c_str());
        return -1;
    }
    return 0;
}


void AuditFile::close_and_rotate_log(std::string& log_path, std::string& archive_path) {
    assert(af.is_open());
    af.close();
    //cp the file to archive path and rename using auditfile_open_time_string
    std::stringstream audit_file;
    std::stringstream archive_file;
    audit_file << log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";

    // form the archive filename
    std::string archive_filename = Audit::hostname;
    assert(!open_time_string.empty());
    if (!Audit::is_timestamp_format_correct(open_time_string)) {
        Audit::log_error(TIMESTAMP_FORMAT_ERROR, open_time_string.c_str());
    }
    std::string ts = open_time_string.substr(0,19);
    std::replace(ts.begin(), ts.end(), ':', '-');
    archive_filename += "-" + ts + "-audit.log";
    // move the audit_log to the archive.
    archive_file << archive_path << DIRECTORY_SEPARATOR_CHARACTER << archive_filename;

    // check if archive file already exists if so delete
    if (file_exists(archive_file.str().c_str())) {
        if (remove(archive_file.str().c_str()) != 0) {
            Audit::log_error(FILE_REMOVE_ERROR, archive_file.str().c_str());
        }
    }
    if (rename (audit_file.str().c_str(), archive_file.str().c_str()) != 0) {
        Audit::log_error(FILE_RENAME_ERROR, audit_file.str().c_str());
    }
    set_open_time = false;
}


int8_t AuditFile::cleanup_old_logfile(std::string& log_path, std::string& archive_path) {
    std::stringstream file;
    file << log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    if (file_exists(file.str())) {
        if (file_size(file.str()) == 0) {
            // the file is empty so just remove
            if (remove(file.str().c_str()) != 0 ) {
                Audit::log_error(FILE_REMOVE_ERROR, file.str().c_str());
                return -1;
            }
            return 0;
        } else {
            // open the audit.log that needs archiving
            std::string str = Audit::load_file(file.str().c_str());
            assert(!str.empty());
            // extract the first event
            std::size_t found = str.find_first_of("\n");
            str.erase(found+1, std::string::npos);
            cJSON *json_ptr = cJSON_Parse(str.c_str());
            if (json_ptr == NULL) {
                Audit::log_error(JSON_PARSING_ERROR, str.c_str());
                return -1;
            }
            // extract the timestamp
            std::string ts;
            cJSON *fields = json_ptr->child;
            while (fields != NULL) {
                std::string name = fields->string;
                if (name.compare("timestamp") == 0) {
                    ts = std::string(fields->valuestring);
                    break;
                }
                fields = fields->next;
            }
            if (ts.empty()) {
                Audit::log_error(TIMESTAMP_MISSING_ERROR, NULL);
                return -1;
            }
            if (!Audit::is_timestamp_format_correct(ts)) {
                Audit::log_error(TIMESTAMP_FORMAT_ERROR, ts.c_str());
                return -1;
            }
            ts = ts.substr(0,19);
            std::replace(ts.begin(), ts.end(), ':', '-');
            // form the archive filename
            std::string archive_filename = Audit::hostname;
            archive_filename += "-" + ts + "-audit.log";
            // move the audit_log to the archive.
            std::stringstream archive_file;
            archive_file << archive_path << DIRECTORY_SEPARATOR_CHARACTER
            << archive_filename;
            if (rename (file.str().c_str(), archive_file.str().c_str()) != 0) {
                Audit::log_error(FILE_RENAME_ERROR, file.str().c_str());
                return -1;
            }
        }
    }
    return 0;
}
