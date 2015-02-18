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
#include <iostream>
#include <algorithm>
#include <sstream>
#include <cJSON.h>
#include <sys/stat.h>
#include <cstring>
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


bool AuditFile::time_to_rotate_log(void) const {
    if (open_time_set) {
        time_t now;
        time(&now);
        if (difftime(now, open_time) > rotate_interval) {
            return true;
        }

        if (current_size > max_log_size) {
            return true;
        }
    }
    return false;
}


bool AuditFile::open(void) {
    cb_assert(!af.is_open());

    std::stringstream file;
    file << log_directory << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    open_file_name = file.str();
    af.open(open_file_name.c_str(), std::ios::out | std::ios::binary);
    if (!af.is_open()) {
        Audit::log_error(FILE_OPEN_ERROR, open_file_name.c_str());
        return false;
    }
    current_size = 0;
    return true;
}


void AuditFile::close_and_rotate_log(void) {
    cb_assert(af.is_open());
    af.close();
    current_size = 0;

    //cp the file to archive path and rename using auditfile_open_time_string

    // form the archive filename
    assert(!open_time_string.empty());
    if (!Audit::is_timestamp_format_correct(open_time_string)) {
        Audit::log_error(TIMESTAMP_FORMAT_ERROR, open_time_string.c_str());
    }

    std::string ts = open_time_string.substr(0,19);
    std::replace(ts.begin(), ts.end(), ':', '-');

    // move the audit_log to the archive.
    int count = 0;
    std::string fname;
    do {
        std::stringstream archive_file;
        archive_file << log_directory
                     << DIRECTORY_SEPARATOR_CHARACTER
                     << Audit::hostname
                     << "-"
                     << ts;
        if (count != 0) {
            archive_file << "-" << count;
        }

        archive_file << "-audit.log";
        fname.assign(archive_file.str());
        ++count;
    } while (file_exists(fname));

    if (rename(open_file_name.c_str(), fname.c_str()) != 0) {
        Audit::log_error(FILE_RENAME_ERROR, open_file_name.c_str());
    }
    open_time_set = false;
}


bool AuditFile::cleanup_old_logfile(const std::string& log_path) {
    std::stringstream file;
    file << log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    if (file_exists(file.str())) {
        if (file_size(file.str()) == 0) {
            // the file is empty so just remove
            if (remove(file.str().c_str()) != 0 ) {
                Audit::log_error(FILE_REMOVE_ERROR, file.str().c_str());
                return false;
            }
            return true;
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
                return false;
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
                return false;
            }
            if (!Audit::is_timestamp_format_correct(ts)) {
                Audit::log_error(TIMESTAMP_FORMAT_ERROR, ts.c_str());
                return false;
            }
            ts = ts.substr(0,19);
            std::replace(ts.begin(), ts.end(), ':', '-');
            // form the archive filename
            std::string archive_filename = Audit::hostname;
            archive_filename += "-" + ts + "-audit.log";
            // move the audit_log to the archive.
            std::stringstream archive_file;
            archive_file << log_path << DIRECTORY_SEPARATOR_CHARACTER
                         << archive_filename;
            if (rename (file.str().c_str(), archive_file.str().c_str()) != 0) {
                Audit::log_error(FILE_RENAME_ERROR, file.str().c_str());
                return false;
            }
        }
    }
    return true;
}


bool AuditFile::set_auditfile_open_time(const std::string &str) {
    assert(!str.empty());
    open_time_string = str;
    if (!Audit::is_timestamp_format_correct(open_time_string)) {
        Audit::log_error(TIMESTAMP_FORMAT_ERROR, open_time_string.c_str());
        return false;
    }
    std::string year = open_time_string.substr(0,4);
    std::string month = open_time_string.substr(5,2);
    std::string day = open_time_string.substr(8,2);
    std::string hour = open_time_string.substr(11,2);
    std::string min = open_time_string.substr(14,2);
    std::string sec = open_time_string.substr(17,2);

    struct tm time_str;
    time_str.tm_year = atoi(year.c_str()) - 1900;
    time_str.tm_mon = atoi(month.c_str()) - 1;
    time_str.tm_mday = atoi(day.c_str());
    time_str.tm_hour = atoi(hour.c_str());
    time_str.tm_min = atoi(min.c_str());
    time_str.tm_sec = atoi(sec.c_str());
    time_str.tm_isdst = 0;
    open_time = mktime(&time_str);
    open_time_set = true;
    return true;
}


bool AuditFile::write_event_to_disk(cJSON *output) {
    char *content = cJSON_PrintUnformatted(output);
    bool ret = true;
    if (content) {
        try {
            af << content << std::endl;
            current_size += strlen(content);
            af.flush();
        } catch (std::ofstream::failure& f) {
            Audit::log_error(WRITING_TO_DISK_ERROR, f.what());
            ret = false;
        }
        cJSON_Free(content);
    } else {
        Audit::log_error(MEMORY_ALLOCATION_ERROR, "failed to convert audit event");
    }

    return ret;
}


void AuditFile::set_log_directory(const std::string &new_directory) {
    if (log_directory == new_directory) {
        // No change
        return;
    }

    if (af.is_open()) {
        close_and_rotate_log();
    }

    log_directory.assign(new_directory);
    open();
}
