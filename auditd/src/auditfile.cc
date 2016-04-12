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
#include <platform/dirutils.h>
#include <memcached/isotime.h>
#include <JSON_checker.h>
#include <fstream>
#include "auditd.h"
#include "audit.h"
#include "auditfile.h"

#ifdef UNITTEST_AUDITFILE
#define log_error(a,b)
#define my_hostname "testing"

static std::string load_file(const char *file) {
    std::ifstream myfile(file, std::ios::in | std::ios::binary);
    if (myfile.is_open()) {
        std::string str((std::istreambuf_iterator<char>(myfile)),
                        std::istreambuf_iterator<char>());
        myfile.close();
        return str;
    } else {
        std::string str;
        return str;
    }
}

#else
#define log_error(a,b) Audit::log_error(a,b)
#define my_hostname Audit::hostname
#define load_file(a) Audit::load_file(a)
#endif

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
    cb_assert(open_time != 0);
    time_t now = auditd_time();
    if (difftime(now, open_time) > rotate_interval) {
        return true;
    }

    if (current_size > max_log_size) {
        return true;
    }

    return false;
}

time_t AuditFile::auditd_time() {
    struct timeval tv;

    if (cb_get_timeofday(&tv) == -1) {
        throw std::runtime_error("auditd_time: cb_get_timeofday failed");
    }
    return tv.tv_sec;
}

bool AuditFile::open(void) {
    cb_assert(file == NULL);
    cb_assert(open_time == 0);

    std::stringstream ss;
    ss << log_directory << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    open_file_name = ss.str();
    file = fopen(open_file_name.c_str(), "wb");
    if (file == NULL) {
        log_error(AuditErrorCode::FILE_OPEN_ERROR, open_file_name.c_str());
        return false;
    }
    current_size = 0;
    open_time = auditd_time();
    return true;
}


void AuditFile::close_and_rotate_log(void) {
    cb_assert(file != NULL);
    fclose(file);
    file = NULL;
    if (current_size == 0) {
        remove(open_file_name.c_str());
        return;
    }

    current_size = 0;

    std::string ts = ISOTime::generatetimestamp(open_time, 0).substr(0,19);
    std::replace(ts.begin(), ts.end(), ':', '-');

    // move the audit_log to the archive.
    int count = 0;
    std::string fname;
    do {
        std::stringstream archive_file;
        archive_file << log_directory
                     << DIRECTORY_SEPARATOR_CHARACTER
                     << my_hostname
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
        log_error(AuditErrorCode::FILE_RENAME_ERROR, open_file_name.c_str());
    }
    open_time = 0;
}


void AuditFile::cleanup_old_logfile(const std::string& log_path) {
    std::stringstream file;
    file << log_path << DIRECTORY_SEPARATOR_CHARACTER << "audit.log";
    std::string filename = file.str();

    if (file_exists(filename)) {
        // open the audit.log that needs archiving
        std::string str;
        try {
            str = load_file(filename.c_str());
        } catch (...) {
            std::stringstream ss;
            ss << "Audit: Failed to read \"" << filename << "\"";
            throw ss.str();
        }
        if (str.empty()) {
            // empty file, just remove it.
            if (remove(filename.c_str()) != 0) {
                std::stringstream ss;
                ss << "Audit: Failed to remove \"" << filename << "\": "
                   << strerror(errno);
                throw ss.str();
            }
            return;
        }

        // extract the first event
        std::size_t found = str.find_first_of("\n");
        if (found != std::string::npos) {
            str.erase(found+1, std::string::npos);
        }

        // check that it is valid json (cJSON doesn't validate
        // and may run outside the buffers...)
        if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(str.data()), str.size())) {
            std::stringstream ss;
            ss << "Audit: Failed to parse data in audit file (invalid JSON) \""
            << filename << "\"";
            throw ss.str();
        }

        cJSON *json_ptr = cJSON_Parse(str.c_str());
        if (json_ptr == NULL) {
            std::stringstream ss;
            ss << "Audit: Failed to parse data in audit file \""
               << filename << "\"";
            throw ss.str();
        }

        // Find the timestamp
        cJSON *timestamp = cJSON_GetObjectItem(json_ptr, "timestamp");
        if (timestamp == NULL) {
            cJSON_Delete(json_ptr);
            std::stringstream ss;
            ss << "Audit: Failed to locate \"timestamp\" in audit file \""
               << filename << "\": " << str;
            throw ss.str();
        }

        if (timestamp->type != cJSON_String) {
            cJSON_Delete(json_ptr);
            std::stringstream ss;
            ss << "Audit: Incorrect format for \"timestamp\" in audit "
               << "file \"" << filename << "\" (expected string): "
               << str;
            throw ss.str();
        }

        std::string ts(timestamp->valuestring);
        if (!is_timestamp_format_correct(ts)) {
            cJSON_Delete(json_ptr);
            std::stringstream ss;
            ss << "Audit: Incorrect format for \"timestamp\" in audit "
               << "file \"" << filename << "\": "
               << ts;
            throw ss.str();
        }

        ts = ts.substr(0, 19);
        std::replace(ts.begin(), ts.end(), ':', '-');
        // form the archive filename
        std::string archive_filename = my_hostname;
        archive_filename += "-" + ts + "-audit.log";
        // move the audit_log to the archive.
        std::stringstream archive_file;
        archive_file << log_path << DIRECTORY_SEPARATOR_CHARACTER
                     << archive_filename;
        if (rename(filename.c_str(), archive_file.str().c_str()) != 0) {
            cJSON_Delete(json_ptr);
            std::stringstream ss;
            ss << "Audit: failed to rename \"" << filename << "\" to \""
               << archive_file.str() << "\": " << strerror(errno);
            throw ss.str();
        }
        cJSON_Delete(json_ptr);
    }
}


bool AuditFile::write_event_to_disk(cJSON *output) {
    char *content = cJSON_PrintUnformatted(output);
    bool ret = true;
    if (content) {
        current_size += fprintf(file, "%s\n", content);
        if (ferror(file)) {
            log_error(AuditErrorCode::WRITING_TO_DISK_ERROR, strerror(errno));
            ret = false;
            close_and_rotate_log();
        } else if (!buffered) {
            ret = flush();
        }
        cJSON_Free(content);
    } else {
        log_error(AuditErrorCode::MEMORY_ALLOCATION_ERROR,
                  "failed to convert audit event");
    }

    return ret;
}


void AuditFile::set_log_directory(const std::string &new_directory) {
    if (log_directory == new_directory) {
        // No change
        return;
    }

    if (file != NULL) {
        close_and_rotate_log();
    }

    log_directory.assign(new_directory);

    if (!CouchbaseDirectoryUtilities::mkdirp(log_directory)) {
        // The directory does not exist and we failed to create
        // it. This is not a fatal error, but it does mean that the
        // node won't be able to do any auditing
        log_error(AuditErrorCode::AUDIT_DIRECTORY_DONT_EXIST,
                  log_directory.c_str());
    }
}

void AuditFile::reconfigure(const AuditConfig &config) {
    rotate_interval = config.get_rotate_interval();
    set_log_directory(config.get_log_directory());
    max_log_size = config.get_rotate_size();
    buffered = config.is_buffered();
}

bool AuditFile::flush(void) {
    if (is_open()) {
        if (fflush(file) != 0) {
            log_error(AuditErrorCode::WRITING_TO_DISK_ERROR,
                      strerror(errno));
            close_and_rotate_log();
            return false;
        }
    }

    return true;
}

bool AuditFile::is_timestamp_format_correct(std::string& str) {
    const char *data = str.c_str();
    if (str.length() < 19) {
        return false;
    } else if (isdigit(data[0]) && isdigit(data[1]) &&
               isdigit(data[2]) && isdigit(data[3]) &&
               data[4] == '-' &&
               isdigit(data[5]) && isdigit(data[6]) &&
               data[7] == '-' &&
               isdigit(data[8]) && isdigit(data[9]) &&
               data[10] == 'T' &&
               isdigit(data[11]) && isdigit(data[12]) &&
               data[13] == ':' &&
               isdigit(data[14]) && isdigit(data[15]) &&
               data[16] == ':' &&
               isdigit(data[17]) && isdigit(data[18])) {
        return true;
    }
    return false;
}
