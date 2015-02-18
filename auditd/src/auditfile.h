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
#ifndef AUDITFILE_H
#define AUDITFILE_H

#include <fstream>
#include <inttypes.h>
#include <string>

class AuditFile {
public:
    std::ofstream af;
    std::string open_time_string;
    bool open_time_set;
    time_t open_time;

    AuditFile(void) :
        open_time_set(false),
        current_size(0),
        max_log_size(20 * 1024 * 1024)
    {
        af.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    }

    bool time_to_rotate_log(uint32_t rotate_interval);
    bool open(std::string& log_path);
    void close_and_rotate_log(std::string& new_file_path);
    bool cleanup_old_logfile(std::string& log_path);
    bool set_auditfile_open_time(std::string str);
    bool write_event_to_disk(const char *output);
    std::string get_open_file_path(void);

    static int64_t file_size(const std::string& name);
    static bool file_exists(const std::string& name);

private:
    std::string open_file_name;
    size_t current_size;
    size_t max_log_size;
};

#endif
