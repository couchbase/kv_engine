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
    bool set_open_time;
    time_t open_time;

    AuditFile(void) {
        set_open_time = false;
    }

    bool time_to_rotate_log(uint32_t rotate_interval);
    int8_t open(std::string& log_path);
    void close_and_rotate_log(std::string& log_path, std::string& archive_path);
    int8_t cleanup_old_logfile(std::string& log_path, std::string& archive_path);

    static int64_t file_size(const std::string& name);
    static bool file_exists(const std::string& name);
};

#endif
