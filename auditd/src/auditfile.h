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
#pragma once

#include "auditconfig.h"

#include <nlohmann/json_fwd.hpp>
#include <cinttypes>
#include <cstdio>
#include <ctime>
#include <string>

class AuditFile {
public:
    ~AuditFile();

    /**
     * Check if we need to rotate the logfile, and if so go ahead and
     * do so.
     */
    bool maybe_rotate_files();

    /**
     * Make sure that the auditfile is open
     *
     * @return true if success, false if we failed to open the file for
     *              some reason.
     */
    bool ensure_open();

    /**
     * Close the audit trail (and rename it to the correct name
     */
    void close();

    /**
     * Look in the log directory if a file named "audit.log" exists
     * and try to move it to the correct name. This method is
     * used during startup for "crash recovery".
     *
     * @param log_path the directory to search
     */
    void cleanup_old_logfile(const std::string& log_path);

    /**
     * Write a json formatted object to the disk
     *
     * @param output the data to write
     * @return true if success, false otherwise
     */
    bool write_event_to_disk(nlohmann::json& output);

    /**
     * Is the audit file open already?
     */
    bool is_open() const {
        return file != nullptr;
    }

    /**
     * Reconfigure the properties for the audit file (log directory,
     * rotation policy.
     */
    void reconfigure(const AuditConfig &config);

    /**
     * Flush the buffers to the disk
     */
    bool flush();

    /**
     * get the number of seconds for the next log rotation
     */
    uint32_t get_seconds_to_rotation() const;

private:
    bool open();
    bool time_to_rotate_log() const;
    void close_and_rotate_log();
    void set_log_directory(const std::string &new_directory);
    bool is_timestamp_format_correct(std::string& str);

    bool is_empty() const {
        return (current_size == 0);
    }

    static time_t auditd_time();

    FILE* file = nullptr;
    std::string open_file_name;
    std::string log_directory;
    time_t open_time = 0;
    size_t current_size = 0;
    size_t max_log_size = 20 * 1024 * 1024;
    uint32_t rotate_interval = 900;
    bool buffered = true;
};

