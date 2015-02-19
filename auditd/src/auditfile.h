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

    AuditFile(void) :
        open_time_set(false),
        current_size(0),
        max_log_size(20 * 1024 * 1024),
        rotate_interval(900),
        buffered(true)
    {
        af.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    }

    ~AuditFile() {
        close();
    }

    /**
     * Check if we need to rotate the logfile, and if so go ahead and
     * do so.
     */
    void maybe_rotate_files(void) {
        if (is_open() && time_to_rotate_log()) {
            close_and_rotate_log();
        }
    }

    /**
     * Make sure that the auditfile is open
     *
     * @return true if success, false if we failed to open the file for
     *              some reason.
     */
    bool ensure_open(void) {
        if (!is_open()) {
            return open();
        }
        return true;
    }

    /**
     * Close the audit trail (and rename it to the correct name
     */
    void close(void) {
        if (is_open()) {
            close_and_rotate_log();
        }
    }

    /**
     * Look in the log directory if a file named "audit.log" exists
     * and try to move it to the correct name. This method is
     * used during startup for "crash recovery".
     *
     * @param log_path the directory to search
     */
    bool cleanup_old_logfile(const std::string& log_path);

    /**
     * Set the timestamp for the first object to put in the file.
     *
     * @param str the time to parse
     * @return true if success, false otherwise
     */
    bool set_auditfile_open_time(const std::string &str);

    /**
     * Has the open time been set already
     */
    bool is_open_time_set(void) const {
        return open_time_set;
    }

    /**
     * Write a json formatted object to the disk
     *
     * @param output the data to write
     * @return true if success, false otherwise
     */
    bool write_event_to_disk(cJSON *output);

    /**
     * Get the size of a file
     *
     * @param name the name of the file to query
     * @return the size of the file or -1 if an error occurs
     */
    static int64_t file_size(const std::string& name);

    /**
     * Check for a file existence
     *
     * @param name the name to check for
     * @return true if the file exists, false otherwise
     */
    static bool file_exists(const std::string& name);

    /**
     * Is the audit file open already?
     */
    bool is_open(void) const {
        return af.is_open();
    }

    /**
     * Reconfigure the properties for the audit file (log directory,
     * rotation policy.
     */
    void reconfigure(const AuditConfig &config);

    /**
     * Flush the buffers to the disk
     */
    void flush(void);

private:
    bool open(void);
    bool time_to_rotate_log(void) const;
    void close_and_rotate_log(void);
    void set_log_directory(const std::string &new_directory);

    std::ofstream af;
    std::string open_time_string;
    std::string open_file_name;
    std::string log_directory;
    bool open_time_set;
    time_t open_time;
    size_t current_size;
    size_t max_log_size;
    uint32_t rotate_interval;
    bool buffered;
};

#endif
