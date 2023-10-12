/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "auditconfig.h"

#include <nlohmann/json_fwd.hpp>
#include <cinttypes>
#include <cstdio>
#include <ctime>
#include <memory>
#include <string>

/**
 * The AuditFile class is responsible for writing audit records to a file
 * and perform file rotation (and prune old audit files).
 *
 * The "current" audit file is named "audit.log", and as part of file
 * rotation it'll be renamed to "hostname-timestamp[-counter]-audit.log"
 * (It is unlikely that the counter will be present, but in the theoretical
 * situation as part of size based rotation and a load where the size would
 * be reached within the second (causing an identical timestamp) the file
 * would already exist)
 *
 */
class AuditFile {
public:
    explicit AuditFile(std::string hostname) : hostname(std::move(hostname)) {
    }

    /**
     * Check if we need to rotate the logfile, and if so go ahead and do so.
     *
     * @returns true if the file was rotated
     */
    bool maybe_rotate_files();

    /**
     * Make sure that the auditfile is open
     *
     * @return true if success, false if we failed to open the file for
     *              some reason.
     */
    bool ensure_open();

    /// Close the audit trail (and rename it to the correct name)
    void close();

    /**
     * Look in the log directory if a file named "audit.log" exists
     * and try to move it to the correct name. This method is
     * used during startup for "crash recovery".
     *
     * @param log_path the directory to search
     * @throws std::system_error for file errors
     * @throws std::runtime_error for json parsing exceptions
     */
    void cleanup_old_logfile(const std::string& log_path);

    /**
     * Write a json formatted object to the disk
     *
     * @param output the data to write
     * @return true if success, false otherwise
     */
    bool write_event_to_disk(const nlohmann::json& output);

    /// Is the audit file open already?
    [[nodiscard]] bool is_open() const {
        return file.operator bool();
    }

    /**
     * Reconfigure the properties for the audit file (log directory,
     * rotation policy.
     */
    void reconfigure(const AuditConfig &config);

    /**
     * Flush the buffers to the disk
     *
     * @return true on success, false if an error occurs (and the file
     *         is closed and rotated)
     */
    bool flush();

    /**
     * get the number of seconds for the next log rotation
     */
    [[nodiscard]] uint32_t get_seconds_to_rotation() const;

protected:
    [[nodiscard]] bool open();
    [[nodiscard]] bool time_to_rotate_log() const;
    void close_and_rotate_log();
    void set_log_directory(const std::string &new_directory);
    [[nodiscard]] bool is_empty() const {
        return (current_size == 0);
    }

    [[nodiscard]] static time_t auditd_time();
    [[nodiscard]] static bool is_timestamp_format_correct(std::string_view str);

    struct FileDeleter {
        void operator()(FILE* fp) {
            fclose(fp);
        }
    };

    const std::string hostname;
    std::unique_ptr<FILE, FileDeleter> file;
    std::string open_file_name;
    std::string log_directory;
    time_t open_time = 0;
    size_t current_size = 0;
    size_t max_log_size = 20 * 1024 * 1024;
    uint32_t rotate_interval = 900;
    bool buffered = true;
};

