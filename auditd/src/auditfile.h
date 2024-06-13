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
#include <filesystem>
#include <functional>
#include <memory>
#include <string>

/**
 * The AuditFile class is responsible for writing audit records to a file
 * and perform file rotation (and prune old audit files).
 *
 * The audit log is written to a file named
 *    "hostname-timestamp[-counter]-audit.log"
 * (It is unlikely that the counter will be present, but in the theoretical
 * situation as part of size based rotation and a load where the size would
 * be reached within the second (causing an identical timestamp) the file
 * would already exist) and there is a symbolic link named "current-audit.log"
 * pointing to the file we're currently writing to.
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
     * Remove the link to the current audit file in the provided log_path
     * (or the log directory if no value present)
     */
    void remove_audit_link(
            const std::optional<std::filesystem::path>& log_path = {}) const;

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

    [[nodiscard]] std::chrono::seconds get_sleep_time() const;

    void prune_old_audit_files();

protected:
    [[nodiscard]] bool open();
    [[nodiscard]] bool time_to_rotate_log() const;
    void close_and_rotate_log();
    void set_log_directory(const std::string& new_directory);
    [[nodiscard]] bool is_empty() const {
        return (current_size == 0);
    }

    [[nodiscard]] static time_t auditd_time();
    static void remove_file(const std::filesystem::path& path);

    /**
     * Iterate over "old" audit log files (named hostname-<timestamp>-audit.log)
     * and call the provided callback with the path
     *
     * @param callback
     */
    void iterate_old_files(
            const std::function<void(const std::filesystem::path&)>& callback)
            const;

    struct FileDeleter {
        void operator()(FILE* fp) {
            fclose(fp);
        }
    };

    const std::string hostname;
    std::unique_ptr<FILE, FileDeleter> file;
    std::filesystem::path open_file_name;
    std::filesystem::path log_directory;
    time_t open_time = 0;
    size_t current_size = 0;
    size_t max_log_size = 20 * 1024 * 1024;
    uint32_t rotate_interval = 900;
    std::optional<std::chrono::seconds> prune_age;
    /// Iterating over all files in the directory and fetch their
    /// modification time may be "costly" so we don't want to run
    /// it too often.
    std::chrono::steady_clock::time_point next_prune =
            std::chrono::steady_clock::now();
    bool buffered = true;
};

