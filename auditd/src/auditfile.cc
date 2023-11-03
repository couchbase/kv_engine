/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "auditfile.h"

#include <folly/ScopeGuard.h>
#include <logger/logger.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/platform_time.h>
#include <platform/strerror.h>
#include <utilities/json_utilities.h>
#include <algorithm>
#include <iostream>
#include <sstream>

/// c++20 std::string::starts_with
bool starts_with(std::string_view name, std::string_view prefix) {
    return name.find(prefix) == 0;
}

/// c++20 std::string::ends_with
bool ends_with(std::string_view name, std::string_view suffix) {
    const auto idx = name.rfind(suffix);
    if (idx == std::string_view::npos) {
        return false;
    }
    return (idx + suffix.size()) == name.length();
}

void AuditFile::iterate_old_files(
        const std::function<void(const std::filesystem::path&)>& callback) {
    using namespace std::filesystem;
    for (const auto& p : directory_iterator(log_directory)) {
        try {
            const auto& path = p.path();
            if (starts_with(path.filename().generic_string(), hostname) &&
                ends_with(path.generic_string(), "-audit.log")) {
                callback(path);
            }
        } catch (const std::exception& e) {
            LOG_WARNING(
                    "AuditFile::iterate_old_files(): Exception occurred "
                    "while inspecting \"{}\": {}",
                    p.path().generic_string(),
                    e.what());
        }
    }
}

void AuditFile::prune_old_audit_files() {
    using namespace std::chrono;
    using namespace std::filesystem;

    const auto filesystem_now = file_time_type::clock::now();
    const auto now = steady_clock::now();
    if (!prune_age.has_value() || next_prune > now) {
        return;
    }

    auto oldest = filesystem_now;

    const auto then = filesystem_now - *prune_age;
    iterate_old_files([this, then, &oldest](const auto& path) {
        auto mtime = last_write_time(path);
        if (mtime < then) {
            remove(path);
        } else if (mtime < oldest) {
            oldest = mtime;
        }
    });

    if (oldest == filesystem_now) {
        next_prune = now + *prune_age;
    } else {
        auto age = duration_cast<seconds>(filesystem_now - oldest);

        // set next prune to a second after the oldest file expire
        next_prune = now + (*prune_age - age) + seconds(1);
    }
}

bool AuditFile::maybe_rotate_files() {
    auto prune = folly::makeGuard([this] { prune_old_audit_files(); });
    if (is_open() && time_to_rotate_log()) {
        if (is_empty()) {
            // Given the audit log is empty on rotation instead of
            // closing and then re-opening we can just keep open and
            // update the open_time.
            open_time = auditd_time();
            return false;
        }
        close_and_rotate_log();
        return true;
    }
    return false;
}

bool AuditFile::ensure_open() {
    if (!is_open()) {
        return open();
    } else {
        if (maybe_rotate_files()) {
            return open();
        }
    }
    return true;
}

void AuditFile::close() {
    if (is_open()) {
        close_and_rotate_log();
    }
}

uint32_t AuditFile::get_seconds_to_rotation() const {
    if (is_open()) {
        time_t now = auditd_time();
        return rotate_interval - (uint32_t)difftime(now, open_time);
    } else {
        return rotate_interval;
    }
}

std::chrono::seconds AuditFile::get_sleep_time() const {
    using namespace std::chrono;

    const auto rotation = seconds{get_seconds_to_rotation()};
    if (!prune_age) {
        return rotation;
    }
    const auto now = steady_clock::now();
    if (next_prune <= now) {
        return seconds{0};
    }
    return std::min(rotation, duration_cast<seconds>(next_prune - now));
}

bool AuditFile::time_to_rotate_log() const {
    if (rotate_interval) {
        cb_assert(open_time != 0);
        time_t now = auditd_time();
        if (difftime(now, open_time) > rotate_interval) {
            return true;
        }
    }

    if (max_log_size && (current_size > max_log_size)) {
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

bool AuditFile::open() {
    cb_assert(!file);
    cb_assert(open_time == 0);

    open_file_name = cb::io::sanitizePath(log_directory + "/audit.log");
    file.reset(fopen(open_file_name.c_str(), "wb"));
    if (!file) {
        LOG_WARNING("Audit: open error on file {}: {}",
                    open_file_name,
                    cb_strerror());
        return false;
    }

    current_size = 0;
    open_time = auditd_time();
    return true;
}

void AuditFile::close_and_rotate_log() {
    cb_assert(file);
    file.reset();
    if (current_size == 0) {
        remove(open_file_name.c_str());
        open_time = 0;
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
        archive_file << log_directory << cb::io::DirectorySeparator << hostname
                     << "-" << ts;
        if (count != 0) {
            archive_file << "-" << count;
        }

        archive_file << "-audit.log";
        fname.assign(archive_file.str());
        ++count;
    } while (cb::io::isFile(fname));

    if (rename(open_file_name.c_str(), fname.c_str()) != 0) {
        LOG_WARNING("Audit: rename error on file {}: {}",
                    open_file_name,
                    cb_strerror());
    }
    open_time = 0;
}

void AuditFile::cleanup_old_logfile(const std::string& log_path) {
    auto filename = cb::io::sanitizePath(log_path + "/audit.log");

    if (cb::io::isFile(filename)) {
        // open the audit.log that needs archiving
        std::string str = cb::io::loadFile(filename);

        if (str.empty()) {
            // empty file, just remove it.
            if (remove(filename.c_str()) != 0) {
                throw std::system_error(errno,
                                        std::system_category(),
                                        "AuditFile::cleanup_old_logfile(): "
                                        "Failed to remove \"" +
                                                filename + "\"");
            }
            return;
        }

        // extract the first event
        std::size_t found = str.find_first_of("\n");
        if (found != std::string::npos) {
            str.erase(found+1, std::string::npos);
        }

        nlohmann::json json;
        try {
            json = nlohmann::json::parse(str);
        } catch (const nlohmann::json::exception&) {
            throw std::runtime_error(
                    "AuditFile::cleanup_old_logfile(): "
                    "Failed to parse data in audit file "
                    "(invalid JSON) " +
                    filename);
        }

        std::string ts;
        try {
            ts = cb::jsonGet<std::string>(json, "timestamp");
        } catch (const nlohmann::json::exception& e) {
            throw std::runtime_error(
                    "AuditFile::cleanup_old_logfile(): "
                    "Could not parse timestamp for auditfile: " +
                    filename + ". Exception thrown: " + e.what());
        }

        if (!is_timestamp_format_correct(ts)) {
            throw std::runtime_error(
                    R"(AuditFile::cleanup_old_logfile(): Incorrect format for
                    "timestamp" in audit file ")" +
                    filename);
        }

        ts = ts.substr(0, 19);
        std::replace(ts.begin(), ts.end(), ':', '-');
        // form the archive filename
        auto archive_file = log_path + "/" + hostname + "-" + ts + "-audit.log";
        archive_file = cb::io::sanitizePath(archive_file);
        if (rename(filename.c_str(), archive_file.c_str()) != 0) {
            throw std::system_error(
                    errno,
                    std::system_category(),
                    "AuditFile::cleanup_old_logfile(): Failed to rename \"" +
                            filename + "\" to \"" + archive_file + "\"");
        }
    }
}

bool AuditFile::write_event_to_disk(nlohmann::json& output) {
    bool ret = true;
    try {
        const auto content = output.dump();
        current_size += fprintf(file.get(), "%s\n", content.c_str());
        if (ferror(file.get())) {
            LOG_WARNING("Audit: writing to disk error: {}", cb_strerror());
            ret = false;
            close_and_rotate_log();
        } else if (!buffered) {
            ret = flush();
        }
    } catch (const std::bad_alloc&) {
        LOG_WARNING_RAW(
                "Audit: memory allocation error for writing audit event to "
                "disk");
        // Failed to write event to disk.
        return false;
    }

    return ret;
}


void AuditFile::set_log_directory(const std::string &new_directory) {
    if (log_directory == new_directory) {
        // No change
        return;
    }

    if (file != nullptr) {
        close_and_rotate_log();
    }

    log_directory.assign(new_directory);
    try {
        cb::io::mkdirp(log_directory);
    } catch (const std::runtime_error& error) {
        // The directory does not exist and we failed to create
        // it. This is not a fatal error, but it does mean that the
        // node won't be able to do any auditing
        LOG_WARNING(R"(Audit: failed to create audit directory "{}": {})",
                    new_directory,
                    error.what());
    }
}

void AuditFile::reconfigure(const AuditConfig &config) {
    rotate_interval = config.get_rotate_interval();
    set_log_directory(config.get_log_directory());
    max_log_size = config.get_rotate_size();
    buffered = config.is_buffered();
    prune_age = config.get_prune_age();
    next_prune = std::chrono::steady_clock::now();
}

bool AuditFile::flush() {
    if (is_open()) {
        if (fflush(file.get()) != 0) {
            LOG_WARNING("Audit: writing to disk error: {}", cb_strerror());
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
