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

#include <dek/manager.h>
#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/platform_time.h>
#include <platform/timeutils.h>
#include <chrono>
#include <filesystem>
#include <sstream>
#include <thread>

/// The name of the link to the "current" audit trail
static constexpr std::string_view current_audit_link_name = "current-audit.log";

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
        const std::function<void(const std::filesystem::path&)>& callback)
        const {
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
        const auto age = duration_cast<seconds>(filesystem_now - oldest);

        // set next prune to a second after the oldest file expire
        next_prune = now + (*prune_age - age) + seconds(1);
    }
}

bool AuditFile::maybe_rotate_files() {
    auto prune = folly::makeGuard([this] { prune_old_audit_files(); });
    if (is_open() && time_to_rotate_log()) {
        close_and_rotate_log();
        return true;
    }
    return false;
}

bool AuditFile::ensure_open() {
    if (!is_open()) {
        return open();
    }

    if (maybe_rotate_files()) {
        return open();
    }

    return true;
}

void AuditFile::close() {
    if (is_open()) {
        close_and_rotate_log();
    }
}

uint32_t AuditFile::get_seconds_to_rotation() const {
    if (rotate_interval) {
        if (is_open()) {
            const auto now = auditd_time();
            const auto diff = static_cast<uint32_t>(difftime(now, open_time));
            return std::clamp(rotate_interval - diff, 0U, rotate_interval);
        }
        return rotate_interval;
    }
    return std::numeric_limits<uint32_t>::max();
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

    if (max_log_size && (file->size() > max_log_size)) {
        return true;
    }

    return false;
}

time_t AuditFile::auditd_time() {
    timeval tv = {};
    if (cb_get_timeofday(&tv) == -1) {
        throw std::runtime_error("auditd_time: cb_get_timeofday failed");
    }
    return tv.tv_sec;
}

bool AuditFile::open() {
    Expects(!file && "The file shouldn't be open");
    open_time = auditd_time();
    auto ts = cb::time::timestamp(open_time).substr(0, 19);
    std::replace(ts.begin(), ts.end(), ':', '-');

    auto key = cb::dek::Manager::instance().lookup(cb::dek::Entity::Audit);
    const auto* extension = key ? "cef" : "log";

    // If we for some reason need to rotate the file and there is already
    // a pre-existing file with the filename: hostname-timestamp-audit.log
    // lets use the name "hostname-timestamp-counter-audit.log".
    // In order for this to happen in production you would generate
    // so much audit data that you would rotate within the same second
    int count = 0;
    do {
        if (count == 0) {
            open_file_name =
                    log_directory /
                    fmt::format("{}-{}-audit.{}", hostname, ts, extension);
        } else {
            open_file_name = log_directory / fmt::format("{}-{}-{}-audit.{}",
                                                         hostname,
                                                         ts,
                                                         count,
                                                         extension);
        }
        ++count;
    } while (exists(open_file_name));

    try {
        file = cb::crypto::FileWriter::create(
                key, open_file_name, 8192, cb::crypto::Compression::ZLIB);
        LOG_INFO_CTX(
                "Audit file",
                {"encrypted", file->is_encrypted() ? "encrypted" : "plain"});
    } catch (const std::exception& exception) {
        LOG_WARNING("Audit: open error on file {}: {}",
                    open_file_name.generic_string(),
                    exception.what());
        return false;
    }

    try {
        remove_audit_link();
        create_symlink(std::filesystem::path{"."} / open_file_name.filename(),
                       log_directory / current_audit_link_name);
    } catch (const std::exception& e) {
        LOG_WARNING("Audit: Failed to create {} symbolic link: {}",
                    current_audit_link_name,
                    e.what());
    }

    open_time = auditd_time();
    return true;
}

void AuditFile::close_and_rotate_log() {
    Expects(file);
    const auto empty = file->size() == 0;
    file->flush();
    file.reset();
    open_time = 0;

    remove_audit_link();
    if (empty) {
        // no output written; just remove the file
        remove_file(open_file_name);
    }
}

void AuditFile::remove_audit_link(
        const std::optional<std::filesystem::path>& log_path) const {
    if (log_path) {
        remove_file(*log_path / current_audit_link_name);
    } else {
        remove_file(log_directory / current_audit_link_name);
    }
}

bool AuditFile::write_event_to_disk(const nlohmann::json& output) {
    try {
        file->write(output.dump());
        file->write("\n");
        if (!buffered) {
            return flush();
        }
        return true;
    } catch (const std::bad_alloc&) {
        LOG_WARNING_RAW(
                "Audit: memory allocation error for writing audit event to "
                "disk");
    } catch (const std::exception& exception) {
        LOG_WARNING("Audit: Failed to write event to disk: {}",
                    exception.what());
        close_and_rotate_log();
    }

    return false;
}

void AuditFile::set_log_directory(const std::string& new_directory) {
    if (log_directory == new_directory) {
        // No change
        return;
    }

    if (file != nullptr) {
        close_and_rotate_log();
    }

    log_directory = cb::io::makeExtendedLengthPath(new_directory);
    try {
        create_directories(log_directory);
    } catch (const std::runtime_error& error) {
        // The directory does not exist and we failed to create
        // it. This is not a fatal error, but it does mean that the
        // node won't be able to do any auditing
        LOG_WARNING(R"(Audit: failed to create audit directory "{}": {})",
                    new_directory,
                    error.what());
    }
}

bool AuditFile::is_empty() const {
    return !file || file->size() == 0;
}

void AuditFile::reconfigure(const AuditConfig& config) {
    rotate_interval = config.get_rotate_interval();
    set_log_directory(config.get_log_directory());
    max_log_size = config.get_rotate_size();
    buffered = config.is_buffered();
    prune_age = config.get_prune_age();
    next_prune = std::chrono::steady_clock::now();
}

bool AuditFile::flush() {
    if (is_open()) {
        try {
            file->flush();
        } catch (const std::exception& exception) {
            LOG_WARNING("Audit: writing to disk error: {}", exception.what());
            close_and_rotate_log();
            return false;
        }
    }

    return true;
}

void AuditFile::remove_file(const std::filesystem::path& path) {
    if (exists(path)) {
        try {
            remove(path);
        } catch (const std::exception& exception) {
            LOG_WARNING("Audit: Failed to remove \"{}\": {}",
                        path.generic_string(),
                        exception.what());
        }
    }
}
