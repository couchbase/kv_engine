//
// Copyright(c) 2015 Gabi Melman.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)
//

/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "custom_rotating_file_sink.h"
#include <cbcrypto/file_writer.h>
#include <dek/manager.h>
#include <gsl/gsl-lite.hpp>
#include <spdlog/details/file_helper.h>
#include <spdlog/details/fmt_helper.h>
#include <charconv>
#include <filesystem>
#include <optional>

/// The absolute maximum number of log files we can keep
constexpr std::size_t max_number_of_log_giles = 10000;

template <class Mutex>
custom_rotating_file_sink<Mutex>::custom_rotating_file_sink(
        const spdlog::filename_t& base_filename,
        std::size_t max_size,
        const std::string& log_pattern,
        std::size_t max_aggregated_size)
    : base_filename(base_filename),
      max_size(max_size),
      max_aggregated_size(max_aggregated_size),
      global_encryption_config_version(
              cb::dek::Manager::instance().getEntityGenerationCounter(
                      cb::dek::Entity::Logs)) {
    scanExistingLogFiles();
    formatter = std::make_unique<spdlog::pattern_formatter>(
            log_pattern, spdlog::pattern_time_type::local);
    file_writer = openFile();
}

/* In addition to the functionality of spdlog's rotating_file_sink,
 * this class adds hooks marking the start and end of a logfile.
 */
template <class Mutex>
void custom_rotating_file_sink<Mutex>::sink_it_(
        const spdlog::details::log_msg& msg) {
    spdlog::memory_buf_t formatted;
    formatter->format(msg, formatted);
    file_writer->write({formatted.data(), formatted.size()});

    // Is it time to wrap to the next file?
    if (file_writer->size() > max_size ||
        encryption_config_version != global_encryption_config_version->load()) {
        try {
            auto next = openFile();
            aggregated_size += file_writer->size();
            std::swap(file_writer, next);
        } catch (...) {
            // Keep on logging to this file, but try swap at the next
            // insert of data (didn't use the next file we need to
            // roll back the next_file_id to avoid getting a hole ;-)
            next_file_id--;
        }

        auto removeFile = [this](const std::filesystem::path& path) {
            std::error_code ec;
            if (exists(path, ec)) {
                const auto size = file_size(path, ec);
                if (remove(path, ec)) {
                    aggregated_size -= size;
                }
                return true;
            }
            return false;
        };

        // Remove old log entries if we have too much data (or too many files)
        while (aggregated_size > max_aggregated_size ||
               ((next_file_id - tail_file_id) > max_number_of_log_giles)) {
            // We can't (or at least shouldn't) remove the "current" log file
            if (tail_file_id == (next_file_id - 1)) {
                break;
            }

            std::filesystem::path path = createFilename(tail_file_id, false);
            if (!removeFile(path)) {
                // We didn't have an unencrypted version of the file;
                // try to remove an encrypted version of the file
                path = createFilename(tail_file_id, true);
                removeFile(path);
            }
            ++tail_file_id;
        }
    }
}

template <class Mutex>
void custom_rotating_file_sink<Mutex>::flush_() {
    file_writer->flush();
}
template <class Mutex>
std::filesystem::path custom_rotating_file_sink<Mutex>::createFilename(
        unsigned long id, bool encrypted) {
    return fmt::format(
            "{}.{:06}.{}", base_filename, id, encrypted ? "cef" : "txt");
}

std::optional<unsigned long> parseNumber(std::string_view view) {
    uint64_t ret;
    auto [ptr, ec] =
            std::from_chars(view.data(), view.data() + view.size(), ret);
    if (ec == std::errc()) {
        return ret;
    }
    (void)ptr;
    return 0;
}

template <class Mutex>
void custom_rotating_file_sink<Mutex>::scanExistingLogFiles() {
    const std::filesystem::path pattern = base_filename;
    const auto prefix = pattern.filename().string();
    std::error_code ec;
    bool found = false;
    for (const auto& p :
         std::filesystem::directory_iterator(pattern.parent_path(), ec)) {
        auto& path = p.path();
        const auto extension = path.extension().string();
        auto file = path.stem().string();
        if (!file.starts_with(prefix) ||
            (extension != ".txt" && extension != ".cef")) {
            continue;
        }

        const auto index = file.rfind('.');
        if (index != std::string::npos) {
            auto value = parseNumber(file.substr(index + 1));
            if (value.has_value()) {
                std::error_code error;
                const auto fsize = file_size(path, error);
                if (error == std::errc()) {
                    next_file_id = std::max(next_file_id, *value);
                    if (!found) {
                        tail_file_id = next_file_id;
                        found = true;
                    }
                    tail_file_id = std::min(tail_file_id, *value);
                    if (value > next_file_id) {
                        next_file_id = *value;
                    }
                    aggregated_size += fsize;
                }
            }
        }
    }
}

template <class Mutex>
std::unique_ptr<cb::crypto::FileWriter>
custom_rotating_file_sink<Mutex>::openFile() {
    using namespace cb::dek;
    encryption_config_version = global_encryption_config_version->load();
    auto dek = Manager::instance().lookup(Entity::Logs);
    std::filesystem::path path;
    std::filesystem::path other;
    do {
        path = createFilename(next_file_id, dek.get() != nullptr);
        other = createFilename(next_file_id, dek.get() == nullptr);
        ++next_file_id;
    } while (exists(path) || exists(other));

    return cb::crypto::FileWriter::create(dek, path);
}

template class custom_rotating_file_sink<std::mutex>;
template class custom_rotating_file_sink<spdlog::details::null_mutex>;
