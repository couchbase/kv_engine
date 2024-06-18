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
#include <platform/dirutils.h>
#include <spdlog/details/file_helper.h>
#include <spdlog/details/fmt_helper.h>

static unsigned long find_first_logfile_id(const std::string& basename) {
    unsigned long id = 0;

    auto files = cb::io::findFilesWithPrefix(basename);
    for (auto& file : files) {
        // the format of the name should be:
        // fnm.number.txt
        auto index = file.rfind(".txt");
        if (index == std::string::npos) {
            continue;
        }

        file.resize(index);
        index = file.rfind('.');
        if (index != std::string::npos) {
            try {
                unsigned long value = std::stoul(file.substr(index + 1));
                if (value > id) {
                    id = value;
                }
            } catch (...) {
                // Ignore
            }
        }
    }

    return id;
}

template <class Mutex>
custom_rotating_file_sink<Mutex>::custom_rotating_file_sink(
        const spdlog::filename_t& base_filename,
        std::size_t max_size,
        const std::string& log_pattern)
    : _base_filename(base_filename),
      _max_size(max_size),
      _next_file_id(find_first_logfile_id(base_filename)) {
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
    if (file_writer->size() > _max_size) {
        try {
            auto next = openFile();
            std::swap(file_writer, next);
        } catch (...) {
            // Keep on logging to this file, but try swap at the next
            // insert of data (didn't use the next file we need to
            // roll back the next_file_id to avoid getting a hole ;-)
            _next_file_id--;
        }
    }
}

template <class Mutex>
void custom_rotating_file_sink<Mutex>::flush_() {
    file_writer->flush();
}

template <class Mutex>
std::unique_ptr<cb::crypto::FileWriter>
custom_rotating_file_sink<Mutex>::openFile() {
    using namespace cb::dek;
    auto dek = Manager::instance().lookup(Entity::Logs);
    std::filesystem::path path;
    do {
        path = fmt::format("{}.{:06}.{}",
                           _base_filename,
                           _next_file_id++,
                           dek ? "cef" : "txt");
    } while (exists(path));

    return cb::crypto::FileWriter::create(dek, path);
}

template class custom_rotating_file_sink<std::mutex>;
template class custom_rotating_file_sink<spdlog::details::null_mutex>;
