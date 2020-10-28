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

#include <platform/dirutils.h>
#include <spdlog/details/file_helper.h>
#include <spdlog/details/fmt_helper.h>

#include <memory>

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
      _current_size(0),
      _next_file_id(find_first_logfile_id(base_filename)) {
    formatter = std::make_unique<spdlog::pattern_formatter>(
            log_pattern, spdlog::pattern_time_type::local);
    _file_helper = openFile();
    _current_size = _file_helper->size(); // expensive. called only once
    addHook(openingLogfile);
}

/* In addition to the functionality of spdlog's rotating_file_sink,
 * this class adds hooks marking the start and end of a logfile.
 */
template <class Mutex>
void custom_rotating_file_sink<Mutex>::sink_it_(
        const spdlog::details::log_msg& msg) {
    _current_size += msg.raw.size();
    fmt::memory_buffer formatted;
    formatter->format(msg, formatted);
    _file_helper->write(formatted);

    // Is it time to wrap to the next file?
    if (_current_size > _max_size) {
        try {
            auto next = openFile();
            addHook(closingLogfile);
            std::swap(_file_helper, next);
            _current_size = _file_helper->size();
            addHook(openingLogfile);
        } catch (...) {
            // Keep on logging to the this file, but try swap at the next
            // insert of data (didn't use the next file we need to
            // roll back the next_file_id to avoid getting a hole ;-)
            _next_file_id--;
        }
    }
}

template <class Mutex>
void custom_rotating_file_sink<Mutex>::flush_() {
    _file_helper->flush();
}

/* Takes a message, formats it and writes it to file */
template <class Mutex>
void custom_rotating_file_sink<Mutex>::addHook(const std::string& hook) {
    spdlog::details::log_msg msg;
    msg.time = spdlog::details::os::now();
    msg.level = spdlog::level::info;

    // Append the hook to the msg
    spdlog::details::fmt_helper::append_str(hook, msg.raw);

    if (hook == openingLogfile) {
        spdlog::details::fmt_helper::append_str(
                std::string(_file_helper->filename().data(),
                            _file_helper->filename().size()),
                msg.raw);
    }
    fmt::memory_buffer formatted;
    formatter->format(msg, formatted);
    _current_size += formatted.size();

    _file_helper->write(formatted);
}

template <class Mutex>
std::unique_ptr<spdlog::details::file_helper>
custom_rotating_file_sink<Mutex>::openFile() {
    std::unique_ptr<spdlog::details::file_helper> ret =
            std::make_unique<spdlog::details::file_helper>();
    do {
        ret->open(fmt::format("{}.{:06}.txt", _base_filename, _next_file_id++));
    } while (ret->size() > _max_size);
    return ret;
}

template <class Mutex>
custom_rotating_file_sink<Mutex>::~custom_rotating_file_sink() {
    addHook(closingLogfile);
}

template class custom_rotating_file_sink<std::mutex>;
template class custom_rotating_file_sink<spdlog::details::null_mutex>;
