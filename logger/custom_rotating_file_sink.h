//
// Copyright(c) 2015 Gabi Melman.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)
//

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

#pragma once

#include <mutex>

#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/base_sink.h>

namespace spdlog {
namespace details {
class file_helper;
} // namespace details
} // namespace spdlog

/**
 * Customised version of spdlog's rotating_file_sink with the following
 * modifications:
 *
 * * Instead of renaming all the files every time we're rotating to
 *   the next file we start a new log file with a higher number
 */
template <class Mutex>
class custom_rotating_file_sink : public spdlog::sinks::base_sink<Mutex> {
public:
    custom_rotating_file_sink(const spdlog::filename_t& base_filename,
                              std::size_t max_size,
                              const std::string& log_pattern);

    ~custom_rotating_file_sink() override;

protected:
    void sink_it_(const spdlog::details::log_msg& msg) override;
    void flush_() override;

private:
    // Calculate the full filename to use the next time
    std::unique_ptr<spdlog::details::file_helper> openFile();

    const spdlog::filename_t _base_filename;
    const std::size_t _max_size;
    std::size_t _current_size;
    std::unique_ptr<spdlog::details::file_helper> _file_helper;
    std::unique_ptr<spdlog::pattern_formatter> formatter;
    unsigned long _next_file_id;
};

using custom_rotating_file_sink_mt = custom_rotating_file_sink<std::mutex>;
using custom_rotating_file_sink_st =
        custom_rotating_file_sink<spdlog::details::null_mutex>;
