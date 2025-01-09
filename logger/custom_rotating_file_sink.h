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

#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/base_sink.h>
#include <atomic>
#include <filesystem>
#include <mutex>

namespace cb::crypto {
class FileWriter;
}

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
                              const std::string& log_pattern,
                              std::size_t max_aggregated_size);

protected:
    void sink_it_(const spdlog::details::log_msg& msg) override;
    void flush_() override;

private:
    std::filesystem::path createFilename(unsigned long id, bool encrypted);

    /// Scan the log directory and initialize:
    /// * next_file_id to contain the number for the next file we should
    ///   create
    /// * tail_file_id to contain the number for the oldest file we found
    /// * aggregated_size to contain the total size of log on disk
    void scanExistingLogFiles();

    /// Calculate the full filename to use the next time and open the new
    /// file
    std::unique_ptr<cb::crypto::FileWriter> openFile();

    const spdlog::filename_t base_filename;
    const std::size_t max_size;
    const std::size_t max_aggregated_size;
    std::unique_ptr<cb::crypto::FileWriter> file_writer;
    std::unique_ptr<spdlog::pattern_formatter> formatter;
    /// The next file we're going to open (not the current) should use
    /// this number
    unsigned long next_file_id = 0;
    /// The oldest file we've got use this number
    unsigned long tail_file_id = 0;
    /// The aggregated size of log we've got (on disk). It does not include
    /// the content of the current file
    std::size_t aggregated_size = 0;
    /// The version of the encryption data used when creating the current
    /// version of the file
    uint64_t encryption_config_version = 0;
    /// Pointer to the generation counter for the log DEK configuration
    /// to allow for checking if we need to rotate the file without having
    /// to acuire the lock for the log DEK configuration
    std::atomic_uint64_t* global_encryption_config_version;
};

using custom_rotating_file_sink_mt = custom_rotating_file_sink<std::mutex>;
using custom_rotating_file_sink_st =
        custom_rotating_file_sink<spdlog::details::null_mutex>;
