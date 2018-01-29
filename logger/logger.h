/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <logger/visibility.h>

#include <boost/optional/optional.hpp>
#include <cJSON.h>
#include <memcached/extension.h>
#include <memcached/server_api.h>
#include <spdlog/logger.h>

#include <string>

namespace cb {
namespace logger {

struct LOGGER_PUBLIC_API Config {
    Config() = default;
    explicit Config(const cJSON& json);

    bool operator==(const Config& other) const;
    bool operator!=(const Config& other) const;

    /// The base name of the log files (we'll append: .000000.txt where
    /// the numbers is a sequence counter. The higher the newer ;)
    std::string filename;
    /// 2 MB for the logging queue
    size_t buffersize = 2048 * 1024;
    /// 100 MB per cycled file
    size_t cyclesize = 100 * 1024 * 1024;
    /// time between forced flushes of the buffer
    size_t sleeptime = 60;
    /// if running in a unit test or not
    bool unit_test = false;
};

/**
 * Initialize the logger
 *
 * @param logger_settings the configuration for the logger
 * @param get_server_api function to retrieve the server API
 * @return optional error message if something goes wrong
 */
LOGGER_PUBLIC_API
boost::optional<std::string> initialize(const Config& logger_settings,
                                        GET_SERVER_API get_server_api);

/**
 * Get the underlying logger object
 */
LOGGER_PUBLIC_API
std::shared_ptr<spdlog::logger> get();

/**
 * Convert a log level as being used by the memcached logger
 * to spdlog's log levels
 *
 * @param sev The memcached severity level
 * @return The corresponding value in spdlog
 */
LOGGER_PUBLIC_API
spdlog::level::level_enum convertToSpdSeverity(EXTENSION_LOG_LEVEL sev);

} // namespace logger
} // namespace cb
