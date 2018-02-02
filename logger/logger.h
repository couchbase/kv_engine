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
    /// Should messages be passed on to the console via stderr
    bool console = true;
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
 * Initialize the logger with the blackhole logger object
 *
 * This method is intended to be used by unit tests which
 * don't need any output (but may call methods who tries
 * to fetch the logger)
 *
 * @throws std::bad_alloc
 * @throws spdlog::spdlog_ex if an error occurs creating the logger
 *                           (if it already exists for instance)
 */
LOGGER_PUBLIC_API
void createBlackholeLogger();

/**
 * Initialize the logger with the logger which logs to the console
 *
 * @throws std::bad_alloc
 * @throws spdlog::spdlog_ex if an error occurs creating the logger
 */
LOGGER_PUBLIC_API
void createConsoleLogger();

/**
 * Get the underlying logger object
 */
LOGGER_PUBLIC_API
std::shared_ptr<spdlog::logger> get();

LOGGER_PUBLIC_API
EXTENSION_LOGGER_DESCRIPTOR& getLoggerDescriptor();

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

#define CB_LOG_ENTRY(level, ...)               \
    do {                                       \
        cb::logger::get()->level(__VA_ARGS__); \
    } while (false)

#define CB_TRACE(...) CB_LOG_ENTRY(trace, __VA_ARGS__)
#define CB_DEBUG(...) CB_LOG_ENTRY(debug, __VA_ARGS__)
#define CB_INFO(...) CB_LOG_ENTRY(info, __VA_ARGS__)
#define CB_WARN(...) CB_LOG_ENTRY(warn, __VA_ARGS__)
#define CB_ERROR(...) CB_LOG_ENTRY(err, __VA_ARGS__)
#define CB_CRIT(...) CB_LOG_ENTRY(critical, __VA_ARGS__)
