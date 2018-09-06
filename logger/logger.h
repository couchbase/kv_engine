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

/*
 *   A note on the thread safety of the logger API:
 *
 *   The API is thread safe unless the underlying logger object is changed
 * during runtime. This means some methods can only be safely called if the
 * caller guarantees no other threads exist and/or are calling the logging
 * functions.
 *
 *   The caveat being we should not change the underlying logger object during
 * run-time, the exception to this is during the initial memcached startup,
 * where we are running in a single thread at the point we switch from console
 * logging to file logging.
 */

#pragma once

#include "config.h"

#include <logger/visibility.h>
#include <spdlog/logger.h>

#include <boost/optional/optional_fwd.hpp>
#include <memcached/extension.h>

#include <string>

struct cJSON;

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
    /// if running in a unit test or not
    bool unit_test = false;
    /// Should messages be passed on to the console via stderr
    bool console = true;
    /// The default log level to initialize the logger to
    spdlog::level::level_enum log_level = spdlog::level::level_enum::info;
};

/**
 * Initialize the logger.
 *
 * The default level for the created logger is set to INFO
 *
 * See note about thread safety at the top of the file
 *
 * @param logger_settings the configuration for the logger
 * @return optional error message if something goes wrong
 */
LOGGER_PUBLIC_API
boost::optional<std::string> initialize(const Config& logger_settings);

/**
 * Initialize the logger with the blackhole logger object
 *
 * This method is intended to be used by unit tests which
 * don't need any output (but may call methods who tries
 * to fetch the logger)
 *
 * See note about thread safety at the top of the file
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
 * See note about thread safety at the top of the file
 *
 * @throws std::bad_alloc
 * @throws spdlog::spdlog_ex if an error occurs creating the logger
 */
LOGGER_PUBLIC_API
void createConsoleLogger();

/**
 * Get the underlying logger object
 *
 * See note about thread safety at the top of the file.
 *
 * This will return null if a logger has not been
 * initialized through one of the following:
 *
 * - initialize()
 * - createBlackholeLogger()
 * - createConsoleLogger()
 */
LOGGER_PUBLIC_API
spdlog::logger* get();

/**
 * Reset the underlying logger object
 *
 * See note about thread safety at the top of the file
 */
LOGGER_PUBLIC_API
void reset();

/**
 * Get a reference to a function that returns a reference to the memcached
 * spdlog::logger object
 *
 * @return Function pointer
 */
LOGGER_PUBLIC_API
EXTENSION_SPDLOG_GETTER& getSpdloggerRef();

/**
 * Tell the logger to flush its buffers
 */
LOGGER_PUBLIC_API
void flush();

/**
 * Tell the logger to shut down (flush buffers) and release _ALL_
 * loggers (you'd need to create new loggers after this method)
 */
LOGGER_PUBLIC_API
void shutdown();

/**
 * @return whether or not the logger has been initialized
 */
LOGGER_PUBLIC_API const bool isInitialized();

} // namespace logger
} // namespace cb

#define CB_LOG_ENTRY(severity, ...)               \
    do {                                          \
        auto _logger_ = cb::logger::get();        \
        if (_logger_->should_log(severity)) {     \
            _logger_->log(severity, __VA_ARGS__); \
        }                                         \
    } while (false)

#define LOG_TRACE(...) \
    CB_LOG_ENTRY(spdlog::level::level_enum::trace, __VA_ARGS__)
#define LOG_DEBUG(...) \
    CB_LOG_ENTRY(spdlog::level::level_enum::debug, __VA_ARGS__)
#define LOG_INFO(...) CB_LOG_ENTRY(spdlog::level::level_enum::info, __VA_ARGS__)
#define LOG_WARNING(...) \
    CB_LOG_ENTRY(spdlog::level::level_enum::warn, __VA_ARGS__)
#define LOG_ERROR(...) CB_LOG_ENTRY(spdlog::level::level_enum::err, __VA_ARGS__)
#define LOG_CRITICAL(...) \
    CB_LOG_ENTRY(spdlog::level::level_enum::critical, __VA_ARGS__)
