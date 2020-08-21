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

#include <logger/visibility.h>
#include <spdlog/fmt/ostr.h>
#include <spdlog/logger.h>

#include <optional>

#include <string>

namespace cb {
namespace logger {

struct Config;

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
std::optional<std::string> initialize(const Config& logger_settings);

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
 * Engines that create their own instances of an spdlogger should register
 * the logger here to ensure that the verbosity of the logger is updated when
 * memcached receives a request to update verbosity
 *
 * @param l spdlogger instance
 */
LOGGER_PUBLIC_API
void registerSpdLogger(std::shared_ptr<spdlog::logger> l);

/**
 * Engines that create their own instances of an spdlogger should unregister
 * the logger here to ensure that resources can be freed when their loggers
 * go out of scope, or unsubscribe from runtime verbosity changes
 *
 * @param n The name of the spdlogger
 */
LOGGER_PUBLIC_API
void unregisterSpdLogger(const std::string& n);

/**
 * Check the log level of all spdLoggers is equal to the given level
 * @param log severity level
 * @return true if all registered loggers have the specified severity level
 */
LOGGER_PUBLIC_API
bool checkLogLevels(spdlog::level::level_enum level);

/**
 * Set the log level of all registered spdLoggers
 * @param log severity level
 */
LOGGER_PUBLIC_API
void setLogLevels(spdlog::level::level_enum level);

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
LOGGER_PUBLIC_API bool isInitialized();

} // namespace logger
} // namespace cb

#define CB_LOG_ENTRY(severity, ...)                       \
    do {                                                  \
        auto _logger_ = cb::logger::get();                \
        if (_logger_ && _logger_->should_log(severity)) { \
            _logger_->log(severity, __VA_ARGS__);         \
        }                                                 \
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
