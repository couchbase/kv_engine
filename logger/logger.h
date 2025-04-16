/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

#include <platform/json_log.h>
#include <spdlog/fmt/ostr.h>
#include <spdlog/logger.h>

#include <optional>
#include <string>
#include <unordered_set>

namespace cb::logger {

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
void createBlackholeLogger();

/**
 * Initialize the logger with the logger which logs to the console
 *
 * See note about thread safety at the top of the file
 *
 * @throws std::bad_alloc
 * @throws spdlog::spdlog_ex if an error occurs creating the logger
 */
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
const std::shared_ptr<spdlog::logger>& get();

/**
 * Reset the underlying logger object
 *
 * See note about thread safety at the top of the file
 */
void reset();

/**
 * Engines that create their own instances of an spdlogger should register
 * the logger here to ensure that the verbosity of the logger is updated when
 * memcached receives a request to update verbosity
 *
 * @param l spdlogger instance
 * @returns false on failure (exceptions are caught and logged)
 */
bool registerSpdLogger(std::shared_ptr<spdlog::logger> l);

/**
 * Engines that create their own instances of an spdlogger should unregister
 * the logger here to ensure that resources can be freed when their loggers
 * go out of scope, or unsubscribe from runtime verbosity changes
 *
 * @param n The name of the spdlogger
 */
void unregisterSpdLogger(const std::string& n);

/**
 * Check the log level of all spdLoggers is equal to the given level
 * @param log severity level
 * @return true if all registered loggers have the specified severity level
 */
bool checkLogLevels(spdlog::level::level_enum level);

/**
 * Set the log level of all registered spdLoggers
 * @param log severity level
 */
void setLogLevels(spdlog::level::level_enum level);

/**
 * Tell the logger to flush its buffers
 */
void flush();

/**
 * Tell the logger to shut down (flush buffers) and release _ALL_
 * loggers (you'd need to create new loggers after this method)
 */
void shutdown();

/**
 * @return whether or not the logger has been initialized
 */
bool isInitialized();

/**
 * Record a log message with additional context.
 * Format: <MSG> <JSON>
 *
 * NOTE: If present, the characters {} in the message are replaced by [].
 *
 * @param lvl The log level to report at
 * @param msg The message to log
 * @param ctx The context object
 * @throws std::invalid_argument if ctx is not an object (disabled in
 * production)
 */
void logWithContext(spdlog::logger& logger,
                    spdlog::level::level_enum lvl,
                    std::string_view msg,
                    Json ctx);

/// Iterate over the log files on disk and generate a list of the DEKs
/// in use in any of the files
std::unordered_set<std::string> getDeksInUse();

} // namespace cb::logger

// Visual Studio doesn't correctly handle the constexpr
// format string checking - see https://github.com/fmtlib/fmt/issues/2328.
// As such, only apply the compile-time check for non-MSVC
#ifdef WIN32
#define CHECK_FMT_STRING(fmt) fmt
#else
#define CHECK_FMT_STRING(fmt) FMT_STRING(fmt)
#endif

#define CB_LOG_ENTRY(severity, fmt, ...)                                 \
    do {                                                                 \
        auto& _logger_ = cb::logger::get();                              \
        if (_logger_ && _logger_->should_log(severity)) {                \
            _logger_->log(severity, CHECK_FMT_STRING(fmt), __VA_ARGS__); \
        }                                                                \
    } while (false)

#define CB_LOG_ENTRY_CTX(severity, msg, ...)                  \
    do {                                                      \
        auto& _logger_ = cb::logger::get();                   \
        if (_logger_ && _logger_->should_log(severity)) {     \
            ::cb::logger::logWithContext(                     \
                    *_logger_, severity, msg, {__VA_ARGS__}); \
        }                                                     \
    } while (false)

#define CB_LOG_RAW(severity, msg)                         \
    do {                                                  \
        auto& _logger_ = cb::logger::get();               \
        if (_logger_ && _logger_->should_log(severity)) { \
            _logger_->log(severity, msg);                 \
        }                                                 \
    } while (false)

#define LOG_WARNING(...) \
    CB_LOG_ENTRY(spdlog::level::level_enum::warn, __VA_ARGS__)
#define LOG_ERROR(...) CB_LOG_ENTRY(spdlog::level::level_enum::err, __VA_ARGS__)
#define LOG_CRITICAL(...) \
    CB_LOG_ENTRY(spdlog::level::level_enum::critical, __VA_ARGS__)

#define LOG_TRACE_CTX(msg, ...) \
    CB_LOG_ENTRY_CTX(spdlog::level::level_enum::trace, msg, __VA_ARGS__)
#define LOG_DEBUG_CTX(msg, ...) \
    CB_LOG_ENTRY_CTX(spdlog::level::level_enum::debug, msg, __VA_ARGS__)
#define LOG_INFO_CTX(msg, ...) \
    CB_LOG_ENTRY_CTX(spdlog::level::level_enum::info, msg, __VA_ARGS__)
#define LOG_WARNING_CTX(msg, ...) \
    CB_LOG_ENTRY_CTX(spdlog::level::level_enum::warn, msg, __VA_ARGS__)
#define LOG_ERROR_CTX(msg, ...) \
    CB_LOG_ENTRY_CTX(spdlog::level::level_enum::err, msg, __VA_ARGS__)
#define LOG_CRITICAL_CTX(msg, ...) \
    CB_LOG_ENTRY_CTX(spdlog::level::level_enum::critical, msg, __VA_ARGS__)

// Convenience macros which log with the given level, and message, if the given
// level is currently enabled.
// @param msg Fixed string (implicitly convertible to `const char*`), or type
//            which supports operator<<.
//
// For example:
//
//     LOG_INFO_RAW("Starting flusher");
//     LOG_INFO_RAW(std:string{...});
//
#define LOG_TRACE_RAW(msg) CB_LOG_RAW(spdlog::level::level_enum::trace, msg)
#define LOG_DEBUG_RAW(msg) CB_LOG_RAW(spdlog::level::level_enum::debug, msg)
#define LOG_INFO_RAW(msg) CB_LOG_RAW(spdlog::level::level_enum::info, msg)
#define LOG_WARNING_RAW(msg) CB_LOG_RAW(spdlog::level::level_enum::warn, msg)
#define LOG_ERROR_RAW(msg) CB_LOG_RAW(spdlog::level::level_enum::err, msg)
#define LOG_CRITICAL_RAW(msg) \
    CB_LOG_RAW(spdlog::level::level_enum::critical, msg)
