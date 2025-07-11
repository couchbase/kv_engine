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

#pragma once

#include "logger/prefix_logger.h"
#include <fmt/core.h>
#include <platform/json_log.h>
#include <spdlog/fmt/ostr.h>

class EventuallyPersistentEngine;

const std::string globalBucketLoggerName = "globalBucketLogger";

/**
 * EP Engine specific logger
 *
 * Prepends an integer connection ID (if set) at the very start of every
 * message.
 *
 * Prepends the engine name after the connection ID when called by a thread
 * associated with an engine.
 *
 * Prepends a given string prefix to log messages after the engine name.
 *
 * Overall message format is as follows:
 *
 * INFO 44: (default) {SpecifiedPrefix} {ActualLogMessage}
 *   |   |       |            |                  |--------|
 *   |   |       |            |                           |
 *   |   |       |            |-------------|             |
 *   |   |       |                          |             |
 *   |   |       |------------|             |             |
 *   |   |                    |             |             |
 *   |   |-------|            |             |             |
 *   |           |            |             |             |
 * LogLevel      ID      EngineName      Prefix      LogMessage
 *
 * Features
 * ========
 *
 * BucketLogger provides a log() method to allow callers to print log messages
 * using the same functionality as normal spdlog:
 *   1. const char* message
 *   2. Any printable type (supports stream operator<<)
 *   3. fmtlib-style message with variable arguments
 * It also provides convenience methods (debug(), info(), warn() etc) which
 * call log() at the given level.
 *
 * One global BucketLogger object is created to handle logging for all ep-engine
 * instances. It uses the thread-local currentEngine from ObjectRegistry to
 * determine the bucket name to prefix. This is sufficient to handle the
 * majority of logging uses.
 * To simplify usage (so the caller doesn't have to explicitly call on the
 * globalBucketLogger object) EP_LOG_xxx() macros are provided - this also
 * matches previous API to reduce code-churn in integrating spdlog.
 *
 * If a customised prefix is needed (for example for a DCP ActiveStream object
 * which wants to prefix all messages with the stream name) then explicit
 * instances of BucketLogger can be created with a custom prefix. These still
 * retrieve the engine name via ObjectRegistry::getCurrentEngine().
 *
 * Implementation
 * ==============
 *
 * BucketLogger:log() hides the parent class' log() method with its own, which
 * prepends the previously mentioned fields to the user's message, then calls
 * the log() method on the stored spdlog::logger to (print/sink) the message.
 *
 * BucketLoggers should be created using the create method to ensure each logger
 * is registered correctly. BucketLoggers must be registered to ensure that
 * their verbosity can be changed at runtime.
 */
class BucketLogger : public cb::logger::PrefixLogger {
public:
    /**
     * Record a log message for the bucket currently associated with the calling
     * thread.
     *
     * JSON Format: {"conn_id": <id>, "bucket": <name>, ...rest}
     *
     * @param lvl The log level to report at
     * @param msg The message to log
     * @param ctx The context object
     */
    void logWithContext(spdlog::level::level_enum lvl,
                        std::string_view msg,
                        cb::logger::Json ctx) override;

    /**
     * Creates a BucketLogger with the given name and then register it in the
     * spdlog registry within the logging library.
     *
     * @param name Registry name for the logger
     * @param prefix Optional prefix to be appended to every message
     */
    static std::shared_ptr<BucketLogger> createBucketLogger(
            const std::string& name);

    /// Set the connection id (printed before any other prefix or message)
    void setConnectionId(uint32_t id) {
        this->connectionId = id;
    }

protected:
    /**
     * Connection ID prefix that is printed if set (printed before any other
     * prefix or message)
     */
    uint32_t connectionId{0};

    /**
     * Constructors have restricted access as users should use the create
     * function to ensure that loggers are registered correctly.
     */

    /**
     * Constructor that assumes that we have already called the setLoggerAPI
     * method to store the spdlog::logger which is loaded once on creation to
     * set the member variable.
     *
     * This constructor should never be called, users should instead call the
     * BucketLogger::create method which will return a shared pointer to a
     * BucketLogger. This constructor is protected to allow for mocking.
     *
     * @param name Registry name for the logger
     */
    explicit BucketLogger(const std::string& name);
};

// Global (one instance shared across all ep-engine instances) BucketLogger
// declaration for use in EP_LOG macros.
// While "global", it will still print the selected bucket for the current
// thread (via ObjectRegistry::getCurrentEngine).
// This is a shared_ptr (not a unique_ptr as one might expect) as
// the spdlog registry only deals with weak_ptrs, and we must register each
// spdlogger we create to respect runtime verbosity changes
std::shared_ptr<BucketLogger>& getGlobalBucketLogger();

// Various implementation details for the EP_LOG_<level> macros below.
// End-users shouldn't use these directly, instead use EP_LOG_<level>.

// Visual Studio doens't correctly handle the constexpr
// format string checking - see https://github.com/fmtlib/fmt/issues/2328.
// As such, only apply the compile-time check for non-MSVC or VS 2019+
#if WIN32
#define CHECK_FMT_STRING(fmt) fmt
#else
#define CHECK_FMT_STRING(fmt) FMT_STRING(fmt)
#endif

#define EP_LOG_FMT(severity, fmt, ...)                                 \
    do {                                                               \
        auto& logger = getGlobalBucketLogger();                        \
        if (logger->should_log(severity)) {                            \
            logger->log(severity, CHECK_FMT_STRING(fmt), __VA_ARGS__); \
        }                                                              \
    } while (false)

#define EP_LOG_CTX(severity, msg, ...)                           \
    do {                                                         \
        auto& logger = getGlobalBucketLogger();                  \
        if (logger->should_log(severity)) {                      \
            logger->logWithContext(                              \
                    severity,                                    \
                    msg,                                         \
                    cb::logger::detail::context({__VA_ARGS__})); \
        }                                                        \
    } while (false)

#define EP_LOG_RAW(severity, msg)               \
    do {                                        \
        auto& logger = getGlobalBucketLogger(); \
        if (logger->should_log(severity)) {     \
            logger->log(severity, msg);         \
        }                                       \
    } while (false)

// Convenience macros which call globalBucketLogger->log() with the given level,
// format string and variadic arguments.
// @param fmt Format string in fmtlib style (https://fmt.dev)
// @param args Variable-length arguments, matching the number of placeholders
//             ({}) specified in the format string.
//
// For example:
//
//     EP_LOG_INFO("Starting flusher on bucket:{} at {}", bucketName, now);
//
// Due to the combination of compile-time checking of format string (which must
// be a string literal), these macros require both a format string and at least
// one argument - i.e. you can't just pass a single element such as:
//
//     EP_LOG_INFO("Fixed message")
//
// Instead, see the EP_LOG_<LEVEL>_R (Raw) macros below.
#define EP_LOG_TRACE(...) \
    EP_LOG_FMT(spdlog::level::level_enum::trace, __VA_ARGS__)

#define EP_LOG_DEBUG(...) \
    EP_LOG_FMT(spdlog::level::level_enum::debug, __VA_ARGS__)

#define EP_LOG_INFO(...) \
    EP_LOG_FMT(spdlog::level::level_enum::info, __VA_ARGS__)

#define EP_LOG_WARN(...) \
    EP_LOG_FMT(spdlog::level::level_enum::warn, __VA_ARGS__)

#define EP_LOG_ERR(...) EP_LOG_FMT(spdlog::level::level_enum::err, __VA_ARGS__)

#define EP_LOG_CRITICAL(...) \
    EP_LOG_FMT(spdlog::level::level_enum::critical, __VA_ARGS__)

#define EP_LOG_TRACE_CTX(msg, ...) \
    EP_LOG_CTX(spdlog::level::level_enum::trace, msg, __VA_ARGS__)

#define EP_LOG_DEBUG_CTX(msg, ...) \
    EP_LOG_CTX(spdlog::level::level_enum::debug, msg, __VA_ARGS__)

#define EP_LOG_INFO_CTX(msg, ...) \
    EP_LOG_CTX(spdlog::level::level_enum::info, msg, __VA_ARGS__)

#define EP_LOG_WARN_CTX(msg, ...) \
    EP_LOG_CTX(spdlog::level::level_enum::warn, msg, __VA_ARGS__)

#define EP_LOG_ERR_CTX(msg, ...) \
    EP_LOG_CTX(spdlog::level::level_enum::err, msg, __VA_ARGS__)

#define EP_LOG_CRITICAL_CTX(msg, ...) \
    EP_LOG_CTX(spdlog::level::level_enum::critical, msg, __VA_ARGS__)

// Convenience macros which call globalBucketLogger->log() with the given level,
// and message.
// @param msg Fixed string (implicitly convertible to `const char*`), or type
//            which supports operator<<.
//
// For example:
//
//     EP_LOG_INFO("Starting flusher");
//     EP_LOG_INFO(std:string{...});
//
#define EP_LOG_TRACE_RAW(msg) EP_LOG_RAW(spdlog::level::level_enum::trace, msg)
#define EP_LOG_DEBUG_RAW(msg) EP_LOG_RAW(spdlog::level::level_enum::debug, msg)
#define EP_LOG_INFO_RAW(msg) EP_LOG_RAW(spdlog::level::level_enum::info, msg)
#define EP_LOG_WARN_RAW(msg) EP_LOG_RAW(spdlog::level::level_enum::warn, msg)
#define EP_LOG_ERR_RAW(msg) EP_LOG_RAW(spdlog::level::level_enum::err, msg)
#define EP_LOG_CRITICAL_RAW(msg) \
    EP_LOG_RAW(spdlog::level::level_enum::critical, msg)
