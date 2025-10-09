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

#include "spdlog/logger.h"
#include <fmt/core.h>
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
 * On construction, a BucketLogger stores a pointer to the memcached
 * spdlog::logger that is responsible for (printing/sinking) to various outputs
 * (stderr, memcached.log with file rotation...) via various sinks.
 *
 * BucketLogger:log() hides the parent class' log() method with its own, which
 * prepends the previously mentioned fields to the user's message, then calls
 * the log() method on the stored spdlog::logger to (print/sink) the message.
 *
 * BucketLoggers should be created using the create method to ensure each logger
 * is registered correctly. BucketLoggers must be registered to ensure that
 * their verbosity can be changed at runtime. Spdlog provides a registry which
 * we can use to do so, however one exists per dynamically linked library. To
 * keep code simple we use only the registry within the logging library. As the
 * spdlog registry deals in shared_ptr<spdlog::logger>'s we can't rely on the
 * destructor of the BucketLogger to unregister the logger from the spdlog
 * registry on destruction of the copy held for our own purposes; as such we
 * must call unregister() on the BucketLogger before destruction to avoid
 * leaking the BucketLogger.
 */
class BucketLogger : public spdlog::logger {
public:
    /**
     * Record a log message for the bucket currently associated with the calling
     * thread. Log message will have the bucket name prepended (assuming a
     * bucket is currently selected).
     * @param lvl The log level to report at
     * @param fmt The format string to use (fmtlib style).
     * @param args Variable arguments to include in the format string.
     */
    template <typename S, typename... Args>
    void log(spdlog::level::level_enum lvl, const S& fmt, Args&&... args);

    template <typename... Args>
    void log(spdlog::level::level_enum lvl, const char* msg);
    template <typename T>
    void log(spdlog::level::level_enum lvl, const T& msg);

    /*
     * The following convenience functions simplify logging a message at the
     * named level (trace, debug, info, ...)
     */
    template <typename... Args>
    void trace(const char* fmt, const Args&... args);

    template <typename... Args>
    void debug(const char* fmt, const Args&... args);

    template <typename... Args>
    void info(const char* fmt, const Args&... args);

    template <typename... Args>
    void warn(const char* fmt, const Args&... args);

    template <typename... Args>
    void error(const char* fmt, const Args&... args);

    template <typename... Args>
    void critical(const char* fmt, const Args&... args);

    template <typename T>
    void trace(const T& msg);

    template <typename T>
    void debug(const T& msg);

    template <typename T>
    void info(const T& msg);

    template <typename T>
    void warn(const T& msg);

    template <typename T>
    void error(const T& msg);

    template <typename T>
    void critical(const T& msg);

    /**
     * Creates a BucketLogger with the given name and then register it in the
     * spdlog registry within the logging library.
     *
     * @param name Registry name for the logger
     * @param prefix Optional prefix to be appended to every message
     */
    static std::shared_ptr<BucketLogger> createBucketLogger(
            const std::string& name, const std::string& prefix = "");

    /// Set the connection id (printed before any other prefix or message)
    void setConnectionId(uint32_t id) {
        this->connectionId = id;
    }

    /// The prefix printed before the log message contents
    std::string prefix;

    /// Unregisters the BucketLogger in the logger library registry.
    void unregister();

protected:
    /// Overriden sink_it_ method to log via ServerAPI logger.
    void sink_it_(const spdlog::details::log_msg& msg) override;

    /// Overriden flush_ method to flush via the ServerAPI logger.
    void flush_() override;

    void logInner(spdlog::level::level_enum lvl,
                  fmt::string_view fmt,
                  fmt::format_args args);

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
     * @param prefix Optional prefix to be appended to every message
     */
    explicit BucketLogger(const std::string& name, std::string prefix = "");

private:
    /**
     * Pointer to the underlying spdlogger within the logging library. This
     * logger will log messages to various sinks after we format it.
     */
    spdlog::logger* spdLogger;
};

// Global BucketLogger declaration for use in macros
// This is a shared_ptr (not a unique_ptr as one might expect) as
// the spdlog registry only deals with weak_ptrs, and we must register each
// spdlogger we create to respect runtime verbosity changes
std::shared_ptr<BucketLogger>& getGlobalBucketLogger();

// Various implementation details for the EP_LOG_<level> macros below.
// End-users shouldn't use these directly, instead use EP_LOG_<level>.

// Visual Studio prior to 2019 doens't correctly handle the constexpr
// format string checking - see https://github.com/fmtlib/fmt/issues/2328.
// As such, only apply the compile-time check for non-MSVC or VS 2019+
#if FMT_MSC_VER && FMT_MSC_VER < 1920
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

template <typename S, typename... Args>
void BucketLogger::log(spdlog::level::level_enum lvl,
                       const S& fmt,
                       Args&&... args) {
    if (!should_log(lvl)) {
        return;
    }

#if FMT_VERSION < 100000
    logInner(lvl, fmt, fmt::make_args_checked<Args...>(fmt, args...));
#else
    logInner(lvl, fmt, fmt::make_format_args(args...));
#endif
}

template <typename... Args>
void BucketLogger::log(spdlog::level::level_enum lvl, const char* msg) {
    if (!should_log(lvl)) {
        return;
    }
    logInner(lvl, msg, {});
}

template <typename T>
void BucketLogger::log(spdlog::level::level_enum lvl, const T& msg) {
    if (!should_log(lvl)) {
        return;
    }

#if FMT_VERSION < 100000
    logInner(lvl, "{}", fmt::make_args_checked<T>("{}", msg));
#else
    logInner(lvl, "{}", fmt::make_format_args(msg));
#endif
}

template <typename... Args>
void BucketLogger::trace(const char* fmt, const Args&... args) {
    log(spdlog::level::trace, fmt, args...);
}

template <typename... Args>
void BucketLogger::debug(const char* fmt, const Args&... args) {
    log(spdlog::level::debug, fmt, args...);
}

template <typename... Args>
void BucketLogger::info(const char* fmt, const Args&... args) {
    log(spdlog::level::info, fmt, args...);
}

template <typename... Args>
void BucketLogger::warn(const char* fmt, const Args&... args) {
    log(spdlog::level::warn, fmt, args...);
}

template <typename... Args>
void BucketLogger::error(const char* fmt, const Args&... args) {
    log(spdlog::level::err, fmt, args...);
}

template <typename... Args>
void BucketLogger::critical(const char* fmt, const Args&... args) {
    log(spdlog::level::critical, fmt, args...);
}

template <typename T>
void BucketLogger::trace(const T& msg) {
    log(spdlog::level::trace, msg);
}

template <typename T>
void BucketLogger::debug(const T& msg) {
    log(spdlog::level::debug, msg);
}

template <typename T>
void BucketLogger::info(const T& msg) {
    log(spdlog::level::info, msg);
}

template <typename T>
void BucketLogger::warn(const T& msg) {
    log(spdlog::level::warn, msg);
}

template <typename T>
void BucketLogger::error(const T& msg) {
    log(spdlog::level::err, msg);
}

template <typename T>
void BucketLogger::critical(const T& msg) {
    log(spdlog::level::critical, msg);
}
