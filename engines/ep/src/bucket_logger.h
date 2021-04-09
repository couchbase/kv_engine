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
#include <memcached/server_log_iface.h>
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
 * keep code simple we use only the registry within the logging library.
 */
class BucketLogger : public spdlog::logger {
public:
    /// Unregister this BucketLogger on destruction
    ~BucketLogger() override;

    /**
     * Record a log message for the bucket currently associated with the calling
     * thread. Log message will have the bucket name prepended (assuming a
     * bucket is currently selected).
     * @param lvl The log level to report at
     * @param fmt The format string to use (fmtlib style).
     * @param args Variable arguments to include in the format string.
     */
    template <typename... Args>
    void log(spdlog::level::level_enum lvl,
             const char* fmt,
             const Args&... args);
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
     * Informs the BucketLogger class of the current logging API.
     *
     * Creates the globalBucketLogger.
     */
    static void setLoggerAPI(ServerLogIface* api);

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
    void sink_it_(spdlog::details::log_msg& msg) override;

    /// Overriden flush_ method to flush via the ServerAPI logger.
    void flush_() override;

    template <typename... Args>
    void logInner(spdlog::level::level_enum lvl,
                  const char* fmt,
                  const Args&... args);
    template <typename T>
    void logInner(spdlog::level::level_enum lvl, const T& msg);

    /**
     * Helper function which prefixes the string with the name of the
     * specified engine, or "No Engine" if engine is null.
     */
    std::string prefixStringWithBucketName(
            const EventuallyPersistentEngine* engine, const char* fmt);

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

    /// Convenience function to obtain a pointer to the ServerLogIface
    static ServerLogIface* getServerLogIface();

private:
    /// Memcached logger API used to construct each instance of the BucketLogger
    static std::atomic<ServerLogIface*> loggerAPI;

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
extern std::shared_ptr<BucketLogger> globalBucketLogger;

// Convenience macros which call globalBucketLogger->log() with the given level
// and arguments.
#define EP_LOG_FMT(severity, ...)                           \
    do {                                                    \
        if (globalBucketLogger->should_log(severity)) {     \
            globalBucketLogger->log(severity, __VA_ARGS__); \
        }                                                   \
    } while (false)

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

#include "bucket_logger_impl.h"
