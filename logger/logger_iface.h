/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/json_log.h>
#include <spdlog/logger.h>
#include <string_view>

namespace cb::logger {

class LoggerIface {
public:
    virtual ~LoggerIface() = default;

    /**
     * Record a log message with additional context.
     *
     * JSON Format: {"conn_id": <id>, "bucket": <name>, ...rest}
     *
     * @param lvl The log level to report at
     * @param msg The message to log
     * @param ctx The context object
     */
    virtual void logWithContext(spdlog::level::level_enum lvl,
                                std::string_view msg,
                                Json ctx) = 0;

    /**
     * Record a log message with a formatted string and arguments.
     *
     * @param lvl The log level to report at
     * @param fmt The format string to use (fmtlib style).
     * @param args Variable arguments to include in the format string.
     */
    virtual void logFormatted(spdlog::level::level_enum lvl,
                              fmt::string_view fmt,
                              fmt::format_args args) = 0;

    /**
     * Check if the logger should log a message at the given level.
     *
     * @param lvl The log level to check
     * @return true if the logger should log at the given level, false otherwise
     */
    virtual bool should_log(spdlog::level::level_enum lvl) const = 0;

    /**
     * Set the log level of the logger.
     *
     * @param lvl The log level to set
     */
    virtual void set_level(spdlog::level::level_enum lvl) = 0;

    /**
     * Flush the logger.
     */
    virtual void flush() = 0;

    /**
     * Get the log level of the logger.
     *
     * @return The log level
     */
    virtual spdlog::level::level_enum level() const = 0;

    /**
     * Get the name of the logger.
     *
     * @return The name of the logger
     */
    virtual const std::string& name() const = 0;

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

    void traceWithContext(std::string_view msg, Json ctx);

    void debugWithContext(std::string_view msg, Json ctx);

    void infoWithContext(std::string_view msg, Json ctx);

    void warnWithContext(std::string_view msg, Json ctx);

    void errorWithContext(std::string_view msg, Json ctx);

    void criticalWithContext(std::string_view msg, Json ctx);
};

} // namespace cb::logger

// Inline/template implementation of methods of the Logger class.

template <typename S, typename... Args>
void cb::logger::LoggerIface::log(spdlog::level::level_enum lvl,
                                  const S& fmt,
                                  Args&&... args) {
    if (!should_log(lvl)) {
        return;
    }

    logFormatted(lvl, fmt, fmt::make_format_args(args...));
}

template <typename... Args>
void cb::logger::LoggerIface::log(spdlog::level::level_enum lvl,
                                  const char* msg) {
    if (!should_log(lvl)) {
        return;
    }
    logFormatted(lvl, msg, {});
}

template <typename T>
void cb::logger::LoggerIface::log(spdlog::level::level_enum lvl, const T& msg) {
    if (!should_log(lvl)) {
        return;
    }

    logFormatted(lvl, "{}", fmt::make_format_args(msg));
}

template <typename... Args>
void cb::logger::LoggerIface::trace(const char* fmt, const Args&... args) {
    log(spdlog::level::trace, fmt, args...);
}

template <typename... Args>
void cb::logger::LoggerIface::debug(const char* fmt, const Args&... args) {
    log(spdlog::level::debug, fmt, args...);
}

template <typename... Args>
void cb::logger::LoggerIface::info(const char* fmt, const Args&... args) {
    log(spdlog::level::info, fmt, args...);
}

template <typename... Args>
void cb::logger::LoggerIface::warn(const char* fmt, const Args&... args) {
    log(spdlog::level::warn, fmt, args...);
}

template <typename... Args>
void cb::logger::LoggerIface::error(const char* fmt, const Args&... args) {
    log(spdlog::level::err, fmt, args...);
}

template <typename... Args>
void cb::logger::LoggerIface::critical(const char* fmt, const Args&... args) {
    log(spdlog::level::critical, fmt, args...);
}

template <typename T>
void cb::logger::LoggerIface::trace(const T& msg) {
    log(spdlog::level::trace, msg);
}

template <typename T>
void cb::logger::LoggerIface::debug(const T& msg) {
    log(spdlog::level::debug, msg);
}

template <typename T>
void cb::logger::LoggerIface::info(const T& msg) {
    log(spdlog::level::info, msg);
}

template <typename T>
void cb::logger::LoggerIface::warn(const T& msg) {
    log(spdlog::level::warn, msg);
}

template <typename T>
void cb::logger::LoggerIface::error(const T& msg) {
    log(spdlog::level::err, msg);
}

template <typename T>
void cb::logger::LoggerIface::critical(const T& msg) {
    log(spdlog::level::critical, msg);
}

inline void cb::logger::LoggerIface::traceWithContext(std::string_view msg,
                                                      cb::logger::Json ctx) {
    logWithContext(spdlog::level::trace, msg, std::move(ctx));
}

inline void cb::logger::LoggerIface::debugWithContext(std::string_view msg,
                                                      cb::logger::Json ctx) {
    logWithContext(spdlog::level::debug, msg, std::move(ctx));
}

inline void cb::logger::LoggerIface::infoWithContext(std::string_view msg,
                                                     cb::logger::Json ctx) {
    logWithContext(spdlog::level::info, msg, std::move(ctx));
}

inline void cb::logger::LoggerIface::warnWithContext(std::string_view msg,
                                                     cb::logger::Json ctx) {
    logWithContext(spdlog::level::warn, msg, std::move(ctx));
}

inline void cb::logger::LoggerIface::errorWithContext(std::string_view msg,
                                                      cb::logger::Json ctx) {
    logWithContext(spdlog::level::err, msg, std::move(ctx));
}

inline void cb::logger::LoggerIface::criticalWithContext(std::string_view msg,
                                                         cb::logger::Json ctx) {
    logWithContext(spdlog::level::critical, msg, std::move(ctx));
}
