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

#include "spdlog/logger.h"
#include <memcached/server_log_iface.h>
#include <spdlog/fmt/ostr.h>

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
 * Requires some indirection to do so as the format library in spdlog requires
 * type safety and we don't want to inline a lot of message formatting as
 * macros. One global BucketLogger object is created without sinks to perform
 * the spd style formatting without logging. Instead of "sinking" this message
 * with the BucketLogger we override the _sink_it method to prepend the
 * engine name and log the message as a pre-formatted string using the
 * original spdlog::logger passed via the SERVER_API.
 *
 * BucketLoggers should be created using the create method to ensure each logger
 * is registered correctly. BucketLoggers must be registered to ensure that
 * their verbosity can be changed at runtime. Spdlog provides a registry which
 * we can use to do so, however one exists per dynamically linked library. To
 * keep code simple we use only the registry within the logging library.
 */
class BucketLogger : public spdlog::logger {
public:
    // Informs the BucketLogger class of the current logging API
    // Creates the globalBucketLogger
    static void setLoggerAPI(ServerLogIface* api);

    // Set the conection id (printed before any other prefix or message)
    void setConnectionId(uint32_t id) {
        this->connectionId = id;
    }

    // The prefix printed before the log message contents
    std::string prefix;

    // Creates a BucketLogger with the given name and then registers it in
    // the spdlog registry before returning
    static std::shared_ptr<BucketLogger> createBucketLogger(
            const std::string& name, const std::string& prefix = "");

protected:
    void _sink_it(spdlog::details::log_msg& msg) override;

    // Connection ID prefix that is printed if set (printed before any other
    // prefix or message)
    uint32_t connectionId{0};

    // Constructors have restricted access as users must use the create
    // function to ensure that loggers are registered correctly.

    // Constructor that assumes that we have already called the setLoggerAPI
    // method to store the spdlog::logger which is loaded once on creation to
    // set the member variable.
    // Protected to allow mocking
    BucketLogger(const std::string& name, const std::string& prefix = "");

private:
    // Copy constructor
    BucketLogger(const BucketLogger& other);

    // memcached logger API used to construct the non-global instances
    static std::atomic<ServerLogIface*> loggerAPI;

    spdlog::logger* spdLogger;
};

// Global BucketLogger declaration for use in macros
// This is a shared_ptr (not a unique_ptr as one might expect) as
// the spdlog registry only deals with shared_ptrs, and we must register each
// spdlogger we create to respect runtime verbosity changes
extern std::shared_ptr<BucketLogger> globalBucketLogger;

#define EP_LOG_FMT(severity, ...)                     \
    do {                                              \
        auto bucketLogger = globalBucketLogger.get(); \
        if (bucketLogger->should_log(severity)) {     \
            bucketLogger->log(severity, __VA_ARGS__); \
        }                                             \
    } while (false)

#define EP_LOG_TRACE(...)                                          \
    do {                                                           \
        EP_LOG_FMT(spdlog::level::level_enum::trace, __VA_ARGS__); \
    } while (false)

#define EP_LOG_DEBUG(...)                                          \
    do {                                                           \
        EP_LOG_FMT(spdlog::level::level_enum::debug, __VA_ARGS__); \
    } while (false)

#define EP_LOG_INFO(...)                                          \
    do {                                                          \
        EP_LOG_FMT(spdlog::level::level_enum::info, __VA_ARGS__); \
    } while (false)

#define EP_LOG_WARN(...)                                          \
    do {                                                          \
        EP_LOG_FMT(spdlog::level::level_enum::warn, __VA_ARGS__); \
    } while (false)

#define EP_LOG_CRITICAL(...)                                          \
    do {                                                              \
        EP_LOG_FMT(spdlog::level::level_enum::critical, __VA_ARGS__); \
    } while (false)
