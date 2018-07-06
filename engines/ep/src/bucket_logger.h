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

/**
 * EP Engine specific logger
 *
 * Prepends the engine name to log messages called via the defined macros using
 * an engine specific logger.
 *
 * Requires some indirection to do so as the format library in spdlog requires
 * type safety and we don't want to inline a lot of message formatting as
 * macros. One global BucketLogger object is created without sinks to perform
 * the spd style formatting without logging. Instead of "sinking" this message
 * with the BucketLogger we override the _sink_it method to prepend the
 * engine name and log the message as a pre-formatted string using the
 * original spdlog::logger passed via the SERVER_API.
 */
class BucketLogger : public spdlog::logger {
public:
    // Constructor taking a spdlog::logger that is used to perform the actual
    // logging after this BucketLogger formats the log messages
    BucketLogger(spdlog::logger* logger);

protected:
    void _sink_it(spdlog::details::log_msg& msg) override;

private:
    spdlog::logger* spdLogger;
};

extern std::unique_ptr<BucketLogger> globalBucketLogger;

#define EP_LOG_FMT(severity, ...)                           \
    do {                                                    \
        if (globalBucketLogger->should_log(severity)) {     \
            globalBucketLogger->log(severity, __VA_ARGS__); \
        }                                                   \
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
