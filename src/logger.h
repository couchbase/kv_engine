/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "config.h"

#include <memcached/extension.h>

#include <atomic>
#include <cstdarg>
#include <string>

/**
 * Simple logger class. Main features:
 *
 * - Common prefix for all messages emitted
 * - Set a minimum log level - any lower priority messages are dropped.
 */
class Logger {
public:
    Logger(const std::string& prefix_ = "");

    /* Log a message with the given severity.
     *
     * @param severity Severity of the log message
     * @param fmt printf-style format string and varargs
     */
    void log(EXTENSION_LOG_LEVEL severity, const char* fmt, ...) const CB_FORMAT_PRINTF(3, 4);

    /* va_list variant of the log() method.
     *
     * @param severity Severity of the log message
     * @param fmt printf-style format string
     * @param va_list Variable arguments as used by fmt.
     */
    void vlog(EXTENSION_LOG_LEVEL severity, const char* fmt, va_list va) const;

    // Informs the Logger class of the current logging API.
    static void setLoggerAPI(SERVER_LOG_API* api);

    // Informs the Logger class of a change in the global log level.
    static void setGlobalLogLevel(EXTENSION_LOG_LEVEL level);

    // Prefix to use for all messages.
    std::string prefix;

    // Minimum log level messages will be printed at.
    EXTENSION_LOG_LEVEL min_log_level;

private:
    // memcached (server) logger API which is actually used to output messages
    // to the underlying log file.
    static std::atomic<SERVER_LOG_API*> logger_api;

    // Global log level; any message less than this will not be output.
    static std::atomic<EXTENSION_LOG_LEVEL> global_log_level;
};
