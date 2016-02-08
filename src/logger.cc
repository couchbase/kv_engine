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

#include "logger.h"

#include "ep_engine.h"
#include "objectregistry.h"
#include "utility.h"
#include <cstdarg>
#include <mutex>

Logger::Logger(const std::string& prefix_)
    : prefix(prefix_),
      min_log_level(EXTENSION_LOG_DETAIL) {
}

void Logger::log(EXTENSION_LOG_LEVEL severity, const char* fmt, ...) const
{
    va_list va;
    va_start(va, fmt);
    vlog(severity, fmt, va);
    va_end(va);
}

void Logger::vlog(EXTENSION_LOG_LEVEL severity, const char* fmt, va_list va) const
{
    if (severity < min_log_level) {
        // Message isn't high enough priority for this logger.
        return;
    }

    if (severity < Logger::global_log_level) {
        // Message isn't high enough for global priority.
        return;
    }

    if (Logger::logger_api.load(std::memory_order_relaxed) == nullptr) {
        // Cannot log it without a valid logger api.
        return;
    }

    static EXTENSION_LOGGER_DESCRIPTOR* logger;
    if (logger == nullptr) {
        // This locking isn't really needed because get_logger will
        // always return the same address, but it'll keep thread sanitizer
        // and other tools from complaining ;-)
        static std::mutex mutex;
        std::lock_guard<std::mutex> guard(mutex);
        logger = Logger::logger_api.load(std::memory_order_relaxed)->get_logger();
        global_log_level.store(Logger::logger_api.load(std::memory_order_relaxed)->get_level(),
                               std::memory_order_relaxed);
    }

    EventuallyPersistentEngine *engine = ObjectRegistry::onSwitchThread(NULL, true);

    // Format the message into a buffer.
    char buffer[2048];
    int pos = 0;
    if (prefix.size() > 0) {
        pos = snprintf(buffer, sizeof(buffer), "%s ", prefix.c_str());
    }
    vsnprintf(buffer + pos, sizeof(buffer) - pos, fmt, va);

    // Log it.
    if (engine) {
        logger->log(severity, NULL, "(%s) %s", engine->getName().c_str(),
                    buffer);
    } else {
        logger->log(severity, NULL, "(No Engine) %s", buffer);
    }

    ObjectRegistry::onSwitchThread(engine);
}

void Logger::setLoggerAPI(SERVER_LOG_API* api) {
    Logger::logger_api.store(api, std::memory_order_relaxed);
}

void Logger::setGlobalLogLevel(EXTENSION_LOG_LEVEL level) {
    Logger::global_log_level.store(level, std::memory_order_relaxed);
}

std::atomic<SERVER_LOG_API*> Logger::logger_api;
std::atomic<EXTENSION_LOG_LEVEL> Logger::global_log_level;
