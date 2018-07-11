/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "config.h"

#include "custom_rotating_file_sink.h"

#include "logger.h"

#include <memcached/engine.h>
#include <memcached/extension.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdio>

#ifndef WIN32
#include <spdlog/sinks/ansicolor_sink.h>
#endif

static const std::string logger_name{"spdlog_file_logger"};

static EXTENSION_LOGGER_DESCRIPTOR descriptor;

/**
 * Custom log pattern which the loggers will use.
 * This pattern is duplicated for some test cases. If you need to update it,
 * please also update in all relevant places.
 * TODO: Remove the duplication in the future, by (maybe) moving
 *       the const to a header file.
 */
static const std::string log_pattern{"%Y-%m-%dT%T.%fZ %l %v"};

spdlog::level::level_enum cb::logger::convertToSpdSeverity(
        EXTENSION_LOG_LEVEL sev) {
    using namespace spdlog::level;
    switch (sev) {
    case EXTENSION_LOG_TRACE:
        return level_enum::trace;
    case EXTENSION_LOG_DEBUG:
        return level_enum::debug;
    case EXTENSION_LOG_INFO:
        return level_enum::info;
    case EXTENSION_LOG_NOTICE:
        return level_enum::info;
    case EXTENSION_LOG_WARNING:
        return level_enum::warn;
    case EXTENSION_LOG_FATAL:
        return level_enum::critical;
    }
    throw std::invalid_argument("Unknown severity level");
}

/**
 * Instances of spdlog (async) file logger.
 * The files logger requires a rotating file sink which is manually configured
 * from the parsed settings.
 * The loggers act as a handle to the sinks. They do the processing of log
 * messages and send them to the sinks, which do the actual writing (to file,
 * to stream etc.) or further processing.
 */
static std::shared_ptr<spdlog::logger> file_logger;

/**
 * Retrieves a message, applies formatting and then logs it to stderr and
 * to file, according to the severity.
 */
static void log(EXTENSION_LOG_LEVEL mcd_severity,
                const void* client_cookie,
                const char* fmt,
                ...) {
    const auto severity = cb::logger::convertToSpdSeverity(mcd_severity);

    // Retrieve formatted log message
    char msg[2048];
    int len;
    va_list va;
    va_start(va, fmt);
    len = vsnprintf(msg, 2048, fmt, va);
    va_end(va);

    // Something went wrong during formatting, so return
    if (len < 0) {
        return;
    }
    // len does not include '\0', hence >= and not >
    if (len >= int(sizeof(msg))) {
        // Crop message for logging
        const char cropped[] = " [cut]";
        snprintf(msg + (sizeof(msg) - sizeof(cropped)),
                 sizeof(cropped),
                 "%s",
                 cropped);
    } else {
        msg[len] = '\0';
    }

    file_logger->log(severity, msg);
}

LOGGER_PUBLIC_API
void cb::logger::flush() {
    if (file_logger) {
        file_logger->flush();
    }
}

LOGGER_PUBLIC_API
void cb::logger::shutdown() {
    flush();
    file_logger.reset();
    spdlog::drop_all();
}

LOGGER_PUBLIC_API
const bool cb::logger::isInitialized() {
    return file_logger != nullptr;
}

/**
 * Initialises the loggers. Called if the logger configuration is
 * specified in a separate settings object.
 */
boost::optional<std::string> cb::logger::initialize(
        const Config& logger_settings) {
    auto fname = logger_settings.filename;
    auto buffersz = logger_settings.buffersize;
    auto cyclesz = logger_settings.cyclesize;

    if (getenv("CB_MAXIMIZE_LOGGER_CYCLE_SIZE") != nullptr) {
        cyclesz = 1024 * 1024 * 1024; // use up to 1 GB log file size
    }

    if (getenv("CB_MAXIMIZE_LOGGER_BUFFER_SIZE") != nullptr) {
        buffersz = 8 * 1024 * 1024; // use an 8MB log buffer
    }

    try {
        // Initialise the loggers.
        //
        // The structure is as follows:
        //
        // file_logger = sends log messages to sink
        //   |__dist_sink_mt = Distribute log messages to multiple sinks
        //       |     |__custom_rotating_file_sink_mt = adds opening & closing
        //       |                                       hooks to the file
        //       |__ (color)__stderr_sink_mt = Send log messages to consloe
        //
        // When a new log message is being submitted to the file_logger it
        // is subject to the log level specified on the file_logger. If it
        // is to be included it is passed down to the dist_sink which will
        // evaluate if the message should be passed on based on its log level.
        // It'll then try to pass the message to the file sink and the
        // console sink and they will evaluate if the message should be
        // logged or not. This means that we should set the file sink
        // loglevel to TRACE so that all messages which goes all the way
        // will end up in the file. Due to the fact that ns_server can't
        // keep up with the rate we might produce log we want the console
        // sink to drop everything below WARNING (unless we're running
        // unit tests (through testapp).
        //
        // When the user change the verbosity level we'll modify the
        // level for the file_logger object causing it to allow more
        // messages to go down to the various sinks.

        auto sink = std::make_shared<spdlog::sinks::dist_sink_mt>();
        sink->set_level(spdlog::level::trace);

        if (!fname.empty()) {
            auto fsink = std::make_shared<custom_rotating_file_sink_mt>(
                    fname, cyclesz, log_pattern);
            fsink->set_level(spdlog::level::trace);
            sink->add_sink(fsink);
        }

        if (logger_settings.console) {
#ifdef WIN32
            auto stderrsink = std::make_shared<spdlog::sinks::stderr_sink_mt>();
#else
            auto stderrsink =
                    std::make_shared<spdlog::sinks::ansicolor_stderr_sink_mt>();
#endif
            if (logger_settings.unit_test) {
                stderrsink->set_level(spdlog::level::trace);
            } else {
                stderrsink->set_level(spdlog::level::warn);
            }
            sink->add_sink(stderrsink);
        }

        spdlog::drop(logger_name);
        if (logger_settings.unit_test) {
            file_logger = spdlog::create(logger_name, sink);
        } else {
            file_logger = spdlog::create_async(
                    logger_name,
                    sink,
                    buffersz,
                    spdlog::async_overflow_policy::block_retry,
                    nullptr,
                    std::chrono::milliseconds(200));
        }
    } catch (const spdlog::spdlog_ex& ex) {
        std::string msg =
                std::string{"Log initialization failed: "} + ex.what();
        return boost::optional<std::string>{msg};
    }

    file_logger->set_pattern(log_pattern);
    file_logger->set_level(logger_settings.log_level);
    return {};
}

spdlog::logger* cb::logger::get() {
    return file_logger.get();
}

void cb::logger::reset() {
    file_logger.reset();
}

void cb::logger::createBlackholeLogger() {
    // delete if already exists
    spdlog::drop(logger_name);

    file_logger = spdlog::create(
            logger_name, std::make_shared<spdlog::sinks::null_sink_mt>());

    file_logger->set_level(spdlog::level::off);
    file_logger->set_pattern(log_pattern);
}

void cb::logger::createConsoleLogger() {
    // delete if already exists
    spdlog::drop(logger_name);
    file_logger = spdlog::stderr_color_mt(logger_name);
    file_logger->set_level(spdlog::level::info);
    file_logger->set_pattern(log_pattern);
}

LOGGER_PUBLIC_API
EXTENSION_LOGGER_DESCRIPTOR& cb::logger::getLoggerDescriptor() {
    descriptor.log = log;
    return descriptor;
}
