/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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
#include "custom_rotating_file_sink.h"

#include "logger.h"
#include "logger_config.h"

#include <memcached/engine.h>
#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <chrono>
#include <cstdio>
static const std::string logger_name{"spdlog_file_logger"};

/**
 * Custom log pattern which the loggers will use.
 * This pattern is duplicated for some test cases. If you need to update it,
 * please also update in all relevant places.
 * TODO: Remove the duplication in the future, by (maybe) moving
 *       the const to a header file.
 */
static const std::string log_pattern{"%^%Y-%m-%dT%T.%f%z %l %v%$"};

/**
 * Instances of spdlog (async) file logger.
 * The files logger requires a rotating file sink which is manually configured
 * from the parsed settings.
 * The loggers act as a handle to the sinks. They do the processing of log
 * messages and send them to the sinks, which do the actual writing (to file,
 * to stream etc.) or further processing.
 */
static std::shared_ptr<spdlog::logger> file_logger;

LOGGER_PUBLIC_API
void cb::logger::flush() {
    if (file_logger) {
        file_logger->flush();
    }
}

LOGGER_PUBLIC_API
void cb::logger::shutdown() {
    // Force a flush (posts a message to the async logger if we are not in unit
    // test mode)
    flush();

    /**
     * This will drop all spdlog instances from the registry, and destruct the
     * thread pool which will post terminate message(s) (one per thread) to the
     * thread pool message queue. The calling thread will then block until all
     * thread pool workers have joined. This ensures that any messages queued
     * before shutdown is called will be flushed to disk. Any messages that are
     * queued after the final terminate message will not be logged.
     *
     * If the logger is running in unit test mode (synchronous) then this is a
     * no-op.
     */
    spdlog::details::registry::instance().shutdown();
    file_logger.reset();
}

LOGGER_PUBLIC_API
const bool cb::logger::isInitialized() {
    return file_logger != nullptr;
}

/**
 * Initialises the loggers. Called if the logger configuration is
 * specified in a separate settings object.
 */
std::optional<std::string> cb::logger::initialize(
        const Config& logger_settings) {
    auto fname = logger_settings.filename;
    auto buffersz = logger_settings.buffersize;
    auto cyclesz = logger_settings.cyclesize;

    if (getenv("CB_MAXIMIZE_LOGGER_CYCLE_SIZE") != nullptr) {
        cyclesz = 1024 * 1024 * 1024; // use up to 1 GB log file size
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
        //       |__ (color)__stderr_sink_mt = Send log messages to console
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
            auto stderrsink =
                    std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

            // Set the formatting pattern of this sink
            stderrsink->set_pattern(log_pattern);
            if (logger_settings.unit_test) {
                stderrsink->set_level(spdlog::level::trace);
            } else {
                stderrsink->set_level(spdlog::level::warn);
            }
            sink->add_sink(stderrsink);
        }

        spdlog::drop(logger_name);

        if (logger_settings.unit_test) {
            file_logger = std::make_shared<spdlog::logger>(logger_name, sink);
        } else {
            // Create the default thread pool for async logging
            spdlog::init_thread_pool(buffersz, 1);

            // Get the thread pool so that we can actually construct the
            // object with already created sinks...
            auto tp = spdlog::thread_pool();
            file_logger = std::make_shared<spdlog::async_logger>(
                    logger_name,
                    sink,
                    tp,
                    spdlog::async_overflow_policy::block);
        }

        file_logger->set_pattern(log_pattern);
        file_logger->set_level(logger_settings.log_level);

        // Set the flushing interval policy
        spdlog::flush_every(std::chrono::seconds(1));

        spdlog::register_logger(file_logger);
    } catch (const spdlog::spdlog_ex& ex) {
        std::string msg =
                std::string{"Log initialization failed: "} + ex.what();
        return std::optional<std::string>{msg};
    }
    return {};
}

spdlog::logger* cb::logger::get() {
    return file_logger.get();
}

LOGGER_PUBLIC_API
void cb::logger::reset() {
    spdlog::drop(logger_name);
    file_logger.reset();
}

void cb::logger::createBlackholeLogger() {
    // delete if already exists
    spdlog::drop(logger_name);

    file_logger = std::make_shared<spdlog::logger>(
            logger_name, std::make_shared<spdlog::sinks::null_sink_mt>());

    file_logger->set_level(spdlog::level::off);
    file_logger->set_pattern(log_pattern);

    spdlog::register_logger(file_logger);
}

void cb::logger::createConsoleLogger() {
    // delete if already exists
    spdlog::drop(logger_name);

    auto stderrsink = std::make_shared<spdlog::sinks::stderr_color_sink_st>();

    file_logger = std::make_shared<spdlog::logger>(logger_name, stderrsink);
    file_logger->set_level(spdlog::level::info);
    file_logger->set_pattern(log_pattern);

    spdlog::register_logger(file_logger);
}

LOGGER_PUBLIC_API
void cb::logger::registerSpdLogger(std::shared_ptr<spdlog::logger> l) {
    try {
        file_logger->debug("Registering logger {}", l->name());
        spdlog::register_logger(l);
    } catch (spdlog::spdlog_ex& e) {
        file_logger->warn(
                "Exception caught when attempting to register the "
                "logger {} in the spdlog registry. The verbosity of "
                "this logger cannot be changed at runtime. e.what()"
                "={}",
                l->name(),
                e.what());
    }
}

LOGGER_PUBLIC_API
void cb::logger::unregisterSpdLogger(const std::string& n) {
    spdlog::drop(n);
}

LOGGER_PUBLIC_API
bool cb::logger::checkLogLevels(spdlog::level::level_enum level) {
    bool correct = true;
    spdlog::apply_all([&](std::shared_ptr<spdlog::logger> l) {
        if (l->level() != level) {
            correct = false;
        }
    });
    return correct;
}

LOGGER_PUBLIC_API
void cb::logger::setLogLevels(spdlog::level::level_enum level) {
    // Apply the function to each registered spdlogger
    spdlog::apply_all([&](std::shared_ptr<spdlog::logger> l) {
        try {
            l->set_level(level);
        } catch (spdlog::spdlog_ex& e) {
            l->warn("Exception caught when attempting to change the verbosity "
                    "of logger {} to spdlog level {}. e.what()={}",
                    l->name(),
                    to_c_str(level),
                    e.what());
        }
    });

    flush();
}
