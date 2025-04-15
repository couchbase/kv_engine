/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "custom_rotating_file_sink.h"
#include "logger.h"
#include "logger_config.h"

#include <cbcrypto/encrypted_file_header.h>
#include <cbcrypto/file_utilities.h>
#include <cbcrypto/file_writer.h>
#include <dek/manager.h>
#include <nlohmann/json.hpp>
#include <platform/cb_arena_malloc.h>
#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdio>
#include <iterator>
#include <stdexcept>

static const std::string logger_name{"spdlog_file_logger"};
static const std::string wrapper_logger_name{"spdlog_file_logger_wrapper"};

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
/**
 * A wrapper around the file_logger which extends the spdlog::logger interface
 * with additional context to the log messages.
 */
static std::shared_ptr<cb::logger::Logger> file_logger_wrapper;

static std::shared_ptr<std::filesystem::path> log_directory;
static std::shared_ptr<std::string> log_file_prefix;

cb::logger::Logger::Logger(const std::string& name,
                           std::shared_ptr<spdlog::logger> inner)
    : spdlog::logger(name, nullptr), baseLogger(inner) {
    // Take the logging level of the inner spdlog::logger so we don't format
    // anything unnecessarily.
    set_level(inner->level());
}

cb::logger::Logger::Logger(const std::string& name,
                           std::shared_ptr<cb::logger::Logger> inner)
    : Logger(name, inner->getSpdLogger()) {
}

void cb::logger::Logger::sink_it_(const spdlog::details::log_msg& msg) {
    baseLogger->log(msg.level, msg.payload);
}

void cb::logger::Logger::flush_() {
    baseLogger->flush();
}

void cb::logger::Logger::fixupContext(Json& ctx) const {
    if (ctx.is_null()) {
        ctx = Json::object();
        return;
    }
    if (!ctx.is_object()) {
#if CB_DEVELOPMENT_ASSERTS
        throw std::invalid_argument(fmt::format(
                "JSON context must be an object, not `{}`", ctx.dump()));
#else
        // In production, handle this case gracefully.
        ctx = Json{{"context", std::move(ctx)}};
#endif
    }
}

void cb::logger::Logger::logFormatted(spdlog::level::level_enum lvl,
                                      fmt::string_view fmt,
                                      fmt::format_args args) {
    fmt::memory_buffer msg;
    // Format the user-specified format string & args.
    fmt::vformat_to(std::back_inserter(msg), fmt, args);
    logWithContext(lvl, {msg.data(), msg.size()}, cb::logger::Json::object());
}

bool cb::logger::Logger::tryRegister() {
    registered = cb::logger::registerSpdLogger(getSpdLogger());
    return registered;
}

void cb::logger::Logger::unregister() {
    if (registered) {
        // Unregister the logger in the logger library registry
        cb::logger::unregisterSpdLogger(name());
        registered = false;
    }
}

bool cb::logger::Logger::isRegistered() const {
    return registered;
}

std::shared_ptr<spdlog::logger> cb::logger::Logger::getSpdLogger() {
    // Need to reinterpret the pointer as the spdlog::logger is a protected base
    // class and static_pointer_cast<> won't work.
    return std::reinterpret_pointer_cast<spdlog::logger>(shared_from_this());
}

void cb::logger::flush() {
    NoArenaGuard guard;
    if (file_logger) {
        file_logger->flush();
    }
}

void cb::logger::shutdown() {
    NoArenaGuard guard;

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
    file_logger.reset();
    file_logger_wrapper.reset();
    spdlog::details::registry::instance().shutdown();
}

bool cb::logger::isInitialized() {
    return file_logger != nullptr;
}

/**
 * Initialises the loggers. Called if the logger configuration is
 * specified in a separate settings object.
 */
std::optional<std::string> cb::logger::initialize(
        const Config& logger_settings) {
    NoArenaGuard guard;

    auto fname = logger_settings.filename;
    std::filesystem::path path = fname;
    log_directory = std::make_shared<std::filesystem::path>(path.parent_path());
    log_file_prefix = std::make_shared<std::string>(path.filename().string());
    auto buffersz = logger_settings.buffersize;
    auto cyclesz = logger_settings.cyclesize;
    auto max_aggregated_size = logger_settings.max_aggregated_size;

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
                    fname, cyclesz, log_pattern, max_aggregated_size);
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
                stderrsink->set_level(spdlog::level::err);
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

        spdlog::drop(wrapper_logger_name);
        file_logger_wrapper =
                std::make_shared<Logger>(wrapper_logger_name, file_logger);

        spdlog::register_logger(file_logger);
        file_logger_wrapper->tryRegister();
    } catch (const spdlog::spdlog_ex& ex) {
        std::string msg =
                std::string{"Log initialization failed: "} + ex.what();
        return std::optional<std::string>{msg};
    }
    return {};
}

const std::shared_ptr<cb::logger::Logger>& cb::logger::get() {
    return file_logger_wrapper;
}

std::vector<spdlog::sink_ptr>& cb::logger::getLoggerSinks() {
    return file_logger->sinks();
}

void cb::logger::reset() {
    NoArenaGuard guard;
    spdlog::drop(logger_name);
    spdlog::drop(wrapper_logger_name);
    file_logger.reset();
    file_logger_wrapper.reset();
}

void cb::logger::createBlackholeLogger() {
    NoArenaGuard guard;
    // delete if already exists
    spdlog::drop(logger_name);

    file_logger = std::make_shared<spdlog::logger>(
            logger_name, std::make_shared<spdlog::sinks::null_sink_mt>());

    file_logger->set_level(spdlog::level::off);
    file_logger->set_pattern(log_pattern);

    spdlog::drop(wrapper_logger_name);
    file_logger_wrapper =
            std::make_shared<Logger>(wrapper_logger_name, file_logger);

    spdlog::register_logger(file_logger);
    file_logger_wrapper->tryRegister();
}

void cb::logger::createConsoleLogger() {
    NoArenaGuard guard;
    // delete if already exists
    spdlog::drop(logger_name);

    auto stderrsink = std::make_shared<spdlog::sinks::stderr_color_sink_st>();

    file_logger = std::make_shared<spdlog::logger>(logger_name, stderrsink);
    file_logger->set_level(spdlog::level::info);
    file_logger->set_pattern(log_pattern);

    spdlog::drop(wrapper_logger_name);
    file_logger_wrapper =
            std::make_shared<Logger>(wrapper_logger_name, file_logger);

    spdlog::register_logger(file_logger);
    file_logger_wrapper->tryRegister();
}

bool cb::logger::registerSpdLogger(std::shared_ptr<spdlog::logger> l) {
    NoArenaGuard guard;
    try {
        file_logger->debug("Registering logger {}", l->name());
        spdlog::register_logger(l);
        return true;
    } catch (spdlog::spdlog_ex& e) {
        file_logger->warn(
                "Exception caught when attempting to register the "
                "logger {} in the spdlog registry. The verbosity of "
                "this logger cannot be changed at runtime. e.what()"
                "={}",
                l->name(),
                e.what());
        return false;
    }
}

void cb::logger::unregisterSpdLogger(const std::string& n) {
    NoArenaGuard guard;
    spdlog::drop(n);
}

bool cb::logger::checkLogLevels(spdlog::level::level_enum level) {
    NoArenaGuard guard;
    bool correct = true;
    spdlog::apply_all([&](std::shared_ptr<spdlog::logger> l) {
        if (l->level() != level) {
            correct = false;
        }
    });
    return correct;
}

void cb::logger::setLogLevels(spdlog::level::level_enum level) {
    NoArenaGuard guard;
    // Apply the function to each registered spdlogger
    spdlog::apply_all([&](std::shared_ptr<spdlog::logger> l) {
        try {
            l->set_level(level);
        } catch (spdlog::spdlog_ex& e) {
            l->warn("Exception caught when attempting to change the verbosity "
                    "of logger {} to spdlog level {}. e.what()={}",
                    l->name(),
                    to_short_c_str(level),
                    e.what());
        }
    });

    flush();
}

void cb::logger::Logger::logWithContext(spdlog::level::level_enum lvl,
                                        std::string_view msg,
                                        Json ctx) {
    fixupContext(ctx);

    // TODO: Consider checking that the message conforms to the conventions and
    // doesn't contain ".:()", starts with a capital, etc.
    if (!should_log(lvl)) {
        return;
    }

    std::string sanitized;
    if (msg.find_first_of("{}") != std::string::npos) {
        sanitized = msg;
        std::ranges::replace(sanitized, '{', '[');
        std::ranges::replace(sanitized, '}', ']');
        msg = sanitized;
    }

    // Remove trailing spaces from the message
    while (!msg.empty() && msg.back() == ' ') {
        msg.remove_suffix(1);
    }

    // We build up the log string here then pass the already-formatted
    // string down to spdlog directly, not using spdlog's formatting
    // functions.
    std::string_view formattedView{msg};
    fmt::memory_buffer formatted;
    if (!ctx.empty()) {
        fmt::format_to(std::back_inserter(formatted), "{} {}", msg, ctx);
        formattedView = {formatted.data(), formatted.size()};
    }

    spdlog::logger::log(lvl, formattedView);
}

std::unordered_set<std::string> cb::logger::getDeksInUse() {
    if (!log_directory || !log_file_prefix) {
        throw std::logic_error(
                "cb::logger::getDeksInUse(): The file logger must be "
                "initialized before calling getDeksInUse()");
    }

    bool unencrypted = false;
    auto deks = cb::crypto::findDeksInUse(
            *log_directory,
            [&unencrypted](const auto& path) {
                if (path.extension() == ".txt" &&
                    path.filename().string().rfind(*log_file_prefix, 0) == 0) {
                    unencrypted = true;
                    return false;
                }

                return path.extension() == ".cef" &&
                       path.filename().string().rfind(*log_file_prefix, 0) == 0;
            },
            [](auto message, const auto& ctx) {
                LOG_WARNING_CTX(message, ctx);
            });
    if (unencrypted) {
        deks.insert(cb::crypto::DataEncryptionKey::UnencryptedKeyId);
    }
    // Add the "current" key as it is always supposed to be "in use"
    auto& manager = cb::dek::Manager::instance();
    auto key = manager.lookup(cb::dek::Entity::Logs);
    if (key) {
        deks.insert(std::string{key->getId()});
    }
    return deks;
}
