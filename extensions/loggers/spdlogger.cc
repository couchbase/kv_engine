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
#include "extensions/protocol_extension.h"

#include <memcached/engine.h>
#include <memcached/extension.h>
#include <phosphor/phosphor.h>
#include <platform/processclock.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdio>

static SERVER_HANDLE_V1* sapi;
static EXTENSION_LOGGER_DESCRIPTOR descriptor;

static auto current_log_level = spdlog::level::warn;
static const auto stderr_output_level = spdlog::level::err;

/* Max suffix appended to the log file name.
 * The actual max no. of files is (max_files + 1), because the numbering starts
 * from the base file name (aka 0) eg. (file, file.1, ..., file.100)
 */
static size_t max_files = 100;

/* Custom log pattern which the loggers will use.
 * This pattern is duplicated for some test cases. If you need to update it,
 * please also update in all relevant places.
 * TODO: Remove the duplication in the future, by (maybe) moving
 *       the const to a header file.
 */
const std::string log_pattern{"%Y-%m-%dT%T.%fZ %l %v"};

static const spdlog::level::level_enum convertToSpdSeverity(
        EXTENSION_LOG_LEVEL sev) {
    using namespace spdlog::level;
    switch (sev) {
    case EXTENSION_LOG_DETAIL:
        return level_enum::trace;
    case EXTENSION_LOG_DEBUG:
        return level_enum::debug;
    case EXTENSION_LOG_INFO:
        return level_enum::info;
    case EXTENSION_LOG_NOTICE:
        return level_enum::warn;
    case EXTENSION_LOG_WARNING:
        return level_enum::err;
    case EXTENSION_LOG_FATAL:
        return level_enum::critical;
    }
    throw std::invalid_argument("Unknown severity level");
}

/*
 * Instances of spdlog (async) file logger and stderr logger.
 * The files logger requires a rotating file sink which is manually configured
 * from the parsed settings, while the sterr logger generates its own.
 * The loggers act as a handle to the sinks. They do the processing of log
 * messages and send them to the sinks, which do the actual writing (to file,
 * to stream etc.)
 */
static std::shared_ptr<spdlog::sinks::sink> rotating_file_sink;
static std::shared_ptr<spdlog::logger> file_logger;
static std::shared_ptr<spdlog::logger> stderr_logger;

/* Returns the name of the file logger */
static const char* get_name() {
    return file_logger->name().c_str();
}

/* Retrieves a message, applies formatting and then logs it to stderr and
 * to file, according to the severity.
 */
static void log(EXTENSION_LOG_LEVEL mcd_severity,
                const void* client_cookie,
                const char* fmt,
                ...) {
    const auto severity = convertToSpdSeverity(mcd_severity);

    // Skip any processing if message wouldn't be logged anyway
    if (severity < current_log_level && severity < stderr_output_level) {
        return;
    }

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
        // Send full message to stderr.
        if (severity >= stderr_output_level) {
            va_start(va, fmt);
            std::cerr << "Truncating big log message. Full message: ";
            vfprintf(stderr, fmt, va);
            std::cerr << std::endl;
            va_end(va);
        }

        // Crop message for logging
        const char cropped[] = " [cut]";
        snprintf(msg + (sizeof(msg) - sizeof(cropped)),
                 sizeof(cropped),
                 "%s",
                 cropped);
    } else {
        msg[len] = '\0';
    }

    stderr_logger->log(severity, msg);
    file_logger->log(severity, msg);
}

/* (Synchronously) flushes  all the messages in the loggers' queue
 * and dereferences the loggers.
 */
static void logger_shutdown(bool force) {
    spdlog::drop(file_logger->name());
    spdlog::drop(stderr_logger->name());

    file_logger.reset();
    stderr_logger.reset();
    rotating_file_sink.reset();
}

/* Updates current log level */
static void on_log_level(const void* cookie,
                         ENGINE_EVENT_TYPE type,
                         const void* event_data,
                         const void* cb_data) {
    if (sapi != NULL) {
        current_log_level = convertToSpdSeverity(sapi->log->get_level());
        file_logger->set_level(current_log_level);
    }
}

/* Initialises the loggers. Called if the logger configuration is
 * specified in a separate settings object.
 */
MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE file_logger_initialize(const LoggerConfig& logger_settings,
                                            GET_SERVER_API get_server_api) {
    sapi = get_server_api();
    if (sapi == nullptr) {
        return EXTENSION_FATAL;
    }

    auto fname = logger_settings.filename;
    auto buffersz = logger_settings.buffersize;
    auto cyclesz = logger_settings.cyclesize;
    auto sleeptime = logger_settings.sleeptime;

    if (getenv("CB_MINIMIZE_LOGGER_SLEEPTIME") != nullptr) {
        sleeptime = 1;
    }

    if (getenv("CB_MAXIMIZE_LOGGER_CYCLE_SIZE") != nullptr) {
        cyclesz = 1024 * 1024 * 1024; // use up to 1 GB log file size
    }

    if (getenv("CB_MAXIMIZE_LOGGER_BUFFER_SIZE") != nullptr) {
        buffersz = 8 * 1024 * 1024; // use an 8MB log buffer
    }

    try {
        rotating_file_sink = std::make_shared<custom_rotating_file_sink_mt>(
                fname, cyclesz, max_files, log_pattern);
        file_logger =
                spdlog::create_async("spdlog_file_logger",
                                     rotating_file_sink,
                                     buffersz,
                                     spdlog::async_overflow_policy::block_retry,
                                     nullptr,
                                     std::chrono::seconds(sleeptime));
        stderr_logger = spdlog::stderr_logger_mt("spdlog_stderr_logger");
    } catch (const spdlog::spdlog_ex& ex) {
        std::cerr << "Log initialization failed: " << ex.what() << std::endl;
        return EXTENSION_FATAL;
    }

    current_log_level = convertToSpdSeverity(sapi->log->get_level());

    file_logger->set_level(current_log_level);
    stderr_logger->set_level(stderr_output_level);
    spdlog::set_pattern(log_pattern);

    descriptor.get_name = get_name;
    descriptor.log = log;
    descriptor.shutdown = logger_shutdown;

    if (!sapi->extension->register_extension(EXTENSION_LOGGER, &descriptor)) {
        return EXTENSION_FATAL;
    }
    sapi->callback->register_callback(NULL, ON_LOG_LEVEL, on_log_level, NULL);

    return EXTENSION_SUCCESS;
}

/* Parses the logger configuration, if specified as an extension.
 * Calls initialize_file_logger, which performs the actual initialisation.
 */
MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE memcached_extensions_initialize(
        const char* config, GET_SERVER_API get_server_api) {
    char* fname = NULL;
    LoggerConfig logger_config;

    sapi = get_server_api();
    if (sapi == NULL) {
        return EXTENSION_FATAL;
    }

    if (config != NULL) {
        struct config_item items[6];
        int ii = 0;
        memset(&items, 0, sizeof(items));

        items[ii].key = "filename";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &fname;
        ++ii;

        items[ii].key = "buffersize";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &(logger_config.buffersize);
        ++ii;

        items[ii].key = "cyclesize";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &(logger_config.cyclesize);
        ++ii;

        items[ii].key = "sleeptime";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &(logger_config.sleeptime);
        ++ii;

        items[ii].key = "unit_test";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &(logger_config.unit_test);
        ++ii;

        items[ii].key = NULL;
        ++ii;
        cb_assert(ii == 6);

        if (sapi->core->parse_config(config, items, stderr) != ENGINE_SUCCESS) {
            return EXTENSION_FATAL;
        }

        if (fname != NULL) {
            logger_config.filename.assign(fname);
        }
    }
    cb_free(fname);

    return file_logger_initialize(logger_config, get_server_api);
}