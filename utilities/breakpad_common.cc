/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "breakpad.h"
#include "terminate_handler.h"

#include <logger/logger.h>
#include <logger/logger_config.h>
#include <platform/dirutils.h>

namespace cb::breakpad {

static std::unique_ptr<internal::BreakpadInstance> handler;

void logCrashData(const std::filesystem::path& filename) {
    std::error_code ec;
    if (exists(filename, ec)) {
        LOG_CRITICAL_RAW("Detected previous crash");

        // OK to get a truncated input, 8K is plenty to capture most dumps and
        // get the CRITICAL messages into the log.
        constexpr size_t fileReadLimit = 8192;
        std::string data = cb::io::loadFile(filename, {}, fileReadLimit);
        auto ss = std::stringstream{data};
        constexpr int lineLimit = 70;
        int lineCount = 0;
        for (std::string line; std::getline(ss, line) && lineCount < lineLimit;
             lineCount++) {
            if (!line.empty()) {
                LOG_CRITICAL("{}", line);
            }
        }
        if (lineCount == lineLimit) {
            LOG_INFO_CTX("logCrashData reached line count limit",
                         {"limit", lineCount});
        }
        if (!remove(filename, ec)) {
            LOG_CRITICAL_CTX("Failed to remove crash log",
                             {"path", filename.generic_string()},
                             {"error", ec.message()});
        }
    }
}

void initialize(const cb::breakpad::Settings& settings,
                const cb::logger::Config& logConfig) {
    // We cannot actually change any of breakpad's settings once created, only
    // remove it and re-create with new settings.
    destroy();

    auto crashLogPath = logConfig.filename + ".breakpad.crash.txt";

    if (settings.enabled) {
        handler = internal::BreakpadInstance::create(settings.minidump_dir,
                                                     crashLogPath);
    }

    if (handler) {
        // Turn off the terminate handler's backtrace - otherwise we
        // just print it twice.
        set_terminate_handler_print_backtrace(false);
        LOG_INFO_CTX("Breakpad enabled", {"path", settings.minidump_dir});
    } else {
        // If breakpad is off, then at least print the backtrace via
        // terminate_handler.
        set_terminate_handler_print_backtrace(true);
        LOG_INFO_RAW("Breakpad disabled");
    }

    logCrashData(crashLogPath);
}

void destroy() {
    if (handler) {
        set_terminate_handler_print_backtrace(true);
    }
    handler.reset();
}

} // namespace cb::breakpad
