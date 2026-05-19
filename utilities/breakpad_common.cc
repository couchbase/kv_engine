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
#include <platform/backtrace.h>
#include <platform/dirutils.h>
#ifdef WIN32
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#else
#include <sys/fcntl.h>
#include <unistd.h>
#endif
namespace cb::breakpad {

static std::unique_ptr<internal::BreakpadInstance> handler;
static std::string crashLogPath;

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

    crashLogPath = logConfig.filename + ".breakpad.crash.txt";

    if (settings.enabled) {
        handler = internal::BreakpadInstance::create(settings.minidump_dir);
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

namespace internal {

#ifdef WIN32
#define write(fd, ptr, len) _write(fd, ptr, len)
#define close(fd) _close(fd)
#endif

// dumpCallback is invoked within the signal handler and as such we are limited
// by POSIX Signal safety as to what can be used. spdlog cannot be called from
// inside the signal handler so we will write directly to stderr messages with
// no timestamp (cannot generate timestamps from a signal handler).
// The write macros are swallowing the return value, if it's not success there's
// not much we can do.
#define WRITE_MSG(FD, MSG)                         \
    do {                                           \
        auto rv = write(FD, MSG, sizeof(MSG) - 1); \
        (void)rv;                                  \
    } while (0)

#define WRITE_CSTR(FD, CSTR)                     \
    do {                                         \
        auto rv = write(FD, CSTR, strlen(CSTR)); \
        (void)rv;                                \
    } while (0)

static void write_to_logger(void* ctx, const char* frame) {
    const auto fd = *static_cast<int*>(ctx);
    WRITE_MSG(fd, "   ");
    WRITE_CSTR(fd, frame);
    WRITE_MSG(fd, "\n");
}

/// @param fd The file descriptor to write to
static void dump_stack(int fd) {
    WRITE_MSG(fd, "Stack backtrace of crashed thread:\n");
    print_backtrace(write_to_logger, &fd);
}

#define MSG_CRITICAL "CRITICAL "
#define MSG1p1 "Breakpad caught a crash (Couchbase version "
#define MSG1p2 "). Writing crash dump to "
#define CAUGHT_CRASH_MSG MSG1p1 PRODUCT_VERSION MSG1p2
#define CRITICAL_CAUGHT_CRASH_MSG MSG_CRITICAL CAUGHT_CRASH_MSG
#define FAILED_OPEN MSG_CRITICAL "dumpCallback failed to open crashLogPath:"

static void do_log_crash_information(int fd,
                                     const char* minidump_file,
                                     bool success) {
    if (fd == STDERR_FILENO) {
        WRITE_MSG(fd, CRITICAL_CAUGHT_CRASH_MSG);
    } else {
        WRITE_MSG(fd, CAUGHT_CRASH_MSG);
    }
    WRITE_CSTR(fd, minidump_file);
    WRITE_MSG(fd, " before terminating. Writing dump succeeded: ");
    WRITE_CSTR(fd, success ? "yes\n" : "no\n");

    if (!success) {
        WRITE_MSG(fd, MSG_CRITICAL "Breakpad failed to write crash dump!\n");
    }

    dump_stack(fd);
}

void log_crash_information(const char* minidump_file, bool success) {
    do_log_crash_information(STDERR_FILENO, minidump_file, success);

    const auto logFd =
#ifdef WIN32
            _open(crashLogPath.c_str(), _O_CREAT | _O_TRUNC | _O_WRONLY);
#else
            open(crashLogPath.c_str(), O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR);
#endif
    if (logFd < 0) {
        WRITE_MSG(STDERR_FILENO, FAILED_OPEN);
        WRITE_CSTR(STDERR_FILENO, crashLogPath.c_str());
        WRITE_MSG(STDERR_FILENO, "\n");
    } else {
        do_log_crash_information(logFd, minidump_file, success);
        auto rv = close(logFd);
        (void)rv; // If !success is ignored
    }
}
} // namespace internal

} // namespace cb::breakpad
