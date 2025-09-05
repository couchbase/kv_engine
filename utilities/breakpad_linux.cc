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

#include <sstream>
#include <string>

#include <client/linux/handler/exception_handler.h>

using namespace google_breakpad;

namespace cb::breakpad::internal {

// A path to use for writing data about a crash.
static std::string crashLogPath;

// dumpCallback is invoked within the signal handler and as such we are limited
// by POSIX Signal safety as to what can be used. spdlog cannot be called from
// inside the signal handler so we will write directly to stderr messages with
// no timestamp (cannot generate timestamps from a signal handler)
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
    int fd = *reinterpret_cast<int*>(ctx);
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

static bool dumpCallback(const MinidumpDescriptor& descriptor,
                         void* context,
                         bool succeeded) {
    auto writeFn = [&descriptor, succeeded](int fd) {
        if (fd == STDERR_FILENO) {
            WRITE_MSG(fd, CRITICAL_CAUGHT_CRASH_MSG);
        } else {
            WRITE_MSG(fd, CAUGHT_CRASH_MSG);
        }
        WRITE_CSTR(fd, descriptor.path());
        WRITE_MSG(fd, " before terminating. Writing dump succeeded: ");
        WRITE_CSTR(fd, succeeded ? "yes\n" : "no\n");

        if (!succeeded) {
            WRITE_MSG(fd,
                      MSG_CRITICAL "Breakpad failed to write crash dump!\n");
        }

        dump_stack(fd);
    };

    writeFn(STDERR_FILENO);

    int logFd =
            open(crashLogPath.c_str(), O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR);
    if (logFd < 0) {
        WRITE_MSG(STDERR_FILENO, FAILED_OPEN);
        WRITE_CSTR(STDERR_FILENO, crashLogPath.c_str());
        WRITE_MSG(STDERR_FILENO, "\n");
    } else {
        writeFn(logFd);
        auto rv = close(logFd);
        (void)rv; // If !success is ignored
    }

    return succeeded;
}

class LinuxBreakpadInstance : public BreakpadInstance {
public:
    LinuxBreakpadInstance(const std::string& minidump_dir,
                          const std::string& log_name) {
        MinidumpDescriptor descriptor(minidump_dir);
        handler =
                std::make_unique<ExceptionHandler>(descriptor,
                                                   nullptr, // filter
                                                   dumpCallback,
                                                   nullptr, // callback context
                                                   true, // install_handler
                                                   -1); // server_fg
        crashLogPath = log_name;
    }

    std::unique_ptr<ExceptionHandler> handler;
};

std::unique_ptr<BreakpadInstance> BreakpadInstance::create(
        const std::string& minidump_dir, const std::string& crash_log) {
    return std::make_unique<LinuxBreakpadInstance>(minidump_dir, crash_log);
}

} // namespace cb::breakpad::internal
