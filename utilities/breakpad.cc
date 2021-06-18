/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
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

#if defined(WIN32) && defined(HAVE_BREAKPAD)

/*
 * For some unknown reason, Folly has decided to undefine some Windows types
 * that are required by "dbghelp.h", which is included in Breakpad's Windows
 * "exception_handler.h". Nobody bothered to document why exactly they were
 * undefined, and it looks as though they are not, and never were, used within
 * the Folly codebase. So, if we have already pulled in Folly's "Windows.h" at
 * this point, then we need to re-define OUT and IN. To avoid any potential
 * issues with Folly, we'll undefine them after we've included Breakpad's
 * "exception_handler.h". If we haven't pulled in Folly's "Windows.h", then
 * Breakpad will pull in it's own, but it won't undefine OUT and IN so there
 * won't be an issue.
 */
#ifdef _WINDOWS_
#ifndef OUT
#define UNDEFOUT 1
#define OUT
#endif

#ifndef IN
#define UNDEFIN 1
#define IN
#endif
#endif

#include "client/windows/handler/exception_handler.h"

#ifdef UNDEFOUT
#undef OUT
#endif

#ifdef UNDEFIN
#undef IN
#endif

#elif defined(linux) && defined(HAVE_BREAKPAD)
#include "client/linux/handler/exception_handler.h"
#else
namespace google_breakpad {
class ExceptionHandler {
public:
};
} // namespace google_breakpad
#endif

using namespace google_breakpad;

// Unique_ptr which holds the pointer to the installed
// breakpad handler
static std::unique_ptr<ExceptionHandler> handler;

// A path to use for writing data about a crash.
static std::string crashLogPath;

#if defined(WIN32) && defined(HAVE_BREAKPAD)
// These methods are called from breakpad when creating
// the dump. They're inside the #ifdef block to avoid
// compilers to complain about static functions never
// being used.

static void write_to_logger(void* ctx, const char* frame) {
    LOG_CRITICAL("    {}", frame);
}

static void dump_stack() {
    LOG_CRITICAL_RAW("Stack backtrace of crashed thread:");
    print_backtrace(write_to_logger, nullptr);
    cb::logger::flush();
}

#elif defined(linux) && defined(HAVE_BREAKPAD)

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
#endif

// Unfortunately Breakpad use a different API on each platform,
// so we need a bit of #ifdef's..

#if defined(WIN32) && defined(HAVE_BREAKPAD)
static bool dumpCallback(const wchar_t* dump_path,
                         const wchar_t* minidump_id,
                         void* context,
                         EXCEPTION_POINTERS* exinfo,
                         MDRawAssertionInfo* assertion,
                         bool succeeded) {
    // Unfortunately the filenames is in wchar's and I got compiler errors
    // from fmt when trying to print them by using {}. Let's just format
    // it into a string first.
    char file[512];
    sprintf(file, "%S\\%S.dmp", dump_path, minidump_id);

    LOG_CRITICAL(
            "Breakpad caught a crash (Couchbase version {}). Writing crash "
            "dump to {} before terminating.",
            PRODUCT_VERSION,
            file);
    dump_stack();
    cb::logger::shutdown();
    return succeeded;
}
#elif defined(linux) && defined(HAVE_BREAKPAD)

#define MSG_CRITICAL "CRITICAL "
#define MSG1p1 "Breakpad caught a crash (Couchbase version "
#define MSG1p2 "). Writing crash dump to "
#define CAUGHT_CRASH_MSG MSG1p1 PRODUCT_VERSION MSG1p2
#define CRITICAL_CAUGHT_CRASH_MSG MSG_CRITICAL CAUGHT_CRASH_MSG
#define FAILED_OPEN MSG_CRITICAL "dumpCallback failed to open crashLogPath:"

static bool dumpCallback(const MinidumpDescriptor& descriptor,
                         void* context,
                         bool succeeded) {
    auto writeFn = [&descriptor](int fd) {
        if (fd == STDERR_FILENO) {
            WRITE_MSG(fd, CRITICAL_CAUGHT_CRASH_MSG);
        } else {
            WRITE_MSG(fd, CAUGHT_CRASH_MSG);
        }
        WRITE_CSTR(fd, descriptor.path());
        WRITE_MSG(fd, " before terminating.\n");
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

#undef MSG_CRITICAL
#undef MSG1p1
#undef MSG1p2
#undef CAUGHT_CRASH_MSG
#undef CRITICAL_CAUGHT_CRASH_MSG
#undef FAILED_OPEN
#endif

void create_handler(const std::string& minidump_dir) {
#if defined(WIN32) && defined(HAVE_BREAKPAD)
    // Takes a wchar_t* on Windows. Isn't the Breakpad API nice and
    // consistent? ;)
    size_t len = minidump_dir.length() + 1;
    std::wstring wc_minidump_dir(len, '\0');
    size_t wlen = 0;
    mbstowcs_s(
            &wlen, &wc_minidump_dir[0], len, minidump_dir.c_str(), _TRUNCATE);

    handler.reset(new ExceptionHandler(&wc_minidump_dir[0],
                                       /*filter*/ NULL,
                                       dumpCallback,
                                       /*callback-context*/ NULL,
                                       ExceptionHandler::HANDLER_ALL,
                                       MiniDumpNormal,
                                       /*pipe*/ (wchar_t*)NULL,
                                       /*custom_info*/ NULL));
#elif defined(linux) && defined(HAVE_BREAKPAD)
    MinidumpDescriptor descriptor(minidump_dir.c_str());
    handler.reset(new ExceptionHandler(descriptor,
                                       /*filter*/ nullptr,
                                       dumpCallback,
                                       /*callback-context*/ nullptr,
                                       /*install_handler*/ true,
                                       /*server_fd*/ -1));
#else
// Not supported on this platform
#endif
}

void logCrashData() {
    if (cb::io::isFile(crashLogPath)) {
#if defined(WIN32)
        // I'm not reading this.
        LOG_CRITICAL_RAW("Unexpected crash file found on WIN32");
#elif defined(linux)
        LOG_CRITICAL_RAW("Detected previous crash");

        // OK to get a truncated input, 8K is plenty to capture most dumps and
        // get the CRITICAL messages into the log.
        const size_t fileReadLimit = 8192;
        std::string data = cb::io::loadFile(crashLogPath, {}, fileReadLimit);
        auto ss = std::stringstream{data};
        const int lineLimit = 70;
        int lineCount = 0;
        for (std::string line; std::getline(ss, line) && lineCount < lineLimit;
             lineCount++) {
            if (!line.empty()) {
                LOG_CRITICAL("{}", line);
            }
        }
        if (lineCount == lineLimit) {
            LOG_INFO("logCrashData reached line count limit {}", lineCount);
        }
#endif
        if (remove(crashLogPath.c_str()) != 0) {
            LOG_CRITICAL("Failed to remove crashLogPath:{} errno:{}",
                         crashLogPath,
                         errno);
        }
    }
}

void cb::breakpad::initialize(const cb::breakpad::Settings& settings,
                              const cb::logger::Config& logConfig) {
    // We cannot actually change any of breakpad's settings once created, only
    // remove it and re-create with new settings.
    destroy();

    bool checkForCrash = crashLogPath.empty();
    crashLogPath = logConfig.filename + ".breakpad.crash.txt";

    if (settings.enabled) {
        create_handler(settings.minidump_dir);
    }

    if (handler) {
        // Turn off the terminate handler's backtrace - otherwise we
        // just print it twice.
        set_terminate_handler_print_backtrace(false);

        LOG_INFO("Breakpad enabled. Minidumps will be written to '{}'",
                 settings.minidump_dir);
    } else {
        // If breakpad is off, then at least print the backtrace via
        // terminate_handler.
        set_terminate_handler_print_backtrace(true);
        LOG_INFO_RAW("Breakpad disabled");
    }

    if (checkForCrash) {
        logCrashData();
    }
}

void cb::breakpad::destroy() {
    if (handler) {
        set_terminate_handler_print_backtrace(true);
    }
    handler.reset();
}
