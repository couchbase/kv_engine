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

#include <platform/backtrace.h>
#include <platform/dirutils.h>

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
#define WRITE_MSG(MSG)                                        \
    do {                                                      \
        auto rv = write(STDERR_FILENO, MSG, sizeof(MSG) - 1); \
        (void)rv;                                             \
    } while (0)

#define WRITE_CSTR(CSTR)                                    \
    do {                                                    \
        auto rv = write(STDERR_FILENO, CSTR, strlen(CSTR)); \
        (void)rv;                                           \
    } while (0)

static void write_to_logger(void* ctx, const char* frame) {
    WRITE_MSG("   ");
    WRITE_CSTR(frame);
    WRITE_MSG("\n");
}

static void dump_stack() {
    WRITE_MSG("Stack backtrace of crashed thread:\n");
    print_backtrace(write_to_logger, nullptr);
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

#define MSG1p1 "CRITICAL Breakpad caught a crash (Couchbase version "
#define MSG1p2 "). Writing crash dump to "
#define CAUGHT_CRASH_MSG MSG1p1 PRODUCT_VERSION MSG1p2

static bool dumpCallback(const MinidumpDescriptor& descriptor,
                         void* context,
                         bool succeeded) {
    WRITE_MSG(CAUGHT_CRASH_MSG);
    WRITE_CSTR(descriptor.path());
    WRITE_MSG(" before terminating.\n");
    dump_stack();
    return succeeded;
}
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
// Not supported on this plaform
#endif
}

void cb::breakpad::initialize(const cb::breakpad::Settings& settings) {
    // We cannot actually change any of breakpad's settings once created, only
    // remove it and re-create with new settings.
    destroy();

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
}

void cb::breakpad::initialize(const std::string& directory) {
    // We cannot actually change any of breakpad's settings once created, only
    // remove it and re-create with new settings.
    destroy();

    if (directory.empty()) {
        // No directory provided
        LOG_INFO_RAW("Breakpad disabled");
        return;
    }

    create_handler(directory);

    if (handler) {
        // Turn off the terminate handler's backtrace - otherwise we
        // just print it twice.
        set_terminate_handler_print_backtrace(false);
    } else {
        // do we want to notify the user that we don't have access?
    }
}

void cb::breakpad::destroy() {
    if (handler) {
        set_terminate_handler_print_backtrace(true);
    }
    handler.reset();
}
