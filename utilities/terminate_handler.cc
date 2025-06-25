/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "terminate_handler.h"

#include <cstdlib>
#include <exception>

#include <logger/logger.h>
#include <platform/backtrace.h>
#include <platform/byte_literals.h>
#include <platform/exceptions.h>

static bool should_include_backtrace = true;
static std::terminate_handler default_terminate_handler = nullptr;

// Logs details on the handled exception. Attempts to log to
// `terminate_logger` if non-null; otherwise prints to stderr.
static void log_handled_exception() {
#ifdef WIN32
    // Windows doesn't like us re-throwing the exception in the handler (and
    // seems to result in immediate process termination). As such skip logging
    // the exception here.
    return;
#endif
    // Attempt to get the exception's what() message.
    try {
        static std::atomic_bool tried_throw = false;
        // try once to re-throw currently active exception (so we can print
        // its what() message).
        if (!tried_throw.exchange(true)) {
            throw;
        }
    } catch (const std::exception& e) {
        LOG_CRITICAL(
                "Caught unhandled std::exception-derived exception. what(): {}",
                e.what());
        if (const auto* backtrace = cb::getBacktrace(e)) {
            LOG_CRITICAL_RAW("Exception thrown from:");
            print_backtrace_frames(*backtrace, [](const char* frame) {
                LOG_CRITICAL("    {}", frame);
            });
        }
    } catch (...) {
        LOG_CRITICAL_RAW("Caught unknown/unhandled exception.");
    }
}

// Log the symbolified backtrace to this point.
static void log_backtrace() {
    std::array<char, 4_KiB> buffer;
    if (print_backtrace_to_buffer("    ", buffer.data(), buffer.size())) {
        LOG_CRITICAL("Call stack: {}", buffer.data());
    } else {
        // Exceeded buffer space - print directly to stderr FD (requires no
        // buffering, but has the disadvantage that we don't get it in the log).
        fputs("Call stack:\n", stderr);
        print_backtrace_to_file(stderr);
        fflush(stderr);
        LOG_CRITICAL_RAW("Call stack exceeds 4k");
    }
}

// Replacement terminate_handler which prints the exception's what() and a
// backtrace of the current stack before chaining to the default handler.
static void backtrace_terminate_handler() {
    LOG_CRITICAL_RAW(
            "*** Fatal error encountered during exception handling ***");

    log_handled_exception();

    if (should_include_backtrace) {
        log_backtrace();
    }

    // Chain to the default handler if available (as it may be able to print
    // other useful information on why we were told to terminate).
    if (default_terminate_handler != nullptr) {
        default_terminate_handler();
    }

#if !defined(HAVE_BREAKPAD)
    // Shut down the logger (and flush everything). If breakpad is installed
    // then we'll let it do it.
    cb::logger::shutdown();
#endif

    std::abort();
}

void install_backtrace_terminate_handler() {
    if (!cb::logger::isInitialized()) {
        // If we don't have a logger, it's time to create one as the
        // backtrace code expects a logger to be present
        cb::logger::createConsoleLogger();
    }

    if (default_terminate_handler != nullptr) {
        // restore the previously saved one before (re)installing ours.
        std::set_terminate(default_terminate_handler);
    }
    default_terminate_handler = std::set_terminate(backtrace_terminate_handler);
}

void set_terminate_handler_print_backtrace(bool print) {
    should_include_backtrace = print;
}
