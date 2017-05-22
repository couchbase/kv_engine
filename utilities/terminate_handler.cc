/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "terminate_handler.h"

#include <cstdlib>
#include <exception>

#include <platform/backtrace.h>

static EXTENSION_LOGGER_DESCRIPTOR* terminate_logger;
static bool should_include_backtrace = true;
static std::terminate_handler default_terminate_handler = nullptr;

/**
 * Prints the given C-style string to either the terminate_logger (if
 * non-null) or otherwise stderr.
 */
static void fatal_msg(const char* msg) {
    if (terminate_logger != nullptr) {
        terminate_logger->log(EXTENSION_LOG_FATAL, nullptr, "%s", msg);
    } else {
        fprintf(stderr, "%s\n", msg);
    }
}

// Logs details on the handled exception. Attempts to log to
// `terminate_logger` if non-null; otherwise prints to stderr.
static void log_handled_exception() {
    char buffer[1024];

    // Attempt to get the exception's what() message.
    try {
        static int tried_throw = 0;
        // try once to re-throw currently active exception (so we can print
        // its what() message).
        if (tried_throw++ == 0) {
            throw;
        }
    } catch (const std::exception& e) {
        snprintf(buffer,
                 sizeof(buffer),
                 "Caught unhandled std::exception-derived exception. what(): %s",
                 e.what());
    } catch (...) {
        snprintf(buffer,
                 sizeof(buffer),
                 "Caught unknown/unhandled exception.");
    }
    fatal_msg(buffer);
}

// Log the symbolified backtrace to this point.
static void log_backtrace() {
    static const char format_str[] = "Call stack:\n%s";

    char buffer[4096];
    if (print_backtrace_to_buffer("    ", buffer, sizeof(buffer))) {
        fatal_msg("Call stack:");
        fatal_msg(buffer);
    } else {
        // Exceeded buffer space - print directly to stderr FD (requires no
        // buffering, but has the disadvantage that we don't get it in the log).
        fprintf(stderr, format_str, "");
        print_backtrace_to_file(stderr);
        fflush(stderr);

        if (terminate_logger != nullptr) {
            terminate_logger->log(
                    EXTENSION_LOG_FATAL, nullptr, "Call stack exceeds 4k");
        }
    }
}

// Replacement terminate_handler which prints the exception's what() and a
// backtrace of the current stack before chaining to the default handler.
static void backtrace_terminate_handler() {
    fatal_msg("*** Fatal error encountered during exception handling ***");

    log_handled_exception();

    if (should_include_backtrace) {
        log_backtrace();
    }

    // Chain to the default handler if available (as it may be able to print
    // other useful information on why we were told to terminate).
    if (default_terminate_handler != nullptr) {
        default_terminate_handler();
    }

    std::abort();
}

void install_backtrace_terminate_handler(EXTENSION_LOGGER_DESCRIPTOR* logger) {
    if (default_terminate_handler != nullptr) {
        // restore the previously saved one before (re)installing ours.
        std::set_terminate(default_terminate_handler);
    }
    terminate_logger = logger;
    default_terminate_handler = std::set_terminate(backtrace_terminate_handler);
}

void set_terminate_handler_print_backtrace(bool print) {
    should_include_backtrace = print;
}
