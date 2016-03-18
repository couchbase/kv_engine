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
static std::terminate_handler default_terminate_handler;

// Replacement terminate_handler which prints a backtrace of the current stack
// before chaining to the default handler.
static void backtrace_terminate_handler() {
    char buffer[1024];

    static const char format_str[] =
            "*** Fatal error encountered during exception handling ***\n"
            "Call stack:\n%s";

    if (print_backtrace_to_buffer("    ", buffer, sizeof(buffer))) {
        if (terminate_logger != nullptr) {
            terminate_logger->log(EXTENSION_LOG_FATAL, nullptr, format_str,
                                  buffer);
        } else {
            fprintf(stderr, format_str, buffer);
            fprintf(stderr, "\n");
        }
    } else {
        // Exceeded buffer space - print directly to stderr FD (requires no
        // buffering, but has the disadvantage that we don't get it in the log).
        fprintf(stderr, format_str, "");
        print_backtrace_to_file(stderr);
        fflush(stderr);

        if (terminate_logger != nullptr) {
            terminate_logger->log(
                EXTENSION_LOG_FATAL, nullptr,
                "*** Fatal error encountered during exception handling ***\n"
                "Call stack exceeds 1k");
        }
    }

    // Chain to the default handler if available (as it may be able to print
    // other useful information on why we were told to terminate).
    if (default_terminate_handler != nullptr) {
        default_terminate_handler();
    }

    std::abort();
}

void install_backtrace_terminate_handler(EXTENSION_LOGGER_DESCRIPTOR* logger) {
    terminate_logger = logger;
    default_terminate_handler = std::set_terminate(backtrace_terminate_handler);
}

