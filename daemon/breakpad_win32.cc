/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#include "breakpad.h"
#include "client/windows/handler/exception_handler.h"
#include "memcached.h"
#include "memcached/extension_loggers.h"
#include <platform/backtrace.h>
#include <stdlib.h>

using namespace google_breakpad;
ExceptionHandler* handler;

/* Callback function to print to the logger. */
static void write_to_logger(void* ctx, const char* frame) {
    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                    "    %s", frame);
}

/* Called when an exception triggers a dump, outputs details to memcached.log */
static bool dumpCallback(const wchar_t* dump_path, const wchar_t* minidump_id,
                         void* context, EXCEPTION_POINTERS* exinfo,
                         MDRawAssertionInfo* assertion, bool succeeded) {
    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                    "Breakpad caught crash in memcached version %s. Writing crash dump to "
                                    "%S\\%S.dmp before terminating.",
                                    get_server_version(),
                                    dump_path, minidump_id);

    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                    "Stack backtrace of crashed thread:");
    print_backtrace(write_to_logger, NULL);

    // Shutdown logger (if present) to force a flush of any pending log messages.
    if (settings.extensions.logger->shutdown != NULL) {
        settings.extensions.logger->shutdown(/*force*/true);
    }
    return succeeded;
}

static void create_breakpad(const std::string& minidump_dir) {
    // Takes a wchar_t* on Windows. Isn't the Breakpad API nice and
    // consistent? ;)
    size_t len = minidump_dir.length() + 1;
    wchar_t* wc_minidump_dir = new wchar_t[len];
    size_t wlen = 0;
    mbstowcs_s(&wlen, wc_minidump_dir, len, minidump_dir.c_str(), _TRUNCATE);

    handler = new ExceptionHandler(wc_minidump_dir, /*filter*/NULL,
                                   dumpCallback, /*callback-context*/NULL,
                                   ExceptionHandler::HANDLER_ALL,
                                   MiniDumpNormal, /*pipe*/(wchar_t*) NULL,
        /*custom_info*/NULL);

    delete[] wc_minidump_dir;
}

void initialize_breakpad(const BreakpadSettings& settings) {
    // We cannot actually change any of breakpad's settings once created, only
    // remove it and re-create with new settings.
    destroy_breakpad();

    if (settings.isEnabled()) {
        create_breakpad(settings.getMinidumpDir());
    }
}

void destroy_breakpad(void) {
    delete handler;
    handler = nullptr;
}
