/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
#if defined(HAVE_BREAKPAD)
#  if defined(WIN32)
#    include "client/windows/handler/exception_handler.h"
#  elif defined(linux)
#    include "client/linux/handler/exception_handler.h"
#  else
#    error Unsupported platform for breakpad, cannot compile.
#  endif

#include "memcached/extension_loggers.h"
#include <platform/backtrace.h>

#include <stdlib.h>

extern "C" {
    extern struct settings settings;
}

using namespace google_breakpad;

ExceptionHandler* handler;

/* Callback function to print to the logger. */
static void write_to_logger(void* ctx, const char* frame) {
    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                    "    %s", frame);
}

#if defined(WIN32)
/* Called when an exception triggers a dump, outputs details to memcached.log */
static bool dumpCallback(const wchar_t* dump_path, const wchar_t* minidump_id,
                         void* context, EXCEPTION_POINTERS* exinfo,
                         MDRawAssertionInfo* assertion, bool succeeded) {
    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Breakpad caught crash in memcached. Writing crash dump to "
        "%S\\%S.dmp before terminating.", dump_path, minidump_id);

    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                    "Stack backtrace of crashed thread:");
    print_backtrace(write_to_logger, NULL);

    // Shutdown logger (if present) to force a flush of any pending log messages.
    if (settings.extensions.logger->shutdown != NULL) {
        settings.extensions.logger->shutdown(/*force*/true);
    }
    return succeeded;
}
#endif

#if defined(linux)
/* Called when an exception triggers a dump, outputs details to memcached.log */
static bool dumpCallback(const MinidumpDescriptor& descriptor,
                         void* context, bool succeeded) {
    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Breakpad caught crash in memcached. Writing crash dump to "
        "%s before terminating.", descriptor.path());

    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                    "Stack backtrace of crashed thread:");
    print_backtrace(write_to_logger, NULL);

    // Shutdown logger (if present) to force a flush of any pending log messages.
    if (settings.extensions.logger->shutdown != NULL) {
        settings.extensions.logger->shutdown(/*force*/true);
    }
    return succeeded;
}
#endif

static void create_breakpad(const char* minidump_dir) {
#if defined(WIN32)
    // Takes a wchar_t* on Windows. Isn't the Breakpad API nice and
    // consistent? ;)
    size_t len = strlen(minidump_dir) + 1;
    wchar_t * wc_minidump_dir = new wchar_t[len];
    size_t wlen = 0;
    mbstowcs_s(&wlen, wc_minidump_dir, len, minidump_dir, _TRUNCATE);

    handler = new ExceptionHandler(wc_minidump_dir, /*filter*/NULL,
                                   dumpCallback, /*callback-context*/NULL,
                                   ExceptionHandler::HANDLER_ALL,
                                   MiniDumpNormal, /*pipe*/(wchar_t*)NULL,
                                   /*custom_info*/NULL);

    delete[] wc_minidump_dir;
#elif defined(linux)
    MinidumpDescriptor descriptor(minidump_dir);
    handler = new ExceptionHandler(descriptor, /*filter*/NULL, dumpCallback,
                                   /*callback-context*/NULL,
                                   /*install_handler*/true, /*server_fd*/-1);
#endif /* defined({OS}) */
}

void initialize_breakpad(const breakpad_settings_t* settings) {
    // We cannot actually change any of breakpad's settings once created, only
    // remove it and re-create with new settings.
    destroy_breakpad();

    if (settings->enabled) {
        create_breakpad(settings->minidump_dir);
    }
}

void destroy_breakpad(void) {
    delete handler;
    handler = NULL;
}

#else /* defined(HAVE_BREAKPAD) */

void initialize_breakpad(const breakpad_settings_t* settings) {
  // do nothing
}

void destroy_breakpad(void) {
  // do nothing
}
#endif /* defined(HAVE_BREAKPAD) */
