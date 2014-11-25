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
#include <stdlib.h>
#include "memcached/extension_loggers.h"

using namespace google_breakpad;

ExceptionHandler* handler;
#if defined(WIN32)
/* Called when an exception triggers a dump, outputs details to memcached.log */
static bool dumpCallback(const wchar_t* dump_path, const wchar_t* minidump_id,
                         void* context, EXCEPTION_POINTERS* exinfo,
                         MDRawAssertionInfo* assertion, bool succeeded){
    get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Breakpad caught crash in memcached. Writing "
                             "%S.dmp to %S before terminating.", minidump_id,
                             dump_path);
    return succeeded;
}
#endif

#if defined(linux)
/* Called when an exception triggers a dump, outputs details to memcached.log */
static bool dumpCallback(const MinidumpDescriptor& descriptor,
                         void* context, bool succeeded) {
    get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Breakpad caught crash in memcached. Writing "
                             "crash dump to %s before terminating.",
                             descriptor.path());
    return succeeded;
}
#endif

/* Setting the exception handler's behaviour for when a dump occurs */
void initialize_breakpad(void) {
    if (getenv("CB_ENABLE_BREAKPAD") != NULL) {
#if defined(WIN32)
        handler = new ExceptionHandler(_wgetenv(L"TEMP"), /*filter*/NULL,
                                       dumpCallback, /*callback-context*/NULL,
                                       ExceptionHandler::HANDLER_ALL,
                                       MiniDumpWithDataSegs, /*pipe*/(wchar_t*)NULL,
                                       /*custom_info*/NULL);
#elif defined(linux)
        MinidumpDescriptor descriptor("/tmp");
        handler = new ExceptionHandler(descriptor, /*filter*/NULL, dumpCallback,
                                       /*callback-context*/NULL,
                                       /*install_handler*/true, /*server_fd*/-1);
#endif /* defined({OS}) */
    }
}

void destroy_breakpad(void) {
    delete handler;
}
#else /* defined(HAVE_BREAKPAD) */
void initialize_breakpad(void) {
  // do nothing
}

void destroy_breakpad(void) {
  // do nothing
}
#endif /* defined(HAVE_BREAKPAD) */
