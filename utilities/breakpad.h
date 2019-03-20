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
#pragma once

#include <memcached/mcd_util-visibility.h>

#include "breakpad_settings.h"

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

namespace cb {
namespace breakpad {
/**
 * Initialize breakpad based on the specified settings struct.
 *
 * The function may be called multiple times and allow for reconfiguration
 * of the breakpad settings.
 */
MCD_UTIL_PUBLIC_API
void initialize(const cb::breakpad::Settings& settings);

/**
 * Initialize breakpad from use of a command line program.
 *
 * @param directory specifies the dump directory to use (an empty directory
 *                  is accepted and ignored).
 */
MCD_UTIL_PUBLIC_API
void initialize(const std::string& dumpdir);

/**
 * Cleaning up when breakpad no longer needed
 * (Assuming it is enabled and has been initialized)
 */
MCD_UTIL_PUBLIC_API
void destroy();

} // namespace breakpad
} // namespace cb
