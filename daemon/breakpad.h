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

#include "config.h"
#include "settings.h"

#if defined(WIN32) && defined(HAVE_BREAKPAD)
#include "client/windows/handler/exception_handler.h"
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
void initialize(const cb::breakpad::Settings& settings);

/**
 * Cleaning up when breakpad no longer needed
 * (Assuming it is enabled and has been initialized)
 */
void destroy();

} // namespace breakpad
} // namespace cb
