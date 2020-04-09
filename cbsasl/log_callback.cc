/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

/*
 *   Due to log.cc being used both in libraries and executables, the file was
 *   not able to be compiled on Windows due to the usage of the API header
 *   linkage. This file was created to allow for the correct API header to be
 *   specified, and so that log.cc can be build into both libraries and
 *   executables.
 */
#include <cbsasl/logging.h>
#include <relaxed_atomic.h>

namespace cb::sasl::logging {

extern cb::RelaxedAtomic<LogCallback> callback;

void set_log_callback(LogCallback logCallback) {
    callback.store(logCallback);
}
} // namespace cb::sasl::logging
