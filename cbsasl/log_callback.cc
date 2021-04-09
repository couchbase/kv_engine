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
