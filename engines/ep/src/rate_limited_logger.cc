/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "rate_limited_logger.h"

#include "bucket_logger.h"

void RateLimitedLogger::success() {
    if (num_failure == 0) {
        return;
    }
    ++num_success;
    maybeFlushOutput();
}

void RateLimitedLogger::failure() {
    if (++num_failure == 1) {
        EP_LOG_CTX(logLevel, message, context);
        lastLogTime = cb::time::steady_clock::now();
        return;
    }
    maybeFlushOutput();
}

void RateLimitedLogger::maybeFlushOutput() {
    const auto now = cb::time::steady_clock::now();
    if (lastLogTime.has_value() && now - *lastLogTime >= interval) {
        auto ctx = context;
        ctx[successLabel] = num_success;
        ctx[failureLabel] = num_failure;
        EP_LOG_CTX(logLevel, message, ctx);
        lastLogTime.reset();
        num_success = 0;
        num_failure = 0;
    }
}
