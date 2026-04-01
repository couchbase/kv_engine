/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "platform/cb_time.h"

#include <nlohmann/json.hpp>
#include <spdlog/common.h>

#include <optional>

/**
 * Rate-limited batch logger for repeated events
 *
 * Reduces log spam when failures or successes occur frequently by batching
 * events and logging only on the first failure and when an interval expires.
 *
 * ## Usage Pattern
 *
 * RateLimitedLogger is designed for scenarios where an event (such as backfill
 * memory pressure) occurs repeatedly in quick succession. Instead of logging
 * every occurrence, the logger:
 *
 * 1. **First failure**: Logs immediately on first `failure()` call
 * 2. **Subsequent failures**: Counts failures but does not log
 * 3. **Successes**: Counts successes but does not log (unless the interval
 *                   expires)
 * 4. **Interval expiry**: When an interval expires, flushes a summary log with
 *                         accumulated counts of both failures and successes
 *
 * ## Example
 *
 * ```cpp
 * RateLimitedLogger memoryPressureLogger(
 *     "Backfill snoozing due to high memory pressure",
 *     {{"bucket", "mybucket"}},
 *     "recovered_count",
 *     "snooze_count",
 *     std::chrono::minutes{1},
 *     spdlog::level::warn);
 *
 * // In a loop where memory pressure is checked:
 * if (highMemoryPressure) {
 *     memoryPressureLogger.failure();  // Logs on first call
 *                                      // Counts on subsequent calls
 * } else {
 *     memoryPressureLogger.success();  // Counts successes
 * }
 *
 * // Periodically call to flush accumulated counts:
 * memoryPressureLogger.maybeFlushOutput();
 * ```
 *
 * ## Logging Behavior
 *
 * The context JSON is enhanced with success and failure counts when the
 * interval expires:
 *
 * - **First failure log**: Includes the base context from constructor
 * - **Summary log** (on interval expiry): Base context + `successLabel` and
 *   `failureLabel` fields with their respective counts
 */
class RateLimitedLogger {
public:
    /**
     * Construct a rate-limited logger
     *
     * @param prefix The log message to include in all log entries
     * @param ctx JSON context object to be logged with each message
     * @param successLabel JSON field name for success count (e.g.,
     *                     "recovered")
     * @param failureLabel JSON field name for failure count (e.g., "failures")
     * @param interval Time between flushes of accumulated counts (default: 1
     *                 minute)
     * @param logLevel spdlog level for all logging (default: info)
     */
    RateLimitedLogger(
            std::string prefix,
            nlohmann::json ctx,
            std::string successLabel,
            std::string failureLabel,
            std::chrono::milliseconds interval = std::chrono::minutes{1},
            spdlog::level::level_enum logLevel = spdlog::level::info)
        : message(std::move(prefix)),
          context(std::move(ctx)),
          successLabel(std::move(successLabel)),
          failureLabel(std::move(failureLabel)),
          interval(interval),
          logLevel(logLevel) {
    }

    /**
     * Flush any pending events to the logger
     */
    ~RateLimitedLogger() noexcept {
        try {
            maybeFlushOutput();
        } catch (...) {
        }
    }

    /**
     * Record a successful event
     *
     * Increments the success counter. Does not log unless the logging interval
     * has expired. Safe to call repeatedly.
     */
    void success();

    /**
     * Record a failed event
     *
     * Logs immediately on the first call, then increments the failure counter
     * on subsequent calls. Use together with success() to track recovery and
     * repeated failures.
     */
    void failure();

    /**
     * Flush accumulated counts if the interval has expired
     *
     * If the logging interval has expired since the first failure, this emits
     * a log entry containing the accumulated success and failure counts in the
     * context and resets the counters. If the interval has not expired, this
     * is a no-op.
     */
    void maybeFlushOutput();

protected:
    const std::string message;
    const nlohmann::json context;
    const std::string successLabel;
    const std::string failureLabel;
    const std::chrono::milliseconds interval;
    const spdlog::level::level_enum logLevel;
    std::optional<cb::time::steady_clock::time_point> lastLogTime;
    size_t num_success = 0;
    size_t num_failure = 0;
};
