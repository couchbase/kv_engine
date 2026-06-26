/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "slow_operation.h"
#include <folly/portability/GTest.h>

// =====================================================================
// TraceEntry constructor
// =====================================================================

TEST(TraceEntry, ValidEntry) {
    cb::TraceEntry entry("request=1000:50");
    EXPECT_EQ("request", entry.name);
    EXPECT_EQ(1000u, entry.timestamp);
    EXPECT_EQ(50u, entry.duration);
}

TEST(TraceEntry, ZeroValues) {
    cb::TraceEntry entry("span=0:0");
    EXPECT_EQ("span", entry.name);
    EXPECT_EQ(0u, entry.timestamp);
    EXPECT_EQ(0u, entry.duration);
}

TEST(TraceEntry, LargeValues) {
    // Timestamps are nanoseconds from steady_clock epoch; large values
    // are common in production.
    cb::TraceEntry entry("request=18446744073709551615:4294967295");
    EXPECT_EQ("request", entry.name);
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), entry.timestamp);
    EXPECT_EQ(4294967295u, entry.duration);
}

TEST(TraceEntry, NameWithUnderscores) {
    cb::TraceEntry entry("resolve_bucket=500:10");
    EXPECT_EQ("resolve_bucket", entry.name);
    EXPECT_EQ(500u, entry.timestamp);
    EXPECT_EQ(10u, entry.duration);
}

TEST(TraceEntry, MissingEquals) {
    EXPECT_THROW(cb::TraceEntry("notokenhere"), std::invalid_argument);
}

TEST(TraceEntry, MissingColon) {
    EXPECT_THROW(cb::TraceEntry("span=1000"), std::invalid_argument);
}

TEST(TraceEntry, EmptyString) {
    EXPECT_THROW(cb::TraceEntry(""), std::invalid_argument);
}

TEST(TraceEntry, InvalidTimestamp) {
    EXPECT_THROW(cb::TraceEntry("span=abc:50"), std::invalid_argument);
}

TEST(TraceEntry, InvalidDuration) {
    EXPECT_THROW(cb::TraceEntry("span=1000:abc"), std::invalid_argument);
}

TEST(TraceEntry, DashDashDurationIsInvalid) {
    // Incomplete spans in the tracer serialise their duration as "--";
    // the constructor rejects this as it cannot be converted to a number.
    EXPECT_THROW(cb::TraceEntry("overflow=1000:--"), std::invalid_argument);
}

TEST(TraceEntry, EmptyName) {
    cb::TraceEntry entry("=1000:50");
    EXPECT_EQ("", entry.name);
    EXPECT_EQ(1000u, entry.timestamp);
    EXPECT_EQ(50u, entry.duration);
}

// =====================================================================
// parse_slow_operation
// =====================================================================

TEST(ParseSlowOperation, EmptyString) {
    auto [command, spans] = cb::parse_slow_operation("");
    EXPECT_TRUE(command.empty());
    EXPECT_TRUE(spans.empty());
}

TEST(ParseSlowOperation, NoSlowOperationMarker) {
    auto [command, spans] =
            cb::parse_slow_operation("2024-01-15 INFO some other log line");
    EXPECT_TRUE(command.empty());
    EXPECT_TRUE(spans.empty());
}

TEST(ParseSlowOperation, SlowOperationMarkerButNoJson) {
    auto [command, spans] =
            cb::parse_slow_operation("2024-01-15 WARN Slow operation not-json");
    EXPECT_TRUE(command.empty());
    EXPECT_TRUE(spans.empty());
}

TEST(ParseSlowOperation, InvalidJson) {
    auto [command, spans] = cb::parse_slow_operation(
            "2024-01-15 WARN Slow operation {broken json");
    EXPECT_TRUE(command.empty());
    EXPECT_TRUE(spans.empty());
}

TEST(ParseSlowOperation, SingleSpan) {
    auto [command, spans] = cb::parse_slow_operation(
            R"(2024-01-15 WARN Slow operation {"command":"GET","trace":"request=1000:50"})");
    EXPECT_EQ("GET", command);
    ASSERT_EQ(1u, spans.size());
    EXPECT_EQ("request", spans[0].name);
    EXPECT_EQ(1000u, spans[0].timestamp);
    EXPECT_EQ(50u, spans[0].duration);
}

TEST(ParseSlowOperation, MultipleSpansSortedByTimestamp) {
    // Spans are already in chronological order; sort must preserve them.
    auto [command, spans] = cb::parse_slow_operation(
            R"(log prefix Slow operation {"command":"SET","trace":"request=100:5 resolve_bucket=200:3 execute=300:8"})");
    EXPECT_EQ("SET", command);
    ASSERT_EQ(3u, spans.size());
    EXPECT_EQ("request", spans[0].name);
    EXPECT_EQ(100u, spans[0].timestamp);
    EXPECT_EQ("resolve_bucket", spans[1].name);
    EXPECT_EQ(200u, spans[1].timestamp);
    EXPECT_EQ("execute", spans[2].name);
    EXPECT_EQ(300u, spans[2].timestamp);
}

TEST(ParseSlowOperation, MultipleSpansUnsortedAreOrdered) {
    // Spans arriving out of timestamp order must be sorted ascending.
    auto [command, spans] = cb::parse_slow_operation(
            R"(log prefix Slow operation {"command":"DELETE","trace":"execute=300:8 request=100:5 resolve_bucket=200:3"})");
    EXPECT_EQ("DELETE", command);
    ASSERT_EQ(3u, spans.size());
    EXPECT_EQ(100u, spans[0].timestamp);
    EXPECT_EQ(200u, spans[1].timestamp);
    EXPECT_EQ(300u, spans[2].timestamp);
}

TEST(ParseSlowOperation, SpanDurationsPreservedAfterSort) {
    auto [command, spans] = cb::parse_slow_operation(
            R"(log prefix Slow operation {"command":"GET","trace":"b=200:99 a=100:42"})");
    ASSERT_EQ(2u, spans.size());
    EXPECT_EQ("a", spans[0].name);
    EXPECT_EQ(42u, spans[0].duration);
    EXPECT_EQ("b", spans[1].name);
    EXPECT_EQ(99u, spans[1].duration);
}

TEST(ParseSlowOperation, JsonWithExtraFields) {
    // Additional fields beyond "command" and "trace" must be ignored.
    auto [command, spans] = cb::parse_slow_operation(
            R"(log prefix Slow operation {"conn_id":1,"command":"NOOP","duration":"1ms","trace":"request=500:1","peer":"127.0.0.1:9999"})");
    EXPECT_EQ("NOOP", command);
    ASSERT_EQ(1u, spans.size());
    EXPECT_EQ("request", spans[0].name);
}

TEST(ParseSlowOperation, MarkerAppearsAfterLongPrefix) {
    // The "Slow operation" marker may appear anywhere in the line.
    auto [command, spans] = cb::parse_slow_operation(
            R"(2026-06-26T12:00:00.000000Z WARNING (conn:1) some extra text Slow operation {"command":"GET","trace":"request=1000:50"})");
    EXPECT_EQ("GET", command);
    ASSERT_EQ(1u, spans.size());
}

TEST(ParseSlowOperation, MissingCommandFieldThrows) {
    // "command" is a required field; its absence propagates an exception
    // from nlohmann::json rather than returning an empty result.
    EXPECT_THROW(cb::parse_slow_operation(
                         R"(log Slow operation {"trace":"request=100:5"})"),
                 std::exception);
}

TEST(ParseSlowOperation, MissingTraceFieldThrows) {
    // "trace" is a required field; its absence propagates an exception.
    EXPECT_THROW(
            cb::parse_slow_operation(R"(log Slow operation {"command":"GET"})"),
            std::exception);
}
