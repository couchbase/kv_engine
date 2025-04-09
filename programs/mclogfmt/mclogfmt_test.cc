/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>

#include "mclogfmt.h"
#include <nlohmann/json.hpp>

using namespace mclogfmt;

struct LogEntry {
    std::string ts;
    std::string lvl;
    std::string msg;
    nlohmann::ordered_json ctx;
};

void from_json(const nlohmann::json& j, LogEntry& e) {
    j.at("ts").get_to(e.ts);
    j.at("lvl").get_to(e.lvl);
    j.at("msg").get_to(e.msg);
    j.at("ctx").get_to(e.ctx);
}

static const std::string TestTimestamp = "2025-01-01T12:30:00.000000+00:00";
static const std::string TestJsonString =
        R"({"conn_id":123,"peer":{"ip":"127.0.0.1","port":43210}})";
static const nlohmann::ordered_json TestJson =
        nlohmann::ordered_json::parse(TestJsonString);

std::string convertLine(std::string_view line, LogFormat output) {
    fmt::memory_buffer buffer;
    convertLine(buffer, line, output, {});
    return {buffer.data(), buffer.size()};
}

nlohmann::ordered_json convertLineToJson(std::string_view line) {
    return nlohmann::ordered_json::parse(convertLine(line, LogFormat::Json));
}

std::string convertLineToJsonLog(std::string_view line) {
    return convertLine(line, LogFormat::JsonLog);
}

TEST(McLogFmt, Basic) {
    std::string line{TestTimestamp + " INFO Accepting client " +
                     TestJsonString};

    LogEntry entry = convertLineToJson(line);

    EXPECT_EQ(TestTimestamp, entry.ts);
    EXPECT_EQ("INFO", entry.lvl);
    EXPECT_EQ("Accepting client", entry.msg);
    EXPECT_EQ(TestJson, entry.ctx);
}

TEST(McLogFmt, Roundtrip) {
    std::string line{TestTimestamp + " INFO Accepting client " +
                     TestJsonString};
    auto output = convertLineToJsonLog(convertLineToJson(line).dump());
    EXPECT_EQ(output, line);
}

TEST(McLogFmt, BeforeAfter) {
    const std::string t1 = "2025-01-01T12:29:00.000000+00:00";
    const std::string t2 = "2025-01-01T12:30:00.000000+00:00";
    const std::string t3 = "2025-01-02T00:30:00.000000+00:00";

    {
        LineFilter filter{t1, t3};
        EXPECT_TRUE(filter(t1));
        EXPECT_TRUE(filter(t2));
        // Too late.
        EXPECT_FALSE(filter(t3));
    }

    {
        LineFilter filter{t1, t2};
        EXPECT_TRUE(filter(t1));
        // Too late.
        EXPECT_FALSE(filter(t2));
        EXPECT_FALSE(filter(t3));
    }

    {
        LineFilter filter{t2, t3};
        // Too early.
        EXPECT_FALSE(filter(t1));
        EXPECT_TRUE(filter(t2));
        // Too late.
        EXPECT_FALSE(filter(t3));
    }
}
