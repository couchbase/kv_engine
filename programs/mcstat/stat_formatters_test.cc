/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "stat_formatters.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <sstream>

TEST(StatFormattersTest, PrintHashStatsWithoutDetail) {
    // vb_<id>:histo arrives as a single JSON histogram object (see
    // TimingHistogramPrinter), not as flat per-bucket stats.
    const std::vector<std::pair<std::string, std::string>> stats = {
            {"vb_0:min_dep", "2"},
            {"vb_0:max_dep", "10"},
            {"vb_0:counted", "100"},
            {"vb_0:histo",
             R"({"bucketsLow":0,"data":[[0,50,100.0]],"total":50})"},
            {"vb_1:min_dep", "4"},
            {"vb_1:max_dep", "20"},
            {"vb_1:counted", "200"},
            {"vb_1:histo",
             R"({"bucketsLow":0,"data":[[0,75,100.0]],"total":75})"}};

    std::stringstream ss;
    printHashStats(stats, false, ss);
    const auto output = ss.str();

    EXPECT_EQ(std::string::npos, output.find("vb_0:"));
    EXPECT_EQ(std::string::npos, output.find("vb_1:"));
    EXPECT_NE(std::string::npos, output.find("avg_min:"));
    EXPECT_NE(std::string::npos, output.find("3"));
    EXPECT_NE(std::string::npos, output.find("avg_max:"));
    EXPECT_NE(std::string::npos, output.find("15"));
    EXPECT_NE(std::string::npos, output.find("min_count:"));
    EXPECT_NE(std::string::npos, output.find("100"));
    EXPECT_NE(std::string::npos, output.find("max_count:"));
    EXPECT_NE(std::string::npos, output.find("200"));
    EXPECT_NE(std::string::npos, output.find("total_counts:"));
    EXPECT_NE(std::string::npos, output.find("300"));
    // Both vbuckets' [0,0] buckets (depth 0) are summed: 50 + 75 = 125.
    EXPECT_NE(std::string::npos, output.find("summary:histo_0,0:"));
    EXPECT_NE(std::string::npos, output.find("125"));
    // Depth 0 for every one of the 125 samples -> mean depth is 0.
    EXPECT_NE(std::string::npos, output.find("summary:histo_mean:"));
}

TEST(StatFormattersTest, PrintHashStatsWithDetail) {
    const std::vector<std::pair<std::string, std::string>> stats = {
            {"vb_0:min_dep", "2"},
            {"vb_0:max_dep", "10"},
            {"vb_0:counted", "100"},
            {"vb_0:histo",
             R"({"bucketsLow":0,"data":[[1,100,100.0]],"total":100})"}};

    std::stringstream ss;
    printHashStats(stats, true, ss);
    const auto output = ss.str();

    EXPECT_NE(std::string::npos, output.find("vb_0:min_dep:"));
    EXPECT_NE(std::string::npos, output.find("vb_0:histo:"));
    EXPECT_NE(std::string::npos, output.find("avg_min:"));
    EXPECT_NE(std::string::npos, output.find("2"));
    // Summary is still computed alongside the raw per-vbucket detail.
    EXPECT_NE(std::string::npos, output.find("summary:histo_0,1:"));
    EXPECT_NE(std::string::npos, output.find("100"));
}

TEST(StatFormattersTest, PrintDispatcherStats) {
    const std::vector<std::pair<std::string, std::string>> stats = {
            {"NonIO_0:jobs", "42"},
            {"NonIO_0:runtime", "5000000"}, // 5s in us -> 5000ms
            {"NonIO_0:log:0:task", "TestJob"},
            {"NonIO_0:slow:0:task", "SlowJob"}};

    // Without logs
    std::stringstream ssNoLogs;
    printDispatcherStats(stats, false, ssNoLogs);
    const auto outNoLogs = ssNoLogs.str();
    EXPECT_NE(std::string::npos, outNoLogs.find("NonIO_0"));
    EXPECT_NE(std::string::npos, outNoLogs.find("jobs:"));
    EXPECT_NE(std::string::npos, outNoLogs.find("42"));
    EXPECT_NE(std::string::npos, outNoLogs.find("runtime:"));
    EXPECT_NE(std::string::npos, outNoLogs.find("5000ms"));
    EXPECT_EQ(std::string::npos, outNoLogs.find("Recent jobs:"));
    EXPECT_EQ(std::string::npos, outNoLogs.find("Slow jobs:"));

    // With logs
    std::stringstream ssWithLogs;
    printDispatcherStats(stats, true, ssWithLogs);
    const auto outWithLogs = ssWithLogs.str();
    EXPECT_NE(std::string::npos, outWithLogs.find("Recent jobs:"));
    EXPECT_NE(std::string::npos, outWithLogs.find("TestJob"));
    EXPECT_NE(std::string::npos, outWithLogs.find("Slow jobs:"));
    EXPECT_NE(std::string::npos, outWithLogs.find("SlowJob"));
    EXPECT_NE(std::string::npos, outWithLogs.find("---------"));
}

TEST(StatFormattersTest, PrintResponsesStats) {
    // Keys match the server's format: lowercase hex, no "0x" prefix (see
    // stat_responses_json_executor() and GetErrorMap's "errors" object).
    const nlohmann::json errorMap = {{"0", {{"name", "SUCCESS"}}},
                                     {"1", {{"name", "KEY_ENOENT"}}},
                                     {"2", {{"name", "KEY_EEXISTS"}}}};
    const nlohmann::json jsonResponses = {{"0", 50}, {"1", 5}, {"2", 0}};
    const std::vector<std::pair<std::string, std::string>> stats = {
            {"responses", jsonResponses.dump()}};

    // Without showAll ("2" is filtered out)
    std::stringstream ssFiltered;
    printResponsesStats(stats, errorMap, false, ssFiltered);
    const auto outFiltered = ssFiltered.str();
    EXPECT_NE(std::string::npos, outFiltered.find("SUCCESS:"));
    EXPECT_NE(std::string::npos, outFiltered.find("50"));
    EXPECT_NE(std::string::npos, outFiltered.find("KEY_ENOENT:"));
    EXPECT_NE(std::string::npos, outFiltered.find("5"));
    EXPECT_EQ(std::string::npos, outFiltered.find("KEY_EEXISTS"));

    // With showAll ("2" is included)
    std::stringstream ssAll;
    printResponsesStats(stats, errorMap, true, ssAll);
    const auto outAll = ssAll.str();
    EXPECT_NE(std::string::npos, outAll.find("SUCCESS:"));
    EXPECT_NE(std::string::npos, outAll.find("KEY_ENOENT:"));
    EXPECT_NE(std::string::npos, outAll.find("KEY_EEXISTS:"));
}

TEST(StatFormattersTest, PrintResponsesStatsUnknownCodeFallsBackToRawHex) {
    // A status code not present in the server's error map (e.g. one added
    // server-side after this client binary was built) must not be dropped;
    // it should fall back to the raw hex key.
    const nlohmann::json errorMap = {{"0", {{"name", "SUCCESS"}}}};
    const nlohmann::json jsonResponses = {{"ff", 3}};
    const std::vector<std::pair<std::string, std::string>> stats = {
            {"responses", jsonResponses.dump()}};

    std::stringstream ss;
    printResponsesStats(stats, errorMap, false, ss);
    const auto output = ss.str();
    EXPECT_NE(std::string::npos, output.find("ff:"));
    EXPECT_EQ(std::string::npos, output.find("SUCCESS"));
}
