/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "tasks_table_printer.h"
#include <folly/portability/GTest.h>
#include <limits>
#include <sstream>

TEST(TasksTablePrinterTest, FormatPsTime) {
    EXPECT_EQ("0:00.00", formatPsTime(0));
    EXPECT_EQ("0:00.12", formatPsTime(120'000'000LL));
    EXPECT_EQ("0:05.50", formatPsTime(5'500'000'000LL));
    // Minutes are zero-padded once seconds no longer need the leading
    // digit from hours (matching cbstats.py's ps_time_label()).
    EXPECT_EQ("01:05.00", formatPsTime(65'000'000'000LL));
    EXPECT_EQ("1:01:05.00", formatPsTime(3'665'000'000'000LL));
    EXPECT_EQ("-0:00.50", formatPsTime(-500'000'000LL));
}

TEST(TasksTablePrinterTest, PrintTasksTable) {
    const uint64_t curTime = 1000'000'000'000ULL; // 1000s in ns
    const nlohmann::json tasksArray = {
            {{"tid", 10},
             {"priority", 5},
             {"state", "running"},
             {"bucket", "default"},
             {"waketime_ns", curTime + 5'000'000'000ULL},
             {"last_starttime_ns",
              curTime - 2'000'000'000ULL}, // running for 2s
             {"previous_runtime_ns", 1'000'000'000ULL},
             {"total_runtime_ns", 10'000'000'000ULL},
             {"num_runs", 15},
             {"type", "Writer"},
             {"name", "FlushTask"},
             {"this", "0x1234"},
             {"description", "Flushing data"}},
            {{"tid", 11},
             {"priority", 0},
             {"state", "snoozed"},
             {"bucket", "default"},
             {"waketime_ns", curTime + 10'000'000'000ULL},
             {"last_starttime_ns", 0},
             {"previous_runtime_ns", 500'000'000ULL},
             {"total_runtime_ns", 2'000'000'000ULL},
             {"num_runs", 5},
             {"type", "Reader"},
             {"name", "BGFetchTask"},
             {"this", "0x5678"},
             {"description", "Background fetch"}}};

    const std::vector<std::pair<std::string, std::string>> stats = {
            {"ep_tasks:cur_time:default", std::to_string(curTime)},
            {"ep_tasks:tasks:default", tasksArray.dump()}};

    std::stringstream ss;
    printTasksTable(stats, "", ss);
    const auto output = ss.str();

    EXPECT_NE(std::string::npos, output.find("Tasks     Writer Reader AuxIO"));
    EXPECT_NE(std::string::npos, output.find("Running   1"));
    EXPECT_NE(std::string::npos, output.find("All       1      1"));
    EXPECT_NE(std::string::npos, output.find("TID"));
    EXPECT_NE(std::string::npos, output.find("FlushTask"));
    EXPECT_NE(std::string::npos, output.find("BGFetchTask"));
    EXPECT_NE(std::string::npos, output.find("*0:02.00")); // running task has *
    EXPECT_NE(std::string::npos, output.find("0:10.00")); // sleep time 10s
}

TEST(TasksTablePrinterTest, SortTasksTable) {
    const uint64_t curTime = 1000'000'000'000ULL;
    const nlohmann::json tasksArray = {
            {{"tid", 2},
             {"priority", 1},
             {"state", "idle"},
             {"bucket", "b"},
             {"waketime_ns", curTime + 1'000'000'000ULL},
             {"last_starttime_ns", 0},
             {"previous_runtime_ns", 100'000'000ULL},
             {"total_runtime_ns", 100'000'000ULL},
             {"num_runs", 1},
             {"type", "NonIO"},
             {"name", "ZTask"},
             {"this", "0x2"},
             {"description", "Z"}},
            {{"tid", 1},
             {"priority", 0},
             {"state", "idle"},
             {"bucket", "a"},
             {"waketime_ns", curTime + 2'000'000'000ULL},
             {"last_starttime_ns", 0},
             {"previous_runtime_ns", 200'000'000ULL},
             {"total_runtime_ns", 200'000'000ULL},
             {"num_runs", 2},
             {"type", "NonIO"},
             {"name", "ATask"},
             {"this", "0x1"},
             {"description", "A"}}};

    const std::vector<std::pair<std::string, std::string>> stats = {
            {"ep_tasks:cur_time:default", std::to_string(curTime)},
            {"ep_tasks:tasks:default", tasksArray.dump()}};

    std::stringstream ss;
    printTasksTable(stats, "name", ss);
    const auto output = ss.str();

    auto posA = output.find("ATask");
    auto posZ = output.find("ZTask");
    EXPECT_NE(std::string::npos, posA);
    EXPECT_NE(std::string::npos, posZ);
    EXPECT_LT(posA, posZ);
}

TEST(TasksTablePrinterTest, SortTasksTableByNumericColumnIndex) {
    const uint64_t curTime = 1000'000'000'000ULL;
    const nlohmann::json tasksArray = {
            {{"tid", 1},
             {"priority", 0},
             {"state", "idle"},
             {"bucket", "a"},
             {"waketime_ns", curTime + 1'000'000'000ULL},
             {"last_starttime_ns", 0},
             {"previous_runtime_ns", 0},
             {"total_runtime_ns", 100'000'000ULL}, // 0.1s
             {"num_runs", 1},
             {"type", "NonIO"},
             {"name", "SmallTotalRun"},
             {"this", "0x1"},
             {"description", "A"}},
            {{"tid", 2},
             {"priority", 0},
             {"state", "idle"},
             {"bucket", "b"},
             {"waketime_ns", curTime + 1'000'000'000ULL},
             {"last_starttime_ns", 0},
             {"previous_runtime_ns", 0},
             {"total_runtime_ns", 9'000'000'000ULL}, // 9s
             {"num_runs", 1},
             {"type", "NonIO"},
             {"name", "BigTotalRun"},
             {"this", "0x2"},
             {"description", "B"}}};

    const std::vector<std::pair<std::string, std::string>> stats = {
            {"ep_tasks:cur_time:default", std::to_string(curTime)},
            {"ep_tasks:tasks:default", tasksArray.dump()}};

    // Column index 6 is TotalRun, which defaults to descending (invertSort).
    std::stringstream ss;
    printTasksTable(stats, "6", ss);
    const auto output = ss.str();

    auto posBig = output.find("BigTotalRun");
    auto posSmall = output.find("SmallTotalRun");
    EXPECT_NE(std::string::npos, posBig);
    EXPECT_NE(std::string::npos, posSmall);
    EXPECT_LT(posBig, posSmall);
}

TEST(TasksTablePrinterTest, SortTasksTableOutOfRangeNumberIsIgnored) {
    const uint64_t curTime = 1000'000'000'000ULL;
    const nlohmann::json tasksArray = {{{"tid", 1},
                                        {"priority", 0},
                                        {"state", "idle"},
                                        {"bucket", "a"},
                                        {"waketime_ns", curTime},
                                        {"last_starttime_ns", 0},
                                        {"previous_runtime_ns", 0},
                                        {"total_runtime_ns", 0},
                                        {"num_runs", 1},
                                        {"type", "NonIO"},
                                        {"name", "OnlyTask"},
                                        {"this", "0x1"},
                                        {"description", "A"}}};

    const std::vector<std::pair<std::string, std::string>> stats = {
            {"ep_tasks:cur_time:default", std::to_string(curTime)},
            {"ep_tasks:tasks:default", tasksArray.dump()}};

    // A numeric sort argument too large for int must not crash or be
    // misreported as a JSON parse failure; it should just be ignored.
    std::stringstream ss;
    printTasksTable(stats, "99999999999", ss);
    const auto output = ss.str();

    EXPECT_NE(std::string::npos, output.find("OnlyTask"));
    EXPECT_EQ(std::string::npos, output.find("Failed to parse"));
}

TEST(TasksTablePrinterTest,
     SortTasksTableAmbiguousShortNamePrefersShortestMatch) {
    const uint64_t curTime = 1000'000'000'000ULL;
    // "St" (state) and "SleepFor" both start with "s"; the shorter column
    // name ("St") must win deterministically rather than whichever column
    // happens to come first/last in the column list.
    const nlohmann::json tasksArray = {
            {{"tid", 1},
             {"priority", 0},
             {"state", "idle"}, // 'i', sorts first ascending
             {"bucket", "a"},
             {"waketime_ns", curTime + 1'000'000'000ULL}, // sleeps 1s
             {"last_starttime_ns", 0},
             {"previous_runtime_ns", 0},
             {"total_runtime_ns", 0},
             {"num_runs", 1},
             {"type", "NonIO"},
             {"name", "TaskIdle"},
             {"this", "0x1"},
             {"description", "A"}},
            {{"tid", 2},
             {"priority", 0},
             {"state", "running"}, // 'r', sorts second ascending
             {"bucket", "b"},
             {"waketime_ns", curTime + 5'000'000'000ULL}, // sleeps 5s
             {"last_starttime_ns", 0},
             {"previous_runtime_ns", 0},
             {"total_runtime_ns", 0},
             {"num_runs", 1},
             {"type", "NonIO"},
             {"name", "TaskRunning"},
             {"this", "0x2"},
             {"description", "B"}}};

    const std::vector<std::pair<std::string, std::string>> stats = {
            {"ep_tasks:cur_time:default", std::to_string(curTime)},
            {"ep_tasks:tasks:default", tasksArray.dump()}};

    std::stringstream ss;
    printTasksTable(stats, "s", ss);
    const auto output = ss.str();

    // If "St" won (as expected), idle sorts before running. If "SleepFor"
    // had won instead, the longer sleep (TaskRunning) would sort first.
    auto posIdle = output.find("TaskIdle");
    auto posRunning = output.find("TaskRunning");
    EXPECT_NE(std::string::npos, posIdle);
    EXPECT_NE(std::string::npos, posRunning);
    EXPECT_LT(posIdle, posRunning);
}

TEST(TasksTablePrinterTest,
     SortTasksTableReverseWithManyEqualKeysDoesNotCrash) {
    // reverse (descending) sort on a column where most rows compare equal
    // used to violate std::sort's strict-weak-ordering requirement (the
    // comparator returned true for both comp(a, b) and comp(b, a) whenever
    // a and b were equal), which could read out of bounds. Build enough
    // equal-SleepFor/Runtime rows to exercise that code path.
    const uint64_t curTime = 1000'000'000'000ULL;
    nlohmann::json tasksArray = nlohmann::json::array();
    for (int i = 0; i < 100; ++i) {
        tasksArray.push_back(
                {{"tid", i},
                 {"priority", 0},
                 {"state", "idle"},
                 {"bucket", "default"},
                 {"waketime_ns", std::numeric_limits<uint64_t>::max()},
                 {"last_starttime_ns", 0},
                 {"previous_runtime_ns", 0},
                 {"total_runtime_ns", 0},
                 {"num_runs", 0},
                 {"type", "NonIO"},
                 {"name", "Task" + std::to_string(i)},
                 {"this", "0x" + std::to_string(i)},
                 {"description", "D"}});
    }

    const std::vector<std::pair<std::string, std::string>> stats = {
            {"ep_tasks:cur_time:default", std::to_string(curTime)},
            {"ep_tasks:tasks:default", tasksArray.dump()}};

    // Column index 4 is SleepFor and defaults to descending (invertSort);
    // every row has the same wake time, so all keys compare equal.
    std::stringstream ss;
    printTasksTable(stats, "4", ss);
    const auto output = ss.str();

    for (int i = 0; i < 100; ++i) {
        EXPECT_NE(std::string::npos, output.find("Task" + std::to_string(i)));
    }
}

TEST(TasksTablePrinterTest, PrintTasksTableMultipleBucketsUseOwnCurTime) {
    // Each bucket/taskable reports its own ep_tasks:cur_time; a task running
    // since last_starttime_ns must use that bucket's cur_time, not some
    // other bucket's, to compute its runtime.
    const uint64_t curTimeA = 1000'000'000'000ULL; // 1000s in ns
    const uint64_t curTimeB = 5000'000'000'000ULL; // 5000s in ns

    const nlohmann::json tasksA = {
            {{"tid", 1},
             {"priority", 0},
             {"state", "running"},
             {"bucket", "bucketA"},
             {"waketime_ns", 0},
             {"last_starttime_ns", curTimeA - 2'000'000'000ULL}, // 2s ago
             {"previous_runtime_ns", 0},
             {"total_runtime_ns", 0},
             {"num_runs", 1},
             {"type", "Writer"},
             {"name", "TaskA"},
             {"this", "0xA"},
             {"description", "A"}}};
    const nlohmann::json tasksB = {
            {{"tid", 2},
             {"priority", 0},
             {"state", "running"},
             {"bucket", "bucketB"},
             {"waketime_ns", 0},
             {"last_starttime_ns", curTimeB - 3'000'000'000ULL}, // 3s ago
             {"previous_runtime_ns", 0},
             {"total_runtime_ns", 0},
             {"num_runs", 1},
             {"type", "Writer"},
             {"name", "TaskB"},
             {"this", "0xB"},
             {"description", "B"}}};

    const std::vector<std::pair<std::string, std::string>> stats = {
            {"ep_tasks:cur_time:bucketA", std::to_string(curTimeA)},
            {"ep_tasks:tasks:bucketA", tasksA.dump()},
            {"ep_tasks:cur_time:bucketB", std::to_string(curTimeB)},
            {"ep_tasks:tasks:bucketB", tasksB.dump()}};

    std::stringstream ss;
    printTasksTable(stats, "", ss);
    const auto output = ss.str();

    // Each running task's runtime must be computed against its own bucket's
    // cur_time (2s and 3s respectively), not the other bucket's.
    EXPECT_NE(std::string::npos, output.find("*0:02.00"));
    EXPECT_NE(std::string::npos, output.find("*0:03.00"));
}
