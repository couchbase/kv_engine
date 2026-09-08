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
#include <fmt/format.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <iostream>
#include <limits>
#include <map>

constexpr uint64_t BIG_VALUE = 1ULL << 60;

std::string formatPsTime(int64_t nanoseconds) {
    int64_t microseconds = nanoseconds / 1000;
    std::string sign = microseconds < 0 ? "-" : "";
    uint64_t absUs = std::abs(microseconds);
    uint64_t centiseconds = absUs / 10000;
    uint64_t totalSeconds = centiseconds / 100;
    uint64_t cs = centiseconds % 100;

    uint64_t hours = totalSeconds / 3600;
    uint64_t minutes = (totalSeconds % 3600) / 60;
    uint64_t seconds = totalSeconds % 60;

    // cbstats.py derives this from Python's timedelta, which always
    // zero-pads minutes to 2 digits once hours are stripped - except when
    // minutes is itself 0, where the leading zero gets stripped too,
    // leaving a bare "0".
    std::string timeStr;
    if (hours > 0) {
        timeStr = fmt::format("{}:{:02}:{:02}", hours, minutes, seconds);
    } else if (minutes > 0) {
        timeStr = fmt::format("{:02}:{:02}", minutes, seconds);
    } else {
        timeStr = fmt::format("{}:{:02}", minutes, seconds);
    }

    return fmt::format("{}{}.{:02}", sign, timeStr, cs);
}

struct TaskRow {
    uint64_t tid = 0;
    int priority = 0;
    std::string state;
    std::string bucket;
    int64_t wakeSort = 0;
    std::string wakeDisplay;
    int64_t runSort = 0;
    std::string runDisplay;
    int64_t totalRunSort = 0;
    std::string totalRunDisplay;
    uint64_t numRuns = 0;
    std::string type;
    std::string name;
    std::string addr;
    std::string descr;

    [[nodiscard]] std::string getFieldStr(size_t index) const {
        switch (index) {
        case 0:
            return std::to_string(tid);
        case 1:
            return std::to_string(priority);
        case 2:
            return state;
        case 3:
            return bucket;
        case 4:
            return wakeDisplay;
        case 5:
            return runDisplay;
        case 6:
            return totalRunDisplay;
        case 7:
            return std::to_string(numRuns);
        case 8:
            return type;
        case 9:
            return name;
        case 10:
            return addr;
        case 11:
            return descr;
        default:
            return {};
        }
    }
};

struct ColDef {
    std::string name;
    bool ralign;
    bool invertSort;
};

static const std::vector<ColDef> columns = {{"TID", true, false},
                                            {"Pri", true, false},
                                            {"St", false, false},
                                            {"Bucket", false, false},
                                            {"SleepFor", true, true},
                                            {"Runtime", true, true},
                                            {"TotalRun", true, true},
                                            {"#Runs", true, true},
                                            {"Type", false, false},
                                            {"Name", false, false},
                                            {"Addr", false, false},
                                            {"Descr.", false, false}};

static void printOneBucketTasks(const nlohmann::json& tasksJson,
                                uint64_t curTime,
                                std::string_view sortBy,
                                std::ostream& out) {
    if (!tasksJson.is_array()) {
        return;
    }

    std::map<std::string, int> totalTasks = {{"Reader", 0},
                                             {"Writer", 0},
                                             {"AuxIO", 0},
                                             {"NonIO", 0},
                                             {"QuickNonIO", 0},
                                             {"SlowIO", 0}};
    auto runningTasks = totalTasks;

    std::vector<TaskRow> rows;
    rows.reserve(tasksJson.size());

    for (const auto& item : tasksJson) {
        if (!item.is_object()) {
            continue;
        }
        TaskRow row;
        row.tid = item.value("tid", 0ULL);
        row.priority = item.value("priority", 0);
        std::string st = item.value("state", "");
        row.state = st.empty() ? "" : std::string(1, st.front());
        row.bucket = item.value("bucket", "");
        row.type = item.value("type", "");
        row.name = item.value("name", "");
        row.addr = item.value("this", "");
        row.descr = item.value("description", "");
        row.numRuns = item.value("num_runs", 0ULL);

        uint64_t wakeNs = item.value("waketime_ns", 0ULL);
        uint64_t lastStartNs = item.value("last_starttime_ns", 0ULL);
        uint64_t prevRunNs = item.value("previous_runtime_ns", 0ULL);
        uint64_t totalRunNs = item.value("total_runtime_ns", 0ULL);

        row.totalRunSort = static_cast<int64_t>(totalRunNs);
        row.totalRunDisplay = formatPsTime(totalRunNs);

        if (lastStartNs != 0) {
            auto runtimeNs = static_cast<int64_t>(curTime) -
                             static_cast<int64_t>(lastStartNs);
            row.runSort = runtimeNs;
            row.runDisplay = "*" + formatPsTime(runtimeNs);
            row.wakeSort = 0;
            row.wakeDisplay = formatPsTime(0);
            runningTasks[row.type]++;
        } else {
            row.runSort = static_cast<int64_t>(prevRunNs);
            row.runDisplay = formatPsTime(prevRunNs);
            if (wakeNs < BIG_VALUE) {
                auto wakeDiff = static_cast<int64_t>(wakeNs) -
                                static_cast<int64_t>(curTime);
                row.wakeSort = wakeDiff;
                row.wakeDisplay = formatPsTime(wakeDiff);
            } else {
                row.wakeSort = std::numeric_limits<int64_t>::max();
                row.wakeDisplay = "inf";
            }
        }
        totalTasks[row.type]++;
        rows.push_back(std::move(row));
    }

    int runningTotal = 0;
    for (const auto& [_, count] : runningTasks) {
        runningTotal += count;
    }
    int allTotal = static_cast<int>(rows.size());

    out << "Tasks     Writer Reader AuxIO  NonIO QNonIO SlowIO  Total      \n";
    out << fmt::format("Running   {:<6} {:<6} {:<6} {:<6} {:<6} {:<6} {:<6}\n",
                       runningTasks["Writer"],
                       runningTasks["Reader"],
                       runningTasks["AuxIO"],
                       runningTasks["NonIO"],
                       runningTasks["QuickNonIO"],
                       runningTasks["SlowIO"],
                       runningTotal);
    out << fmt::format(
            "All       {:<6} {:<6} {:<6} {:<6} {:<6} {:<6} {:<6}\n\n",
            totalTasks["Writer"],
            totalTasks["Reader"],
            totalTasks["AuxIO"],
            totalTasks["NonIO"],
            totalTasks["QuickNonIO"],
            totalTasks["SlowIO"],
            allTotal);

    // Sorting
    int sortCol = -1;
    bool reverse = false;

    if (!sortBy.empty()) {
        std::string sortStr{sortBy};
        bool isNumber = !sortStr.empty() &&
                        std::ranges::all_of(sortStr, [](unsigned char c) {
                            return std::isdigit(c);
                        });
        if (isNumber) {
            try {
                sortCol = std::stoi(sortStr);
            } catch (const std::exception&) {
                sortCol = -1;
            }
            if (sortCol < 0 || sortCol >= static_cast<int>(columns.size())) {
                sortCol = -1;
            }
        } else {
            std::string lowerSort;
            for (char c : sortStr) {
                lowerSort.push_back(static_cast<char>(
                        std::tolower(static_cast<unsigned char>(c))));
            }

            std::vector<std::string> colLowers(columns.size());
            for (size_t i = 0; i < columns.size(); ++i) {
                for (char c : columns[i].name) {
                    colLowers[i].push_back(static_cast<char>(
                            std::tolower(static_cast<unsigned char>(c))));
                }
            }

            // Resolve ambiguous/partial column names deterministically by
            // preferring, in order: an exact match, then the shortest
            // column name that the input is a prefix of, then the
            // shortest column name that matches as a substring in either
            // direction. Earlier logic picked whichever column happened
            // to come first in `columns`, which was surprising (e.g.
            // sorting by "s" matched "St" purely because it precedes
            // "SleepFor" in the list).
            for (size_t i = 0; i < columns.size() && sortCol < 0; ++i) {
                if (colLowers[i] == lowerSort) {
                    sortCol = static_cast<int>(i);
                }
            }
            if (sortCol < 0) {
                size_t bestLen = std::numeric_limits<size_t>::max();
                for (size_t i = 0; i < columns.size(); ++i) {
                    if (colLowers[i].starts_with(lowerSort) &&
                        colLowers[i].size() < bestLen) {
                        sortCol = static_cast<int>(i);
                        bestLen = colLowers[i].size();
                    }
                }
            }
            if (sortCol < 0) {
                size_t bestLen = std::numeric_limits<size_t>::max();
                for (size_t i = 0; i < columns.size(); ++i) {
                    const auto& colLower = colLowers[i];
                    if ((colLower.find(lowerSort) != std::string::npos ||
                         lowerSort.find(colLower) != std::string::npos) &&
                        colLower.size() < bestLen) {
                        sortCol = static_cast<int>(i);
                        bestLen = colLower.size();
                    }
                }
            }
        }

        if (sortCol >= 0) {
            reverse = columns[sortCol].invertSort;
        }
    }

    if (sortCol >= 0) {
        std::ranges::sort(
                rows, [sortCol, reverse](const TaskRow& a, const TaskRow& b) {
                    auto less = [sortCol](const TaskRow& x, const TaskRow& y) {
                        switch (sortCol) {
                        case 0:
                            return x.tid < y.tid;
                        case 1:
                            return x.priority < y.priority;
                        case 2:
                            return x.state < y.state;
                        case 3:
                            return x.bucket < y.bucket;
                        case 4:
                            return x.wakeSort < y.wakeSort;
                        case 5:
                            return x.runSort < y.runSort;
                        case 6:
                            return x.totalRunSort < y.totalRunSort;
                        case 7:
                            return x.numRuns < y.numRuns;
                        case 8:
                            return x.type < y.type;
                        case 9:
                            return x.name < y.name;
                        case 10:
                            return x.addr < y.addr;
                        case 11:
                            return x.descr < y.descr;
                        default:
                            return x.tid < y.tid;
                        }
                    };
                    return reverse ? less(b, a) : less(a, b);
                });
    }

    // Compute each row's field strings once and reuse them for both width
    // calculation and printing, instead of calling getFieldStr() twice per
    // cell.
    std::vector<std::vector<std::string>> rowFields;
    rowFields.reserve(rows.size());
    for (const auto& row : rows) {
        std::vector<std::string> fields;
        fields.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            fields.push_back(row.getFieldStr(i));
        }
        rowFields.push_back(std::move(fields));
    }

    std::vector<size_t> colWidths(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        colWidths[i] = columns[i].name.size();
    }
    for (const auto& fields : rowFields) {
        for (size_t i = 0; i < columns.size(); ++i) {
            colWidths[i] = std::max(colWidths[i], fields[i].size());
        }
    }

    // Print Header
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
            out << "  ";
        }
        if (columns[i].ralign) {
            out << fmt::format("{:>{}}", columns[i].name, colWidths[i]);
        } else {
            out << fmt::format("{:<{}}", columns[i].name, colWidths[i]);
        }
    }
    out << "\n\n";

    // Print Rows
    for (const auto& fields : rowFields) {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) {
                out << "  ";
            }
            if (i == columns.size() - 1 && !columns[i].ralign) {
                // Last column without trailing spaces
                out << fields[i];
            } else if (columns[i].ralign) {
                out << fmt::format("{:>{}}", fields[i], colWidths[i]);
            } else {
                out << fmt::format("{:<{}}", fields[i], colWidths[i]);
            }
        }
        out << "\n";
    }
}

void printTasksTable(
        const std::vector<std::pair<std::string, std::string>>& stats,
        std::string_view sortBy,
        std::ostream& out) {
    // The server reports one "ep_tasks:cur_time:<taskable>" and one
    // "ep_tasks:tasks:<taskable>" entry per bucket/taskable (tasks-all
    // returns one pair per bucket); match them up by the taskable name
    // suffix rather than assuming a single, shared cur_time for all of them.
    static constexpr std::string_view curTimePrefix = "ep_tasks:cur_time:";
    static constexpr std::string_view tasksPrefix = "ep_tasks:tasks:";

    std::map<std::string, uint64_t> curTimeByTaskable;
    std::map<std::string, std::string> tasksJsonByTaskable;
    std::vector<std::string> taskables;

    for (const auto& [key, value] : stats) {
        if (key.starts_with(curTimePrefix)) {
            uint64_t curTime = 0;
            try {
                curTime = std::stoull(value);
            } catch (...) {
            }
            curTimeByTaskable[key.substr(curTimePrefix.size())] = curTime;
        } else if (key.starts_with(tasksPrefix)) {
            auto taskable = key.substr(tasksPrefix.size());
            tasksJsonByTaskable[taskable] = value;
            taskables.push_back(std::move(taskable));
        }
    }

    for (size_t i = 0; i < taskables.size(); ++i) {
        const auto& taskable = taskables[i];
        auto curTimeIt = curTimeByTaskable.find(taskable);
        uint64_t curTime =
                curTimeIt != curTimeByTaskable.end() ? curTimeIt->second : 0;
        try {
            auto parsed = nlohmann::json::parse(tasksJsonByTaskable[taskable]);
            printOneBucketTasks(parsed, curTime, sortBy, out);
            if (i + 1 < taskables.size()) {
                out << "\n";
            }
        } catch (const std::exception& e) {
            out << "Failed to parse tasks JSON: " << e.what() << "\n";
        }
    }
}
