/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <stdexcept>
#include <string>

enum class TaskType {
    None = -1,
    Writer = 0,
    Reader = 1,
    AuxIO = 2,
    NonIO = 3,
    Count = 4 // keep this as last element of the enum
};

static inline std::string to_string(const TaskType type) {
    switch (type) {
    case TaskType::Writer:
        return "Writer";
    case TaskType::Reader:
        return "Reader";
    case TaskType::AuxIO:
        return "AuxIO";
    case TaskType::NonIO:
        return "NonIO";
    case TaskType::None:
        return "None";
    case TaskType::Count:
        return "Count";
    default:
        throw std::invalid_argument("to_string(task_type_t) unknown type:{" +
                                    std::to_string(int(type)) + "}");
    }
}

inline auto format_as(const TaskType type) {
    return to_string(type);
}
