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

enum task_type_t {
    NO_TASK_TYPE=-1,
    WRITER_TASK_IDX=0,
    READER_TASK_IDX=1,
    AUXIO_TASK_IDX=2,
    NONIO_TASK_IDX=3,
    NUM_TASK_GROUPS=4 // keep this as last element of the enum
};

static inline std::string to_string(const task_type_t type) {
    switch (type) {
    case WRITER_TASK_IDX:
        return "Writer";
    case READER_TASK_IDX:
        return "Reader";
    case AUXIO_TASK_IDX:
        return "AuxIO";
    case NONIO_TASK_IDX:
        return "NonIO";
    case NO_TASK_TYPE:
        return "NO_TASK_TYPE";
    case NUM_TASK_GROUPS:
        return "NUM_TASK_GROUPS";
    default:
        throw std::invalid_argument("to_string(task_type_t) unknown type:{" +
                                    std::to_string(int(type)) + "}");
    }
}
