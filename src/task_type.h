/*
 *     Copyright 2014 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
        return "writer_worker_";
    case READER_TASK_IDX:
        return "reader_worker_";
    case AUXIO_TASK_IDX:
        return "auxIO_worker_";
    case NONIO_TASK_IDX:
        return "nonIO_worker_";
    case NO_TASK_TYPE:
        return "NO_TASK_TYPE_worker_";
    case NUM_TASK_GROUPS:
        return "NUM_TASK_GROUPS_worker_";
    default:
        throw std::invalid_argument("to_string(task_type_t) unknown type:{" +
                                    std::to_string(int(type)) + "}");
    }
}
