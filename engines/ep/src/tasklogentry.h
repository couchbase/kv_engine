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

#ifndef SRC_TASKLOGENTRY_H_
#define SRC_TASKLOGENTRY_H_ 1

#include "config.h"

#include <platform/processclock.h>

#include <chrono>
#include <deque>
#include <list>
#include <map>
#include <queue>
#include <string>
#include <utility>
#include <vector>
#include "task_type.h"

/**
 * Log entry for previous job runs.
 */
class TaskLogEntry {
public:

    // This is useful for the ringbuffer to initialize
    TaskLogEntry() : name("invalid"),
                     duration(ProcessClock::duration::zero()) {}

    TaskLogEntry(const std::string &n, task_type_t type,
                 const ProcessClock::duration d,
                 rel_time_t t = 0)
        : name(n), taskType(type), ts(t), duration(d) {}

    /**
     * Get the name of the job.
     */
    std::string getName() const { return name; }

    /**
     * Get the type of the task (Writer, Reader, AuxIO, NonIO)
     */
     task_type_t getTaskType() const { return taskType; }

    /**
     * Get the duration this job took to run.
     */
    ProcessClock::duration getDuration() const { return duration; }

    /**
     * Get a timestamp indicating when this thing started.
     */
    rel_time_t getTimestamp() const { return ts; }

private:
    std::string name;
    task_type_t taskType;
    rel_time_t ts;
    ProcessClock::duration duration;
};

#endif  // SRC_TASKLOGENTRY_H_
