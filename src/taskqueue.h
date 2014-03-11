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

#ifndef SRC_TASKQUEUE_H_
#define SRC_TASKQUEUE_H_ 1

#include "config.h"

#include <queue>

#include "ringbuffer.h"
#include "task_type.h"
#include "tasks.h"
#include "tasklogentry.h"
#define TASK_LOG_SIZE 80

class ExecutorPool;

class TaskQueue {
    friend class ExecutorPool;
public:
    TaskQueue(ExecutorPool *m, task_type_t t, const char *nm);
    ~TaskQueue();
    void schedule(ExTask &task);

    struct timeval reschedule(ExTask &task);

    bool fetchNextTask(ExTask &task, struct timeval &tv, int &taskIdx,
                       struct timeval now);

    void wake(ExTask &task);

    static const std::string taskType2Str(task_type_t type);

    const std::string getName() const;

    void addLogEntry(const std::string &desc, const hrtime_t runtime,
                     rel_time_t startRelTime, bool isSlowJob);

    const std::vector<TaskLogEntry> getLog() { return tasklog.contents(); }

    const std::vector<TaskLogEntry> getSlowLog() { return slowjobs.contents();}

private:
    bool empty(void);
    void moveReadyTasks(struct timeval tv);
    void pushReadyTask(ExTask &tid);
    ExTask popReadyTask(void);

    SyncObject mutex;
    AtomicValue<bool> isLock;
    const std::string name;
    task_type_t queueType;
    ExecutorPool *manager;

    // sorted by task priority then waketime ..
    std::priority_queue<ExTask, std::deque<ExTask >,
                        CompareByPriority> readyQueue;
    std::priority_queue<ExTask, std::deque<ExTask >,
                        CompareByDueDate> futureQueue;

    RingBuffer<TaskLogEntry> tasklog;
    RingBuffer<TaskLogEntry> slowjobs;
};

#endif  // SRC_TASKQUEUE_H_
