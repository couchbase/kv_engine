/*
 *     Copyright 2013 Couchbase, Inc.
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

#ifndef SRC_SCHEDULER_H_
#define SRC_SCHEDULER_H_ 1

#include "config.h"

#include <deque>
#include <list>
#include <map>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "atomic.h"
#include "common.h"
#include "mutex.h"
#include "objectregistry.h"
#include "tasks.h"
#include "task_type.h"
#include "tasklogentry.h"
#define TASK_LOG_SIZE 80

#define MIN_SLEEP_TIME 2.0

class ExecutorPool;
class ExecutorThread;
class TaskQueue;
class WorkLoadPolicy;

typedef enum {
    EXECUTOR_CREATING,
    EXECUTOR_RUNNING,
    EXECUTOR_WAITING,
    EXECUTOR_SLEEPING,
    EXECUTOR_SHUTDOWN,
    EXECUTOR_DEAD
} executor_state_t;


class ExecutorThread {
    friend class ExecutorPool;
    friend class TaskQueue;
public:

    ExecutorThread(ExecutorPool *m, int startingQueue,
                   const std::string nm) : manager(m),
          startIndex(startingQueue), name(nm),
          state(EXECUTOR_CREATING), taskStart(0),
          currentTask(NULL), curTaskType(NO_TASK_TYPE),
          tasklog(TASK_LOG_SIZE), slowjobs(TASK_LOG_SIZE) {
              set_max_tv(waketime);
    }

    ~ExecutorThread() {
        LOG(EXTENSION_LOG_INFO, "Executor killing %s", name.c_str());
    }

    void start(void);

    void run(void);

    void stop(bool wait=true);

    void shutdown() { state = EXECUTOR_SHUTDOWN; }

    void schedule(ExTask &task);

    void reschedule(ExTask &task);

    void wake(ExTask &task);

    const std::string& getName() const { return name; }

    const std::string getTaskName() const {
        if (currentTask) {
            return currentTask->getDescription();
        } else {
            return std::string("Not currently running any task");
        }
    }

    hrtime_t getTaskStart() const { return taskStart; }

    const std::string getStateName();

    void addLogEntry(const std::string &desc, const task_type_t taskType,
                     const hrtime_t runtime, rel_time_t startRelTime,
                     bool isSlowJob);

    const std::vector<TaskLogEntry> getLog() {
        LockHolder lh(logMutex);
        return tasklog.contents();
    }

    const std::vector<TaskLogEntry> getSlowLog() {
        LockHolder lh(logMutex);
        return slowjobs.contents();
    }

    struct timeval getWaketime(void) { return waketime; }

    struct timeval getCurTime(void) { return now; }

private:

    cb_thread_t thread;
    ExecutorPool *manager;
    int startIndex;
    const std::string name;
    AtomicValue<executor_state_t> state;

    struct  timeval    now;  // record of current time
    struct timeval waketime; // set to the earliest

    hrtime_t taskStart;
    ExTask currentTask;
    task_type_t curTaskType;

    Mutex logMutex;
    RingBuffer<TaskLogEntry> tasklog;
    RingBuffer<TaskLogEntry> slowjobs;
};

#endif  // SRC_SCHEDULER_H_
