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
#include "ringbuffer.h"
#include "tasks.h"

#define TASK_LOG_SIZE 20
#define MIN_SLEEP_TIME 2.0

class ExecutorPool;
class ExecutorThread;
class WorkLoadPolicy;

typedef enum {
    EXECUTOR_CREATING,
    EXECUTOR_RUNNING,
    EXECUTOR_WAITING,
    EXECUTOR_SLEEPING,
    EXECUTOR_SHUTDOWN,
    EXECUTOR_DEAD
} executor_state_t;

typedef enum {
    WRITER_TASK_IDX=0,
    READER_TASK_IDX=1,
    AUXIO_TASK_IDX=2,
    NO_TASK_TYPE=-1
} task_type_t;

/**
 * Log entry for previous job runs.
 */
class TaskLogEntry {
public:

    // This is useful for the ringbuffer to initialize
    TaskLogEntry() : name("invalid"), duration(0) {}
    TaskLogEntry(const std::string &n, const hrtime_t d, rel_time_t t = 0)
        : name(n), ts(t), duration(d) {}

    /**
     * Get the name of the job.
     */
    std::string getName() const { return name; }

    /**
     * Get the amount of time (in microseconds) this job ran.
     */
    hrtime_t getDuration() const { return duration; }

    /**
     * Get a timestamp indicating when this thing started.
     */
    rel_time_t getTimestamp() const { return ts; }

private:
    std::string name;
    rel_time_t ts;
    hrtime_t duration;
};

class TaskQueue {
public:
    TaskQueue(ExecutorPool *m, task_type_t t, const char *nm) :
    name(nm), hasWokenTask(false), queueType(t), manager(m) { }

    ~TaskQueue(void) {
        LOG(EXTENSION_LOG_INFO, "Task Queue killing %s", name.c_str());
    }

    void schedule(ExTask &task);

    struct timeval reschedule(ExTask &task);

    bool fetchNextTask(ExTask &task, struct timeval &tv, int &taskIdx,
                       struct timeval now);

    void wake(ExTask &task);

    static const std::string taskType2Str(task_type_t type);

    const std::string getName() const {
        return (name+taskType2Str(queueType));
    }
private:

    bool empty(void) { return readyQueue.empty() && futureQueue.empty(); }

    void moveReadyTasks(struct timeval tv);

    void pushReadyTask(ExTask &tid);

    ExTask popReadyTask(void);

    SyncObject mutex;
    const std::string name;

    bool hasWokenTask;
    task_type_t queueType;

    ExecutorPool *manager;

    // sorted by task priority then waketime ..
    std::priority_queue<ExTask, std::deque<ExTask >,
                        CompareByPriority> readyQueue;
    std::priority_queue<ExTask, std::deque<ExTask >,
                        CompareByDueDate> futureQueue;
};

class ExecutorThread {
    friend class ExecutorPool;
public:

    ExecutorThread(ExecutorPool *m, size_t startingQueue, const std::string nm)
        : manager(m), startIndex(startingQueue), name(nm),
          state(EXECUTOR_CREATING), taskStart(0),
          tasklog(TASK_LOG_SIZE), slowjobs(TASK_LOG_SIZE), currentTask(NULL),
          curTaskType(-1) { set_max_tv(waketime); }

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

    const std::vector<TaskLogEntry> getLog() { return tasklog.contents(); }

    const std::vector<TaskLogEntry> getSlowLog() { return slowjobs.contents();}

private:

    pthread_t thread;
    ExecutorPool *manager;
    size_t startIndex;
    const std::string name;
    executor_state_t state;

    struct timeval waketime; // set to the earliest

    hrtime_t taskStart;
    RingBuffer<TaskLogEntry> tasklog;
    RingBuffer<TaskLogEntry> slowjobs;

    ExTask currentTask;
    int curTaskType;
};

typedef std::vector<ExecutorThread *> ThreadQ;
typedef std::pair<ExTask, TaskQueue *> TaskQpair;
typedef std::pair<RingBuffer<TaskLogEntry>*, RingBuffer<TaskLogEntry> *>
                                                                TaskLog;
typedef std::vector<TaskQueue *> TaskQ;

class ExecutorPool {
public:

    void moreWork(void);

    void lessWork(void);

    void doneWork(int &doneTaskType);

    int tryNewWork(int newTaskType);

    bool trySleep(ExecutorThread &t, struct timeval &now);

    TaskQueue *nextTask(ExecutorThread &t, uint8_t tick);

    bool cancel(size_t taskId, bool eraseTask=false);

    bool wake(size_t taskId);

    void notifyOne(void);

    bool snooze(size_t taskId, double tosleep);

    void registerBucket(EventuallyPersistentEngine *engine);

    void unregisterBucket(EventuallyPersistentEngine *engine);

    void doWorkerStat (EventuallyPersistentEngine *engine, const void *cookie,
                       ADD_STAT add_stat);
protected:

    ExecutorPool(size_t m, size_t nTaskSets);
    ~ExecutorPool(void);

    bool startWorkers(WorkLoadPolicy &workload);
    size_t schedule(ExTask task, task_type_t qidx);

    size_t     numReadyTasks;
    size_t     highWaterMark; // High Water Mark for num Ready Tasks
    SyncObject mutex; // Thread management condition var + mutex
    // sync: numReadyTasks, highWaterMark, defaultQ

    //! A mapping of task ids to Task, TaskQ in the thread pool
    std::map<size_t, TaskQpair> taskLocator;

    //A list of threads
    ThreadQ threadQ;

    // Global cross bucket priority queues where tasks get scheduled into ...
    TaskQ hpTaskQ; // a vector array of numTaskSets elements for high priority
    bool isHiPrioQset;

    TaskQ lpTaskQ; // a vector array of numTaskSets elements for low priority
    bool isLowPrioQset;

    size_t numBuckets;

    SyncObject tMutex; // to serialize taskLocator, threadQ, numBuckets access

    uint16_t *curWorkers; // for every TaskSet track its no. of worker threads
    uint16_t *maxWorkers; // and limit it to the value set here

    size_t numTaskSets; // safe to read lock-less not altered after creation
    size_t maxIOThreads;
};
#endif  // SRC_SCHEDULER_H_
