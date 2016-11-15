/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#ifndef SRC_TASKS_H_
#define SRC_TASKS_H_ 1

#include "config.h"

#include <platform/processclock.h>

#include <array>
#include <chrono>
#include <string>
#include "atomic.h"
#include "kvstore.h"

enum task_state_t {
    TASK_RUNNING,
    TASK_SNOOZED,
    TASK_DEAD
};

enum class TaskId : int {
#define TASK(name, prio) name,
#include "tasks.def.h"
#undef TASK
    TASK_COUNT
};

typedef int queue_priority_t;

enum class TaskPriority : int {
#define TASK(name, prio) name = prio,
#include "tasks.def.h"
#undef TASK
    PRIORITY_COUNT
};

class BgFetcher;
class CompareTasksByDueDate;
class CompareTasksByPriority;
class EventuallyPersistentEngine;
class Flusher;
class Warmup;
class Taskable;

class GlobalTask : public RCValue {
friend class CompareByDueDate;
friend class CompareByPriority;
friend class ExecutorPool;
friend class ExecutorThread;
friend class TaskQueue;
public:

    GlobalTask(Taskable& t,
               TaskId taskId,
               double sleeptime = 0,
               bool completeBeforeShutdown = true);

    GlobalTask(EventuallyPersistentEngine *e,
               TaskId taskId,
               double sleeptime = 0,
               bool completeBeforeShutdown = true);

    /* destructor */
    virtual ~GlobalTask(void) {}

    /**
     * The invoked function when the task is executed.
     *
     * @return Whether or not this task should be rescheduled
     */
    virtual bool run(void) = 0;

    /**
     * Gives a description of this task.
     *
     * @return A description of this task
     */
    virtual std::string getDescription(void) = 0;

    virtual int maxExpectedDuration(void) {
        return 3600;
    }

    /**
     * test if a task is dead
     */
    bool isdead(void) {
        return (state == TASK_DEAD);
    }

    /**
     * Cancels this task by marking it dead.
     */
    void cancel(void) {
        state = TASK_DEAD;
    }

    /**
     * Puts the task to sleep for a given duration.
     */
    virtual void snooze(const double secs);

    /**
     * Returns the id of this task.
     *
     * @return A unique task id number.
     */
    size_t getId() const { return uid; }

    /**
     * Returns the type id of this task.
     *
     * @return A type id of the task.
     */
    TaskId getTypeId() const { return typeId; }

    /**
     * Gets the engine that this task was scheduled from
     *
     * @returns A handle to the engine
     */
    EventuallyPersistentEngine* getEngine() { return engine; }

    task_state_t getState(void) {
        return state.load();
    }

    void setState(task_state_t tstate, task_state_t expected) {
        state.compare_exchange_strong(expected, tstate);
    }

    Taskable& getTaskable() {
        return taskable;
    }

    queue_priority_t getQueuePriority() const {
        return static_cast<queue_priority_t>(priority);
    }

    /*
     * Lookup the task name for TaskId id.
     * The data used is generated from tasks.def.h
     */
    static const char* getTaskName(TaskId id);

    /*
     * Lookup the task priority for TaskId id.
     * The data used is generated from tasks.def.h
     */
    static TaskPriority getTaskPriority(TaskId id);

    /*
     * A vector of all TaskId generated from tasks.def.h
     */
    static std::array<TaskId, static_cast<int>(TaskId::TASK_COUNT)> allTaskIds;

protected:
    bool blockShutdown;
    AtomicValue<task_state_t> state;
    const size_t uid;
    TaskId typeId;
    TaskPriority priority;
    EventuallyPersistentEngine *engine;
    Taskable& taskable;

    static AtomicValue<size_t> task_id_counter;
    static size_t nextTaskId() { return task_id_counter.fetch_add(1); }

    ProcessClock::time_point getWaketime() const {
        const auto waketime_chrono = std::chrono::nanoseconds(waketime);
        return ProcessClock::time_point(waketime_chrono);
    }

    void updateWaketime(const ProcessClock::time_point tp) {
        waketime = to_ns_since_epoch(tp).count();
    }

    void updateWaketimeIfLessThan(const ProcessClock::time_point tp) {
        const auto tp_ns = to_ns_since_epoch(tp).count();
        if (tp_ns > waketime) {
            waketime = tp_ns;
        }
    }

private:
    /**
     * We are using a uint64_t as opposed to ProcessTime::time_point because
     * was want the access to be atomic without the use of a mutex.
     * The reason for this is that the CompareByDueDate function has been shown
     * to be pretty hot and we want to avoid the overhead of acquiring
     * two mutexes (one for ExTask 1 and one for ExTask 2) for every invocation
     * of the function.
     */
    std::atomic<int64_t> waketime; // used for priority_queue
};

typedef SingleThreadedRCPtr<GlobalTask> ExTask;

/**
 * A task for persisting items to disk.
 */
class FlusherTask : public GlobalTask {
public:
    FlusherTask(EventuallyPersistentEngine *e, Flusher* f, uint16_t shardid,
                bool completeBeforeShutdown = true)
        : GlobalTask(e, TaskId::FlusherTask, 0, completeBeforeShutdown),
          flusher(f) {
        std::stringstream ss;
        ss<<"Running a flusher loop: shard "<<shardid;
        desc = ss.str();
    }

    bool run();

    std::string getDescription() {
        return desc;
    }

private:
    Flusher* flusher;
    std::string desc;
};

/**
 * A task for deleting VBucket files from disk and cleaning up any outstanding
 * writes for that VBucket file.
 * sid (shard ID) passed on to GlobalTask indicates that task needs to be
 *     serialized with other tasks that require serialization on its shard
 */
class VBDeleteTask : public GlobalTask {
public:
    VBDeleteTask(EventuallyPersistentEngine *e, uint16_t vbid, const void* c,
                 bool completeBeforeShutdown = true)
        : GlobalTask(e, TaskId::VBDeleteTask, 0, completeBeforeShutdown),
          vbucketId(vbid), cookie(c) {}

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Deleting VBucket:"<<vbucketId;
        return ss.str();
    }

private:
    uint16_t vbucketId;
    const void* cookie;
};

/**
 * A task for compacting a vbucket db file
 */
class CompactTask : public GlobalTask {
public:
    CompactTask(EventuallyPersistentEngine *e,
                compaction_ctx c, const void *ck,
                bool completeBeforeShutdown = false) :
                GlobalTask(e, TaskId::CompactVBucketTask, 0, completeBeforeShutdown),
                           compactCtx(c), cookie(ck) {
        desc = "Compact DB file " + std::to_string(c.db_file_id);
    }

    bool run();

    std::string getDescription() {
        return desc;
    }

private:
    compaction_ctx compactCtx;
    const void* cookie;
    std::string desc;
};

/**
 * A task that periodically takes a snapshot of the stats and persists them to
 * disk.
 */
class StatSnap : public GlobalTask {
public:
    StatSnap(EventuallyPersistentEngine *e, bool runOneTimeOnly = false,
             bool sleeptime = 0, bool completeBeforeShutdown = false)
        : GlobalTask(e, TaskId::StatSnap, sleeptime, completeBeforeShutdown),
          runOnce(runOneTimeOnly) {}

    bool run();

    std::string getDescription() {
        std::string rv("Updating stat snapshot on disk");
        return rv;
    }

private:
    bool runOnce;
};

/**
 * A task for fetching items from disk.
 * This task is used if EventuallyPersistentStore::multiBGFetchEnabled is true.
 */
class MultiBGFetcherTask : public GlobalTask {
public:
    MultiBGFetcherTask(EventuallyPersistentEngine *e, BgFetcher *b, bool sleeptime = 0,
                        bool completeBeforeShutdown = false)
        : GlobalTask(e, TaskId::MultiBGFetcherTask, sleeptime, completeBeforeShutdown),
          bgfetcher(b) {}

    bool run();

    std::string getDescription() {
        return std::string("Batching background fetch");
    }

private:
    BgFetcher *bgfetcher;
};

/**
 * A task that performs the bucket flush operation.
 */
class FlushAllTask : public GlobalTask {
public:
    FlushAllTask(EventuallyPersistentEngine *e, double when)
        : GlobalTask(e, TaskId::FlushAllTask, when, false) {}

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss << "Performing flush_all operation.";
        return ss.str();
    }
};

/**
 * A task for performing disk fetches for "stats vkey".
 */
class VKeyStatBGFetchTask : public GlobalTask {
public:
    VKeyStatBGFetchTask(EventuallyPersistentEngine *e, const std::string &k,
                        uint16_t vbid, uint64_t s, const void *c, int sleeptime = 0,
                        bool completeBeforeShutdown = false)
        : GlobalTask(e, TaskId::VKeyStatBGFetchTask, sleeptime, completeBeforeShutdown),
          key(k),
          vbucket(vbid),
          bySeqNum(s),
          cookie(c) {}

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss << "Fetching item from disk for vkey stat:  " << key<<" vbucket "
           <<vbucket;
        return ss.str();
    }

private:
    std::string                      key;
    uint16_t                         vbucket;
    uint64_t                         bySeqNum;
    const void                      *cookie;
};

/**
 * A task that performs disk fetches for non-resident get requests.
 * This task is used if EventuallyPersistentStore::multiBGFetchEnabled is false.
 */
class SingleBGFetcherTask : public GlobalTask {
public:
    SingleBGFetcherTask(EventuallyPersistentEngine *e, const std::string &k,
                       uint16_t vbid, const void *c, bool isMeta,
                       int sleeptime = 0, bool completeBeforeShutdown = false)
        : GlobalTask(e, TaskId::SingleBGFetcherTask, sleeptime, completeBeforeShutdown),
          key(k),
          vbucket(vbid),
          cookie(c),
          metaFetch(isMeta),
          init(gethrtime()) {}

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss << "Fetching item from disk:  " << key<<" vbucket "<<vbucket;
        return ss.str();
    }

private:
    const std::string          key;
    uint16_t                   vbucket;
    const void                *cookie;
    bool                       metaFetch;
    hrtime_t                   init;
};

/**
 * A task that monitors if a bucket is read-heavy, write-heavy, or mixed.
 */
class WorkLoadMonitor : public GlobalTask {
public:
    WorkLoadMonitor(EventuallyPersistentEngine *e,
                    bool completeBeforeShutdown = false);

    bool run();

    std::string getDescription() {
        return desc;
    }

private:

    size_t getNumMutations();
    size_t getNumGets();

    size_t prevNumMutations;
    size_t prevNumGets;
    std::string desc;
};

/**
 * Order tasks by their priority and taskId (try to ensure FIFO)
 * @return true if t2 should have priority over t1
 */
class CompareByPriority {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return (t1->getQueuePriority() == t2->getQueuePriority()) ?
               (t1->uid > t2->uid) :
               (t1->getQueuePriority() > t2->getQueuePriority());
    }
};

/**
 * Order tasks by their ready date.
 * @return true if t2 should have priority over t1
 */
class CompareByDueDate {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return t2->waketime < t1->waketime;
    }
};

#endif  // SRC_TASKS_H_
