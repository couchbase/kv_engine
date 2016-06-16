/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#ifndef SRC_TASKS_H_
#define SRC_TASKS_H_ 1

#include "config.h"

#include <list>
#include <string>
#include <utility>

#include "atomic.h"
#include "priority.h"

typedef enum {
    TASK_RUNNING,
    TASK_SNOOZED,
    TASK_DEAD
} task_state_t;

class BgFetcher;
class CompareTasksByDueDate;
class CompareTasksByPriority;
class EventuallyPersistentEngine;
class Flusher;
class Warmup;

/**
 * Compaction context to perform compaction
 */

typedef struct {
    uint64_t revSeqno;
    std::string keyStr;
} expiredItemCtx;

typedef struct {
    uint64_t purge_before_ts;
    uint64_t purge_before_seq;
    uint8_t  drop_deletes;
    uint64_t max_purged_seq;
    uint32_t curr_time;
    std::list<expiredItemCtx> expiredItems;
} compaction_ctx;

class GlobalTask : public RCValue {
friend class CompareByDueDate;
friend class CompareByPriority;
friend class ExecutorPool;
friend class ExecutorThread;
friend class TaskQueue;
public:
    GlobalTask(EventuallyPersistentEngine *e, const Priority &p,
               double sleeptime = 0, bool completeBeforeShutdown = true) :
          RCValue(), priority(p),
          blockShutdown(completeBeforeShutdown),
          state(TASK_RUNNING), taskId(nextTaskId()), engine(e) {
        snooze(sleeptime);
    }

    /* destructor */
    virtual ~GlobalTask(void) {
    }

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
    size_t getId() { return taskId; }

    /**
     * Returns the type id of this task.
     *
     * @return A type id of the task.
     */
    type_id_t getTypeId() { return priority.getTypeId(); }

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

protected:
    const Priority &priority;
    bool blockShutdown;
    AtomicValue<task_state_t> state;
    const size_t taskId;
    EventuallyPersistentEngine *engine;

    static AtomicValue<size_t> task_id_counter;
    static size_t nextTaskId() { return task_id_counter.fetch_add(1); }

    hrtime_t getWaketime() {
        return waketime.load();
    }

    void updateWaketime(hrtime_t to) {
        waketime.store(to);
    }

    void updateWaketimeIfLessThan(hrtime_t to) {
        atomic_setIfBigger(waketime, to);
    }

private:
    AtomicValue<hrtime_t> waketime;      // used for priority_queue
};

typedef SingleThreadedRCPtr<GlobalTask> ExTask;

/**
 * A task for persisting items to disk.
 */
class FlusherTask : public GlobalTask {
public:
    FlusherTask(EventuallyPersistentEngine *e, Flusher* f, const Priority &p,
                uint16_t shardid, bool completeBeforeShutdown = true) :
                GlobalTask(e, p, 0, completeBeforeShutdown), flusher(f) {
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
 * A task for persisting VBucket state changes to disk and creating new
 * VBucket database files.
 * sid (shard ID) passed on to GlobalTask indicates that task needs to be
 *     serialized with other tasks that require serialization on its shard
 */
class VBSnapshotTask : public GlobalTask {
public:
    VBSnapshotTask(EventuallyPersistentEngine *e, const Priority &p,
                uint16_t sID = 0, bool completeBeforeShutdown = true) :
                GlobalTask(e, p, 0, completeBeforeShutdown), shardID(sID) {
        std::stringstream ss;
        ss<<"Snapshotting vbucket states for the shard: "<<shardID;
        desc = ss.str();
    }

    bool run();

    std::string getDescription() {
        return desc;
    }

private:
    uint16_t shardID;
    std::string desc;
};

/**
 * A daemon task for persisting VBucket state changes to disk periodically.
 */
class DaemonVBSnapshotTask : public GlobalTask {
public:
    DaemonVBSnapshotTask(EventuallyPersistentEngine *e,
                         bool completeBeforeShutdown = true);

    bool run();

    std::string getDescription() {
        return desc;
    }

private:
    std::string desc;
};

/**
 * A task for persisting a VBucket state to disk and creating a vbucket
 * database file if necessary.
 */
class VBStatePersistTask : public GlobalTask {
public:
    VBStatePersistTask(EventuallyPersistentEngine *e, const Priority &p,
                       uint16_t vbucket, bool completeBeforeShutdown = true) :
        GlobalTask(e, p, 0, completeBeforeShutdown), vbid(vbucket) {
        std::stringstream ss;
        ss<<"Persisting a vbucket state for vbucket: "<< vbid;
        desc = ss.str();
    }

    bool run();

    std::string getDescription() {
        return desc;
    }

private:
    uint16_t vbid;
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
                 const Priority &p, bool completeBeforeShutdown = true) :
        GlobalTask(e, p, 0, completeBeforeShutdown),
        vbucketId(vbid), cookie(c) { }

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
class CompactVBucketTask : public GlobalTask {
public:
    CompactVBucketTask(EventuallyPersistentEngine *e, const Priority &p,
                uint16_t vbucket, compaction_ctx c, const void *ck,
                bool completeBeforeShutdown = false) :
                GlobalTask(e, p, 0, completeBeforeShutdown),
                           vbid(vbucket), compactCtx(c), cookie(ck)
    {
        std::stringstream ss;
        ss<<"Compact VBucket "<<vbid;
        desc = ss.str();
    }

    bool run();

    std::string getDescription() {
        return desc;
    }

private:
    uint16_t vbid;
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
    StatSnap(EventuallyPersistentEngine *e, const Priority &p,
             bool runOneTimeOnly = false, bool sleeptime = 0,
             bool completeBeforeShutdown = false) :
        GlobalTask(e, p, sleeptime, completeBeforeShutdown),
        runOnce(runOneTimeOnly) { }

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
 */
class BgFetcherTask : public GlobalTask {
public:
    BgFetcherTask(EventuallyPersistentEngine *e, BgFetcher *b,
                  const Priority &p, bool sleeptime = 0,
                  bool completeBeforeShutdown = false) :
        GlobalTask(e, p, sleeptime, completeBeforeShutdown),
        bgfetcher(b) { }

    bool run();

    std::string getDescription() {
        return std::string("Batching background fetch");
    }

private:
    BgFetcher *bgfetcher;
};

/**
 * A task for performing disk fetches for "stats vkey".
 */
class VKeyStatBGFetchTask : public GlobalTask {
public:
    VKeyStatBGFetchTask(EventuallyPersistentEngine *e, const std::string &k,
                        uint16_t vbid, uint64_t s, const void *c,
                        const Priority &p, int sleeptime = 0,
                        bool completeBeforeShutdown = false) :
        GlobalTask(e, p, sleeptime, completeBeforeShutdown), key(k),
                   vbucket(vbid), bySeqNum(s), cookie(c) { }

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
 */
class BGFetchTask : public GlobalTask {
public:
    BGFetchTask(EventuallyPersistentEngine *e, const std::string &k,
            uint16_t vbid, uint64_t s, const void *c, bool isMeta,
            const Priority &p, int sleeptime = 0,
            bool completeBeforeShutdown = false) :
        GlobalTask(e, p, sleeptime, completeBeforeShutdown),
        key(k), vbucket(vbid), seqNum(s), cookie(c), metaFetch(isMeta),
        init(gethrtime()) { }

    bool run();

    std::string getDescription() {
        std::stringstream ss;
        ss << "Fetching item from disk:  " << key<<" vbucket "<<vbucket;
        return ss.str();
    }

private:
    const std::string          key;
    uint16_t                   vbucket;
    uint64_t                   seqNum;
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
 */
class CompareByPriority {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return (t1->priority == t2->priority) ?
               (t1->taskId   > t2->taskId)    :
               (t1->priority < t2->priority);
    }
};

/**
 * Order tasks by their ready date.
 */
class CompareByDueDate {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return t2->waketime < t1->waketime;
    }
};

#endif  // SRC_TASKS_H_
