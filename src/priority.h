/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_PRIORITY_H_
#define SRC_PRIORITY_H_ 1

#include "config.h"

#include "utility.h"

enum type_id_t {
    BGFETCHER_ID=0,
    BGFETCHER_GET_META_ID,
    TAP_BGFETCHER_ID,
    VKEY_STAT_BGFETCHER_ID,
    WARMUP_ID,
    VBUCKET_PERSIST_HIGH_ID,
    VBUCKET_DELETION_ID,
    FLUSHER_ID,
    FLUSHALL_ID,
    COMPACTOR_ID,
    VBUCKET_PERSIST_LOW_ID,
    STAT_SNAP_ID,
    MUTATION_LOG_COMPACTOR_ID,
    ACCESS_SCANNER_ID,
    TAP_CONN_NOTIFICATION_ID,
    CHECKPOINT_REMOVER_ID,
    VB_MEMORY_DELETION_ID,
    CHECKPOINT_STATS_ID,
    ITEM_PAGER_ID,
    BACKFILL_TASK_ID,
    WORKLOAD_MONITOR_TASK_ID,
    TAP_RESUME_ID,
    TAP_CONNECTION_REAPER_ID,
    HT_RESIZER_ID,
    PENDING_OPS_ID,
    TAP_CONN_MGR_ID,
    DEFRAGMENTER_ID,

    MAX_TYPE_ID // Keep this as the last enum value
};

/**
 * Task priority definition.
 */
class Priority {
public:
    // Priorities for Read-only tasks
    static const Priority BgFetcherPriority;
    static const Priority BgFetcherGetMetaPriority;
    static const Priority TapBgFetcherPriority;
    static const Priority VKeyStatBgFetcherPriority;
    static const Priority WarmupPriority;

    // Priorities for Read-Write tasks
    static const Priority VBucketPersistHighPriority;
    static const Priority VBucketDeletionPriority;
    static const Priority FlusherPriority;
    static const Priority FlushAllPriority;
    static const Priority CompactorPriority;
    static const Priority VBucketPersistLowPriority;
    static const Priority StatSnapPriority;
    static const Priority MutationLogCompactorPriority;
    static const Priority AccessScannerPriority;

    // Priorities for NON-IO tasks
    static const Priority TapConnNotificationPriority;
    static const Priority CheckpointRemoverPriority;
    static const Priority VBMemoryDeletionPriority;
    static const Priority CheckpointStatsPriority;
    static const Priority ItemPagerPriority;
    static const Priority BackfillTaskPriority;
    static const Priority WorkLoadMonitorPriority;
    static const Priority TapResumePriority;
    static const Priority TapConnectionReaperPriority;
    static const Priority HTResizePriority;
    static const Priority PendingOpsPriority;
    static const Priority TapConnMgrPriority;
    static const Priority DefragmenterTaskPriority;

    bool operator==(const Priority &other) const {
        return other.getPriorityValue() == this->priority;
    }

    bool operator<(const Priority &other) const {
        return this->priority > other.getPriorityValue();
    }

    bool operator>(const Priority &other) const {
        return this->priority < other.getPriorityValue();
    }

    /**
     * Return the task name.
     *
     * @return a task name
     */
    static const char *getTypeName(const type_id_t i);

    /**
     * Return the type id representing a task
     *
     * @return type id
     */
    type_id_t getTypeId() const {
        return t_id;
    }

    /**
     * Return an integer value that represents a priority.
     *
     * @return a priority value
     */
    int getPriorityValue() const {
        return priority;
    }

    // gcc didn't like the idea of having a class with no constructor
    // available to anyone.. let's make it protected instead to shut
    // gcc up :(
protected:
    Priority(type_id_t id, int p) : t_id(id), priority(p) { }
    type_id_t t_id;
    int priority;

private:
    DISALLOW_COPY_AND_ASSIGN(Priority);
};

#endif  // SRC_PRIORITY_H_
