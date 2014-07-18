/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_KVSHARD_H_
#define SRC_KVSHARD_H_ 1

#include "config.h"

#include <memcached/engine.h>

#include <algorithm>
#include <map>
#include <set>
#include <sstream>
#include <vector>

#include "bgfetcher.h"
#include "kvstore.h"


/**
 * Base class encapsulating individual couchstore(vbucket) into a
 * logical group representing underlying storage operations
 *
 * KVShard(Shard) is the highest level abstraction of underlying
 * storage partitions used within the EventuallyPersistentEngine(ep)
 * and the global I/O Task Manager(iom). It gathers a collection
 * of logical partition(vbucket) into single data access administrative
 * unit for multiple data access dispatchers(threads)
 *
 *   (EP) ---> (VBucketMap) ---> Shards[0...N]
 *
 *   Shards[n]:
 *   ------------------------KVShard----
 *   | shardId: uint16_t(n)            |
 *   | highPrioritySnapshot: bool      |
 *   | lowPrioritySnapshot: bool       |
 *   |                                 |
 *   | vbuckets: VBucket[] (partitions)|----> [(VBucket),(VBucket)..]
 *   |                                 |
 *   | flusher: Flusher                |
 *   | BGFetcher: bgFetcher            |
 *   |                                 |
 *   | rwUnderlying: KVStore (write)   |----> (CouchKVStore)
 *   | roUnderlying: KVStore (read)    |----> (CouchKVStore)
 *   -----------------------------------
 *
 */
class Flusher;

class KVShard {
    friend class VBucketMap;
public:
    KVShard(uint16_t id, EventuallyPersistentStore &store);
    ~KVShard();

    KVStore *getRWUnderlying();
    KVStore *getROUnderlying();

    Flusher *getFlusher();
    BgFetcher *getBgFetcher();

    RCPtr<VBucket> getBucket(uint16_t id) const;
    void setBucket(const RCPtr<VBucket> &b);
    void resetBucket(uint16_t id);

    uint16_t getId() { return shardId; }
    std::vector<int> getVBucketsSortedByState();
    std::vector<int> getVBuckets();
    size_t getMaxNumVbuckets() { return maxVbuckets; }

    /**
     * Set the flag to coordinate the scheduled high priority vbucket
     * snapshot and new snapshot requests with the high priority. The
     * flag is "true" if a snapshot task with the high priority is
     * currently scheduled, otherwise "false".  If (1) the flag is
     * currently "false" and (2) a new snapshot request invokes this
     * method by passing "true" parameter, this will set the flag to
     * "true" and return "true" to indicate that the new request can
     * be scheduled now. Otherwise, return "false" to prevent
     * duplciate snapshot tasks from being scheduled.  When the
     * snapshot task is running and about to writing to disk, it will
     * invoke this method to reset the flag by passing "false"
     * parameter.
     *
     * @param highPrioritySnapshot bool flag for coordination between
     *                             the scheduled snapshot task and new
     *                             snapshot requests.
     * @return "true" if a flag's value was changed. Otherwise "false".
     */
    bool setHighPriorityVbSnapshotFlag(bool highPrioritySnapshot);
    bool getHighPriorityVbSnapshotFlag(void) {
        return highPrioritySnapshot;
    }

    /**
     * Set the flag to coordinate the scheduled low priority vbucket
     * snapshot and new snapshot requests with the low priority. The
     * flag is "true" if a snapshot task with the low priority is
     * currently scheduled, otherwise "false".  If (1) the flag is
     * currently "false" and (2) a new snapshot request invokes this
     * method by passing "true" parameter, this will set the flag to
     * "true" and return "true" to indicate that the new request can
     * be scheduled now. Otherwise, return "false" to prevent
     * duplciate snapshot tasks from being scheduled.  When the
     * snapshot task is running and about to writing to disk, it will
     * invoke this method to reset the flag by passing "false"
     * parameter.
     *
     * @param lowPrioritySnapshot bool flag for coordination between
     *                             the scheduled low priority snapshot
     *                             task and new snapshot requests with
     *                             low priority.
     *
     * @return "true" if a flag's value was changed. Otherwise
     *                "false".
     */
    bool setLowPriorityVbSnapshotFlag(bool lowPrioritySnapshot);
    bool getLowPriorityVbSnapshotFlag(void) {
        return lowPrioritySnapshot;
    }

    /**
     * Return the number of non-deleted items from a given vbucket database
     * @param vbid vbucket from which the number of items is retrived
     * @return the number of non-deleted items from a given vbucket database
     */
    size_t getNumItemsOnDisk(uint16_t vbid) {
        return rwUnderlying->getNumItems(vbid);
    }

    bool tryLockShardTask(ExTask &task) {
        if (!opLock) {
            opLock = true;
            return true;
        } else { // block the task in a pendingQueue
            pendingQueue.push_back(task);
        }
        return false;
    }

    ExTask unlockShardTask(void) {
        ExTask retVal;
        opLock = false;
        if (!pendingQueue.empty()) { // return any blocked shard serial task
            retVal = pendingQueue.front();
            pendingQueue.pop_front();
        } else {
            retVal = NULL;
        }
        return retVal;
    }

    size_t getPendingTaskCount(void) {
        return pendingQueue.size();
    }

    bool isShardLock(void) {
        return opLock;
    }

private:
    RCPtr<VBucket> *vbuckets;

    KVStore    *rwUnderlying;
    KVStore    *roUnderlying;

    Flusher    *flusher;
    BgFetcher  *bgFetcher;

    size_t maxVbuckets;
    uint16_t shardId;

    bool opLock; // Used by ExecutoPool infrastructure to serialize operations
    std::list<ExTask> pendingQueue;

    AtomicValue<bool> highPrioritySnapshot;
    AtomicValue<bool> lowPrioritySnapshot;

public:
    AtomicValue<size_t> highPriorityCount;

    DISALLOW_COPY_AND_ASSIGN(KVShard);
};

#endif  // SRC_KVSHARD_H_
