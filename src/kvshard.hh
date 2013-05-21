#ifndef KVSHARD_HH
#define KVSHARD_HH 1

#include <cassert>

#include <map>
#include <vector>
#include <set>
#include <sstream>
#include <algorithm>

#include <memcached/engine.h>
#include "kvstore.hh"
#include "bgfetcher.hh"

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

private:
    RCPtr<VBucket> *vbuckets;

    KVStore    *rwUnderlying;
    KVStore    *roUnderlying;

    Flusher    *flusher;
    BgFetcher  *bgFetcher;

    size_t maxVbuckets;
    uint16_t shardId;
    Atomic<bool> highPrioritySnapshot;
    Atomic<bool> lowPrioritySnapshot;

public:
    Atomic<size_t> highPriorityCount;

    DISALLOW_COPY_AND_ASSIGN(KVShard);
};

#endif /* KVSHARD_HH */
