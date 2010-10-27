/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef VBUCKET_HH
#define VBUCKET_HH 1

#include <cassert>

#include <map>
#include <vector>
#include <sstream>
#include <algorithm>

#include <memcached/engine.h>

#include "common.hh"
#include "atomic.hh"
#include "stored-value.hh"

const size_t BASE_VBUCKET_SIZE=1024;

/**
 * Function object that returns true if the given vbucket is acceptable.
 */
class VBucketFilter {
public:

    /**
     * Instiatiate a VBucketFilter that always returns true.
     */
    explicit VBucketFilter() : acceptable() {}

    /**
     * Instantiate a VBucketFilter that returns true for any of the
     * given vbucket IDs.
     */
    explicit VBucketFilter(std::vector<uint16_t> a) : acceptable(a) {
        std::sort(acceptable.begin(), acceptable.end());
    }

    bool operator ()(uint16_t v) const {
        return acceptable.empty() || std::binary_search(acceptable.begin(),
                                                        acceptable.end(), v);
    }

    size_t size() const { return acceptable.size(); }

    bool empty() const { return acceptable.empty(); }

    /**
     * Calculate the difference between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [1,2,5,6]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in only one of the two
     *         filters.
     */
    VBucketFilter filter_diff(const VBucketFilter &other) const;

    /**
     * Calculate the intersection between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [3,4]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in both of the two
     *         filters.
     */
    VBucketFilter filter_intersection(const VBucketFilter &other) const;

    const std::vector<uint16_t> &getVector() const { return acceptable; }

    /**
     * Dump the filter in a human readable form ( "{ bucket, bucket, bucket }"
     * to the specified output stream.
     */
    friend std::ostream& operator<< (std::ostream& out,
                                     const VBucketFilter &filter);

private:

    std::vector<uint16_t> acceptable;
};

/**
 * An individual vbucket.
 */
class VBucket : public RCValue {
public:

    VBucket(int i, vbucket_state_t initialState, EPStats &st) :
        ht(st), id(i), state(initialState), stats(st) {
        pendingOpsStart = 0;
        stats.memOverhead.incr(sizeof(VBucket)
                               + ht.memorySize());
        assert(stats.memOverhead.get() < GIGANTOR);
    }

    ~VBucket() {
        if (!pendingOps.empty()) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Have %d pending ops while destroying vbucket\n",
                             pendingOps.size());
        }
        stats.memOverhead.decr(sizeof(VBucket) + ht.memorySize());
        assert(stats.memOverhead.get() < GIGANTOR);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Destroying vbucket %d\n", id);
    }

    int getId(void) const { return id; }
    vbucket_state_t getState(void) const { return state; }
    void setState(vbucket_state_t to, SERVER_HANDLE_V1 *sapi);

    const char * getStateString(void) const {
        return VBucket::toString(state);
    }

    bool addPendingOp(const void *cookie) {
        LockHolder lh(pendingOpLock);
        if (state != pending) {
            // State transitioned while we were waiting.
            return false;
        }
        // Start a timer when enqueuing the first client.
        if (pendingOps.empty()) {
            pendingOpsStart = gethrtime();
        }
        pendingOps.push_back(cookie);
        ++stats.pendingOps;
        ++stats.pendingOpsTotal;
        return true;
    }

    void fireAllOps(SERVER_HANDLE_V1 *sapi);

    size_t size(void) {
        HashTableDepthStatVisitor v;
        ht.visitDepth(v);
        return v.size;
    }

    HashTable ht;

    static const char* toString(vbucket_state_t s) {
        switch(s) {
        case active: return "active"; break;
        case replica: return "replica"; break;
        case pending: return "pending"; break;
        case dead: return "dead"; break;
        }
        return "unknown";
    }

    static const vbucket_state_t ACTIVE;
    static const vbucket_state_t REPLICA;
    static const vbucket_state_t PENDING;
    static const vbucket_state_t DEAD;

private:

    void fireAllOps(SERVER_HANDLE_V1 *sapi, ENGINE_ERROR_CODE code);

    int                      id;
    Atomic<vbucket_state_t>  state;
    Mutex                    pendingOpLock;
    std::vector<const void*> pendingOps;
    hrtime_t                 pendingOpsStart;
    EPStats                 &stats;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};

class NeedMoreBuckets : std::exception {};

class VBucketHolder : public RCValue {
public:
    VBucketHolder(size_t sz) :
        buckets(new RCPtr<VBucket>[sz]),
        bucketDeletion(new Atomic<bool>[sz]),
        bucketVersions(new Atomic<uint16_t>[sz]),
        size(sz) {
        highPriorityVbSnapshot.set(false);
        for (size_t i = 0; i < size; ++i) {
            bucketDeletion[i].set(false);
            bucketVersions[i].set(static_cast<uint16_t>(-1));
        }
    }

    VBucketHolder(const RCPtr<VBucketHolder> &vbh, size_t sz) :
        buckets(new RCPtr<VBucket>[sz]),
        bucketDeletion(new Atomic<bool>[sz]),
        bucketVersions(new Atomic<uint16_t>[sz]),
        size(sz) {

        // No shrinkage allowed currently.
        assert(sz >= vbh->getSize());

        highPriorityVbSnapshot.set(vbh->isHighPriorityVbSnapshotScheduled());

        std::copy(buckets, buckets+vbh->getSize(), buckets);
        size_t vbh_size = vbh->getSize();
        for (size_t i = 0; i < vbh_size; ++i) {
            bucketDeletion[i].set(vbh->isBucketDeletion(i));
            bucketVersions[i].set(vbh->getBucketVersion(i));
        }
        for (size_t i = vbh_size; i < size; ++i) {
            bucketDeletion[i].set(false);
            bucketVersions[i].set(static_cast<uint16_t>(-1));
        }
    }

    ~VBucketHolder() {
        delete[] buckets;
        delete[] bucketDeletion;
        delete[] bucketVersions;
    }

    RCPtr<VBucket> getBucket(int id) const {
        assert(id >= 0);
        static RCPtr<VBucket> emptyVBucket;
        if (static_cast<size_t>(id) < size) {
            return buckets[id];
        } else {
            return emptyVBucket;
        }
    }

    void addBucket(const RCPtr<VBucket> &b) {
        if (static_cast<size_t>(b->getId()) < size) {
            buckets[b->getId()].reset(b);
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Mapped new vbucket %d in state %s\n",
                             b->getId(), VBucket::toString(b->getState()));
        } else {
            throw new NeedMoreBuckets;
        }
    }

    /**
     * Remove a vbucket by ID.
     *
     * @return the number of items removed
     */
    HashTableStatVisitor removeBucket(int id) {
        assert(id >= 0);
        HashTableStatVisitor rv;

        if (static_cast<size_t>(id) < size) {
            // Theoretically, this could be off slightly.  In
            // practice, this happens only on dead vbuckets.
            buckets[id]->ht.visit(rv);
            buckets[id].reset();
        }

        return rv;
    }

    std::vector<int> getBuckets(void) const {
        std::vector<int> rv;
        for (size_t i = 0; i < size; ++i) {
            RCPtr<VBucket> b(buckets[i]);
            if (b) {
                rv.push_back(b->getId());
            }
        }
        return rv;
    }

    size_t getSize(void) const {
        return size;
    }

    bool isBucketDeletion(int id) {
        assert(id >= 0 && static_cast<size_t>(id) < size);
        return bucketDeletion[id].get();
    }

    bool setBucketDeletion(int id, bool delBucket) {
        assert(id >= 0 && static_cast<size_t>(id) < size);
        return bucketDeletion[id].cas(!delBucket, delBucket);
    }

    uint16_t getBucketVersion(int id) {
        assert(id >= 0 && static_cast<size_t>(id) < size);
        return bucketVersions[id].get();
    }

    void setBucketVersion(int id, uint16_t vb_version) {
        assert(id >= 0 && static_cast<size_t>(id) < size);
        bucketVersions[id].set(vb_version);
    }

    /**
     * Check if a vbucket snapshot task is currently scheduled with the high priority.
     * @return "true" if a snapshot task with the high priority is currently scheduled.
     */
    bool isHighPriorityVbSnapshotScheduled(void) {
        return highPriorityVbSnapshot.get();
    }

    /**
     * Set the flag to coordinate the scheduled high priority vbucket snapshot and new
     * snapshot requests with the high priority. The flag is "true" if a snapshot
     * task with the high priority is currently scheduled, otherwise "false".
     * If (1) the flag is currently "false" and (2) a new snapshot request invokes
     * this method by passing "true" parameter, this will set the flag to "true" and
     * return "true" to indicate that the new request can be scheduled now. Otherwise,
     * return "false" to prevent duplciate snapshot tasks from being scheduled.
     * When the snapshot task is running and about to writing to disk, it will invoke
     * this method to reset the flag by passing "false" parameter.
     * @param highPrioritySnapshot bool flag for coordination between the scheduled
     *        snapshot task and new snapshot requests.
     * @return "true" if a flag's value was changed. Otherwise "false".
     */
    bool setHighPriorityVbSnapshotFlag(bool highPrioritySnapshot) {
        return highPriorityVbSnapshot.cas(!highPrioritySnapshot, highPrioritySnapshot);
    }

private:
    RCPtr<VBucket> *buckets;
    Atomic<bool> *bucketDeletion;
    Atomic<uint16_t> *bucketVersions;
    Atomic<bool> highPriorityVbSnapshot;
    size_t size;
};

/**
 * A map of known vbuckets.
 */
class VBucketMap {
public:
    VBucketMap() : buckets(new VBucketHolder(BASE_VBUCKET_SIZE)) { }

    void addBucket(RCPtr<VBucket> &b) {
        assert(b);
        RCPtr<VBucketHolder> o(buckets);
        try {
            o->addBucket(b);
        } catch (NeedMoreBuckets &e) {
            grow(b->getId())->addBucket(b);
        }
    }

    HashTableStatVisitor removeBucket(int id) {
        RCPtr<VBucketHolder> o(buckets);
        return o->removeBucket(id);
    }

    void addBuckets(const std::vector<VBucket*> &newBuckets) {
        std::vector<VBucket*>::const_iterator it;
        for (it = newBuckets.begin(); it != newBuckets.end(); ++it) {
            RCPtr<VBucket> v(*it);
            addBucket(v);
        }
    }

    RCPtr<VBucket> getBucket(int id) const {
        RCPtr<VBucketHolder> o(buckets);
        return o->getBucket(id);
    }

    size_t getSize() const {
        RCPtr<VBucketHolder> o(buckets);
        return o->getSize();
    }

    std::vector<int> getBuckets(void) {
        RCPtr<VBucketHolder> o(buckets);
        return o->getBuckets();
    }

    bool isBucketDeletion(int id) {
        RCPtr<VBucketHolder> o(buckets);
        return o->isBucketDeletion(id);
    }

    bool setBucketDeletion(int id, bool delBucket) {
        RCPtr<VBucketHolder> o(buckets);
        return o->setBucketDeletion(id, delBucket);
    }

    uint16_t getBucketVersion(uint16_t id) {
        RCPtr<VBucketHolder> o(buckets);
        return o->getBucketVersion(id);
    }

    void setBucketVersion(uint16_t id, uint16_t vb_version) {
        RCPtr<VBucketHolder> o(buckets);
        o->setBucketVersion(id, vb_version);
    }

    bool isHighPriorityVbSnapshotScheduled(void) {
        RCPtr<VBucketHolder> o(buckets);
        return o->isHighPriorityVbSnapshotScheduled();
    }

    bool setHighPriorityVbSnapshotFlag(bool highPrioritySnapshot) {
        RCPtr<VBucketHolder> o(buckets);
        return o->setHighPriorityVbSnapshotFlag(highPrioritySnapshot);
    }

private:
    RCPtr<VBucketHolder> grow(size_t id) {
        LockHolder lh(mutex);
        if (buckets->getSize() <= id) {
            // still not big enough
            size_t n(0);
            for (n = BASE_VBUCKET_SIZE; n <= id; n *= 2) {} // find next power of 2
            RCPtr<VBucketHolder> nbh(new VBucketHolder(buckets, n));
            buckets = nbh;
        }
        return buckets;
    }

    mutable RCPtr<VBucketHolder> buckets;
    Mutex mutex; // Not acquired often, but you could have a lot of stuff waiting on it

    DISALLOW_COPY_AND_ASSIGN(VBucketMap);
};

#endif /* VBUCKET_HH */
