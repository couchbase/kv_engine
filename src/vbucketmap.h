/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef VBUCKETMAP_HH
#define VBUCKETMAP_HH 1

#include "configuration.h"
#include "vbucket.h"

/**
 * A map of known vbuckets.
 */
class VBucketMap {
public:
    VBucketMap(Configuration &config);
    ~VBucketMap();

    ENGINE_ERROR_CODE addBucket(const RCPtr<VBucket> &b);
    void removeBucket(uint16_t id);
    void addBuckets(const std::vector<VBucket*> &newBuckets);
    RCPtr<VBucket> getBucket(uint16_t id) const;
    size_t getSize() const;
    std::vector<int> getBuckets(void) const;
    std::vector<int> getBucketsSortedByState(void) const;
    bool isBucketDeletion(uint16_t id) const;
    bool setBucketDeletion(uint16_t id, bool delBucket);
    bool isBucketCreation(uint16_t id) const;
    bool setBucketCreation(uint16_t id, bool rv);
    uint64_t getPersistenceCheckpointId(uint16_t id) const;
    void setPersistenceCheckpointId(uint16_t id, uint64_t checkpointId);
    /**
     * Check if a vbucket snapshot task is currently scheduled with
     * the high priority.
     *
     * @return true if a snapshot task with the high priority is
     *              currently scheduled.
     */
    bool isHighPriorityVbSnapshotScheduled(void) const;
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
     * Check if a vbucket snapshot task is currently scheduled with
     * the low priority.
     *
     * @return "true" if a snapshot task with the low priority is
     *                currently scheduled.
     */
    bool isLowPriorityVbSnapshotScheduled(void) const;

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

    RCPtr<VBucket> *buckets;
    Atomic<bool> *bucketDeletion;
    Atomic<bool> *bucketCreation;
    Atomic<uint64_t> *persistenceCheckpointIds;
    Atomic<bool> highPriorityVbSnapshot;
    Atomic<bool> lowPriorityVbSnapshot;
    size_t size;

    DISALLOW_COPY_AND_ASSIGN(VBucketMap);
};

#endif /* VBUCKET_HH */
