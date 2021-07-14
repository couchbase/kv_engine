/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "configuration.h"
#include "kvshard.h"
#include "utility.h"

#include <folly/SharedMutex.h>
#include <platform/non_negative_counter.h>
#include <vector>

class KVBucket;
class VBucket;

/**
 * A map of known vbuckets.
 */
class VBucketMap {

// TODO: Remove this once there's a cleaner API to iterator KVShards.
friend class EPBucket;
friend class KVBucket;
friend class Warmup;

    class VBucketConfigChangeListener : public ValueChangedListener {
    public:
        explicit VBucketConfigChangeListener(VBucketMap& vbucketMap)
            : map(vbucketMap) {
        }

        void sizeValueChanged(const std::string &key, size_t value) override;

    private:
        VBucketMap& map;
    };

public:

    VBucketMap(Configuration& config, KVBucket& store);

    /**
     * Add the VBucket to the map - extending the lifetime of the object until
     * it is removed from the map via dropAndDeleteVBucket.
     * @param vb shared pointer to the VBucket we are storing.
     */
    cb::engine_errc addBucket(VBucketPtr vb);

    void enablePersistence(EPBucket& ep);

    /**
     * Drop the vbucket from the map and setup deferred deletion of the VBucket.
     * Once the VBucketPtr has no more references the vbucket is deleted, but
     * deletion occurs via a task that is scheduled by the VBucketPtr deleter,
     * ensuring no front-end thread deletes the memory/disk associated with the
     * VBucket.
     *
     * @param id The VB to drop
     * @param cookie Optional connection cookie, this cookie will be notified
     *        when the deletion task is completed.
     */
    void dropVBucketAndSetupDeferredDeletion(Vbid id,
                                             const CookieIface* cookie);
    VBucketPtr getBucket(Vbid id) const;

    // Returns the size of the map, i.e. the total number of VBuckets it can
    // contain.
    size_t getSize() const {
        return size;
    }
    std::vector<Vbid> getBuckets() const;
    std::vector<Vbid> getBucketsSortedByState() const;

    /**
     * Returns a vector containing the vbuckets from the map that are in the
     * given state.
     * @param state  the state used to filter which vbuckets to return
     * @return  vector of vbuckets that are in the given state.
     */
    std::vector<Vbid> getBucketsInState(vbucket_state_t state) const;

    /**
     * Returns a vector of vbuckets with the amount of memory that each
     * vbucket's checkpoint manager uses.  The vbuckets are sorted with
     * those having the highest checkpoint manager memory usage at the
     * beginning.
     * @return vector of pairs comprised of vbucket and amount of memory
     * used by their respective checkpoint manager.
     */
    std::vector<std::pair<Vbid, size_t>> getVBucketsSortedByChkMgrMem() const;

    KVShard* getShardByVbId(Vbid id) const;
    KVShard* getShard(KVShard::id_type shardId) const;
    size_t getNumShards() const;
    void setHLCDriftAheadThreshold(std::chrono::microseconds threshold);
    void setHLCDriftBehindThreshold(std::chrono::microseconds threshold);

    /**
     * Decrement the vb count for the given state.
     * @param state  the state for which the vb count is to be decremented.
     */
    void decVBStateCount(vbucket_state_t state) {
        --vbStateCount[state - vbucket_state_active];
    }

    /**
     * Increment the vb count for the given state.
     * @param state the state for which the vb count is to be incremented.
     */
    void incVBStateCount(vbucket_state_t state) {
        ++vbStateCount[state - vbucket_state_active];
    }

    /**
     * Get the state count for the given state.
     * @param state  the state for which the vb count is to be retrieved.
     * @rturn  the current vb count in the given state.
     */
    uint16_t getVBStateCount(vbucket_state_t state) const {
        return vbStateCount[state - vbucket_state_active];
    }

    /**
     * Set the state of the vBucket and set VBucketMap invariants
     * @param vb reference to the vBucket to change
     * @param newState desired state
     * @param meta optional meta information to apply alongside the state.
     * @return the old state of the vBucket
     */
    vbucket_state_t setState(VBucket& vb,
                             vbucket_state_t newState,
                             const nlohmann::json* meta);

    /**
     * Set the state of the vBucket and set VBucketMap invariants
     * @param vb reference to the vBucket to change
     * @param newState desired state
     * @param meta optional meta information to apply alongside the state.
     * @param the state lock (write mode) for the vbucket
     * @return the old state of the vBucket
     */
    vbucket_state_t setState_UNLOCKED(
            VBucket& vb,
            vbucket_state_t newState,
            const nlohmann::json* meta,
            folly::SharedMutex::WriteHolder& vbStateLock);

    /**
     * Call the function p and supply each shard as a parameter.
     */
    template <class Predicate>
    void forEachShard(Predicate p) {
        for (auto& shard : shards) {
            p(*shard.get());
        }
    }

private:

    std::vector<std::unique_ptr<KVShard>> shards;

    const size_t size;

    /**
     * Count of how many vbuckets in vbMap are in each of the four valid
     * states of active, replica, pending or dead.
     */
    std::array<cb::NonNegativeCounter<uint16_t>, 4> vbStateCount{{0, 0, 0, 0}};

    DISALLOW_COPY_AND_ASSIGN(VBucketMap);
};
