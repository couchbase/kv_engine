/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "kvstore/kvstore_config.h"
#include "utility.h"
#include "vbucket_fwd.h"
#include <memcached/vbucket.h>
#include <atomic>
#include <mutex>

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
 *   | rwUnderlying: KVStore (write)   |
 *   -----------------------------------
 *
 * vBuckets are mapped to Shards by (vbid modulo numShards) - see
 * VBucketMap::getShardByVbId(). For example, with 4 shards, the following
 * vBuckets map to:
 *
 *     VB        Shard
 *     ====================
 *     0         0
 *     1            1
 *     2               2
 *     3                  3
 *     4         0
 *     5            1
 *     ...
 *     1022            2
 *     1023               3
 */
class Configuration;
class CookieIface;
class KVStoreIface;

class KVShard {
public:
    // Identifier for a KVShard
    using id_type = uint16_t;
    KVShard(Configuration& config, id_type numShards, id_type id);
    ~KVShard();
    KVShard(const KVShard&) = delete;
    KVShard(KVShard&&) = delete;
    const KVShard& operator=(const KVShard&) = delete;
    void operator=(const KVShard&&) = delete;

    KVStoreIface* getRWUnderlying() {
        return rwStore.get();
    }

    const KVStoreIface* getROUnderlying() const {
        return rwStore.get();
    }

    void setRWUnderlying(std::unique_ptr<KVStoreIface> newStore);

    /**
     * move the rw store out of the shard, the shard will be left with no
     * KVStores after calling this method
     */
    std::unique_ptr<KVStoreIface> takeRW();

    template <class UnaryFunction>
    void forEachKVStore(UnaryFunction f) {
        f(rwStore.get());
    }

    VBucketPtr getBucket(Vbid id) const;
    void setBucket(VBucketPtr vb);

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

    KVShard::id_type getId() const {
        return kvConfig->getShardId();
    }

    std::vector<Vbid> getVBuckets();

private:
    // Holds the store configuration for the current shard.
    // We need to use a unique_ptr in place of the concrete class because
    // KVStoreConfig is a polymorphic type, and this unique_ptr can hold a
    // pointer to either the base class or a child class (e.g.,
    // RocksDBKVStoreConfig) instance.
    std::unique_ptr<KVStoreConfig> kvConfig;

    /**
     * VBMapElement comprises the VBucket smart pointer and a mutex.
     * Access to the smart pointer must be performed through the ::Access object
     * which will perform RAII locking of the mutex.
     */
    class VBMapElement {
    public:
        /**
         * Access for const/non-const VBMapElement (using enable_if to hide
         * const methods for non-const users and vice-versa)
         *
         * This class did have an -> operator for directly reading the VBucket*
         * but MSVC could not cope with it, so now you must use:
         *    access.get()-><Vbucket-method>
         */
        template <class T>
        class Access {
        public:
            Access(std::mutex& m, T e) : lock(m), element(e) {
            }

            /**
             * @return a const VBBucketPtr& (which may have no real pointer)
             */
            template <typename U = T>
            typename std::enable_if<
                    std::is_const<
                            typename std::remove_reference<U>::type>::value,
                    const VBucketPtr&>::type
            get() const {
                return element.vbPtr;
            }

            /**
             * @return the VBBucketPtr (which may have no real pointer)
             */
            template <typename U = T>
            typename std::enable_if<
                    !std::is_const<
                            typename std::remove_reference<U>::type>::value,
                    VBucketPtr>::type
            get() const {
                return element.vbPtr;
            }

            /**
             * @param set a new VBBucketPtr for the VB
             */
            template <typename U = T>
            typename std::enable_if<!std::is_const<
                    typename std::remove_reference<U>::type>::value>::type
            set(VBucketPtr vb) {
                element.vbPtr = vb;
            }

            /**
             * @param reset VBBucketPtr for the VB
             */
            template <typename U = T>
            typename std::enable_if<!std::is_const<
                    typename std::remove_reference<U>::type>::value>::type
            reset() {
                element.vbPtr.reset();
            }

        private:
            std::unique_lock<std::mutex> lock;
            T& element;
        };

        Access<VBMapElement&> lock() {
            return {mutex, *this};
        }

        Access<const VBMapElement&> lock() const {
            return {mutex, *this};
        }

    private:
        mutable std::mutex mutex;
        VBucketPtr vbPtr;
    };

    /**
     * Helper methods to lookup and lock the VBMap element for the given id.
     * @returns a RAII-style Access object which has the element locked while
     * valid.
     */
    VBMapElement::Access<VBMapElement&> getElement(Vbid id);
    VBMapElement::Access<const KVShard::VBMapElement&> getElement(
            Vbid id) const;

    /**
     * !! create rwStore before vbuckets, and destruct in reverse !!
     * A vbucket may try to access a KVStore during destruct, e.g. a RangeScan
     * closing its ScanContext, these operations will touch the rwStore
     */
    std::unique_ptr<KVStoreIface> rwStore;

    /**
     * VBuckets owned by this Shard.
     * Note that elements are indexed by the vbid % numShards, e.g for shard 1
     * the first element in the vector is vb:1, second is vb:5, 3rd is vb:9 ...
     */
    std::vector<VBMapElement> vbuckets;
};
