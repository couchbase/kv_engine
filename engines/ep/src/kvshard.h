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

#pragma once

#include "config.h"

#include "kvstore.h"
#include "utility.h"
#include "vbucket.h"

#include <atomic>

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
class BgFetcher;
class Flusher;
class KVBucket;

class KVShard {
public:
    // Identifier for a KVShard
    typedef uint16_t id_type;
    KVShard(KVShard::id_type id, KVBucket& store);
    ~KVShard();

    KVStore* getRWUnderlying() {
        return rwStore.get();
    }

    KVStore* getROUnderlying() {
        if (roStore) {
            return roStore.get();
        }
        return rwStore.get();
    }

    Flusher *getFlusher();
    BgFetcher *getBgFetcher();

    VBucketPtr getBucket(VBucket::id_type id) const;
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
    void dropVBucketAndSetupDeferredDeletion(VBucket::id_type id,
                                             const void* cookie);

    KVShard::id_type getId() const {
        return kvConfig.getShardId();
    }

    std::vector<VBucket::id_type> getVBucketsSortedByState();
    std::vector<VBucket::id_type> getVBuckets();

private:
    KVStoreConfig kvConfig;

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

    std::vector<VBMapElement> vbuckets;

    std::unique_ptr<KVStore> rwStore;
    std::unique_ptr<KVStore> roStore;

    std::unique_ptr<Flusher> flusher;
    std::unique_ptr<BgFetcher> bgFetcher;

public:
    std::atomic<size_t> highPriorityCount;

    DISALLOW_COPY_AND_ASSIGN(KVShard);
};

/**
 * Callback for notifying flusher about pending mutations.
 */
class NotifyFlusherCB: public Callback<uint16_t> {
public:
    NotifyFlusherCB(KVShard *sh)
        : shard(sh) {}

    void callback(uint16_t &vb);

private:
    KVShard *shard;
};
