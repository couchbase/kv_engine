/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/collection_persisted_stats.h"
#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest_entry.h"
#include "systemevent.h"

#include <folly/SharedMutex.h>
#include <folly/container/F14Map.h>
#include <platform/non_negative_counter.h>

#include <functional>
#include <iostream>
#include <mutex>
#include <optional>

class VBucket;

namespace flatbuffers {
class FlatBufferBuilder;
}

namespace Collections {
namespace KVStore {
struct Manifest;
}

namespace VB {

/**
 * Collections::VB::Manifest is a container for all of the collections a VBucket
 * knows about.
 *
 * Each collection is represented by a Collections::VB::ManifestEntry and all of
 * the collections are stored in an unordered_map. The map is implemented to
 * allow look-up by collection-name without having to allocate a std::string,
 * callers only need a std::string_view for look-ups.
 *
 * The Manifest allows for an external manager to drive the lifetime of each
 * collection.
 *
 * This class is intended to be thread-safe when accessed through the read
 * or write handles (providing RAII locking).
 *
 * Access to the class is performed by the ReadHandle and WriteHandle classes
 * which perform RAII locking on the manifest's internal lock. A user of the
 * manifest is required to hold the correct handle for the required scope to
 * to ensure any actions they take based upon a collection's existence are
 * consistent. The important consistency issue is the checkpoint. For example
 * when setting a document code must first check the document's collection
 * exists, the document must then only enter the checkpoint after the creation
 * event for the collection and also before a delete event foe the collection.
 * Thus the set path must obtain read access to collections and keep read access
 * for the entire scope of the set path to ensure no other thread can interleave
 * collection create/delete and cause an inconsistency in the checkpoint
 * ordering.
 */
class Manifest {
public:
    using container = folly::F14FastMap<CollectionID, ManifestEntry>;
#ifdef THREAD_SANITIZER
    // SharedMutexReadPriority has no TSAN annotations, so use WritePrioity
    using mutex_type = folly::SharedMutexWritePriority;
#else
    using mutex_type = folly::SharedMutexReadPriority;
#endif
    enum class UpdateStatus { Success, Behind, EqualUidWithDifferences };

    /**
     * RAII read locking for access to the Manifest.
     */
    class ReadHandle {
    public:
        /**
         * To keep the RAII style locking but also allow us to avoid extra
         * memory allocations we can provide a default constructor giving us
         * an unlocked ReadHandle and assign locked/default constructed
         * ReadHandles where required to lock/unlock a given manifest. Note,
         * a default constructed ReadHandle doesn't point to any manifest so
         * no other function in the ReadHandle should be called.
         */
        ReadHandle() = default;

        ReadHandle(const Manifest* m, mutex_type& lock)
            : readLock(lock), manifest(m) {
        }

        ReadHandle(ReadHandle&& rhs)
            : readLock(std::move(rhs.readLock)), manifest(rhs.manifest) {
        }

        ReadHandle& operator=(ReadHandle&& other) {
            readLock = std::move(other.readLock);
            manifest = std::move(other.manifest);

            return *this;
        }

        /**
         * Does the key contain a valid collection?
         *
         * - If the key applies to the default collection, the default
         *   collection must exist.
         *
         * - If the key applies to a collection, the collection must exist and
         *   must not be in the process of deletion.
         */
        bool doesKeyContainValidCollection(DocKey key) const {
            return manifest->doesKeyContainValidCollection(key);
        }

        /**
         * Does the manifest know of the scope?
         *
         * @param scopeID the scopeID
         * @return true if it the scope is known
         */
        bool isScopeValid(ScopeID scopeID) const {
            return manifest->isScopeValid(scopeID);
        }

        /**
         * Given a key and it's seqno, the manifest can determine if that key
         * is logically deleted - that is part of a collection which is in the
         * process of being erased.
         *
         * @return true if the key belongs to a deleted collection.
         */
        bool isLogicallyDeleted(DocKey key, int64_t seqno) const {
            return manifest->isLogicallyDeleted(key, seqno);
        }

        /**
         * @returns true/false if _default exists
         */
        bool doesDefaultCollectionExist() const {
            return manifest->doesDefaultCollectionExist();
        }

        /**
         * @returns optional vector of CollectionIDs associated with the
         *          scope. Returns uninitialized if the scope does not exist
         */
        std::optional<std::vector<CollectionID>> getCollectionsForScope(
                ScopeID identifier) const {
            return manifest->getCollectionsForScope(identifier);
        }

        /**
         * @return true if the collection exists in the internal container
         */
        bool exists(CollectionID identifier) const {
            return manifest->exists(identifier);
        }

        /**
         * @return scope-id of the collection if it exists
         */
        std::optional<ScopeID> getScopeID(CollectionID identifier) const {
            return manifest->getScopeID(identifier);
        }

        /// @return the manifest UID that last updated this vb::manifest
        ManifestUid getManifestUid() const {
            return manifest->getManifestUid();
        }

        uint64_t getItemCount(CollectionID collection) const {
            return manifest->getItemCount(collection);
        }

        uint64_t getHighSeqno(CollectionID collection) const {
            return manifest->getHighSeqno(collection);
        }

        uint64_t getPersistedHighSeqno(CollectionID collection) const {
            return manifest->getPersistedHighSeqno(collection);
        }

        /**
         * Set the persisted high seqno of the given colletion to the given
         * value
         *
         * @param collection The collection to update
         * @param value The value to update the persisted high seqno to
         * @param noThrow Should we suppress exceptions if we can't find the
         *                collection?
         */
        void setPersistedHighSeqno(CollectionID collection,
                uint64_t value,
                bool noThrow = false) const {
            manifest->setPersistedHighSeqno(collection, value, noThrow);
        }

        void setHighSeqno(CollectionID collection, uint64_t value) const {
            manifest->setHighSeqno(collection, value);
        }

        void incrementDiskCount(CollectionID collection) const {
            manifest->incrementDiskCount(collection);
        }

        void decrementDiskCount(CollectionID collection) const {
            manifest->decrementDiskCount(collection);
        }

        bool addCollectionStats(Vbid vbid,
                                const void* cookie,
                                const AddStatFn& add_stat) const {
            return manifest->addCollectionStats(vbid, cookie, add_stat);
        }

        bool addScopeStats(Vbid vbid,
                           const void* cookie,
                           const AddStatFn& add_stat) const {
            return manifest->addScopeStats(vbid, cookie, add_stat);
        }

        void updateSummary(Summary& summary) const {
            manifest->updateSummary(summary);
        }

        /**
         * @return true if a collection drop is in-progress, at least 1
         * collection is in the state isDeleting
         */
        bool isDropInProgress() const {
            return manifest->isDropInProgress();
        }

        /**
         * Dump this VB::Manifest to std::cerr
         */
        void dump() const {
            std::cerr << manifest << std::endl;
        }

        /**
         * We may wish to keep hold of a ReadHandle without actually keeping
         * hold of the lock to avoid unnecessary locking, in particular in
         * the PagingVisitor. To make the code more explicit (rather than
         * simply assigning a default constructed, unlocked ReadHandle, allow a
         * user to manually unlock the ReadHandle, after which it should not
         * be used.
         */
        void unlock() {
            readLock.unlock();
            manifest = nullptr;
        }

    protected:
        friend std::ostream& operator<<(std::ostream& os,
                                        const Manifest::ReadHandle& readHandle);
        mutex_type::ReadHolder readLock{nullptr};
        const Manifest* manifest{nullptr};
    };

    /**
     * CachingReadHandle provides a limited set of functions to allow various
     * functional paths in KV-engine to perform multiple collection 'legality'
     * checks with one map lookup.
     *
     * The pattern is that the caller creates a CachingReadHandle and during
     * creation of the object, the collection entry is located (or not) and
     * the data cached
     *
     * The caller next can check if the read handle represents a valid
     * collection, allowing code to return 'unknown_collection'.
     *
     * Finally a caller can pass a seqno into the isLogicallyDeleted function
     * to test if that seqno is a logically deleted key. The seqno should have
     * been found by searching for the key used during in construction.
     *
     * Privately inherited from ReadHandle so we have a readlock/manifest
     * without exposing the ReadHandle public methods that don't quite fit in
     * this class.
     */
    class CachingReadHandle : private ReadHandle {
    public:
        /**
         * @param allowSystem true if system keys are allowed (the KV
         *        internal keys like create collection). A frontend operation
         *        should not be allowed, whereas a disk backfill is allowed
         */
        CachingReadHandle(const Manifest* m,
                          mutex_type& lock,
                          DocKey key,
                          bool allowSystem)
            : ReadHandle(m, lock),
              itr(m->getManifestEntry(key, allowSystem)),
              key(key) {
        }

        /**
         * @return true if the key used in construction is associated with a
         *         valid and open collection.
         */
        bool valid() const {
            return itr != manifest->end();
        }

        /**
         * @return the key used in construction
         */
        DocKey getKey() const {
            return key;
        }

        /**
         * @param a seqno to check, the seqno should belong to the document
         *        identified by the key returned by ::getKey()
         * @return true if the key@seqno is logically deleted.
         */
        bool isLogicallyDeleted(int64_t seqno) const {
            return manifest->isLogicallyDeleted(itr, seqno);
        }

        /// @return the manifest UID that last updated this vb::manifest
        ManifestUid getManifestUid() const {
            return manifest->getManifestUid();
        }

        /**
         * This increment is possible via this CachingReadHandle, which has
         * shared access to the Manifest, because the read-lock only ensures
         * that the underlying collection map doesn't change. Data inside the
         * collection entry maybe mutable, such as the item count, hence this
         * method is marked const because the manifest is const.
         *
         * increment the key's collection item count by 1
         */
        void incrementDiskCount() const {
            // We may be flushing keys written to a dropped collection so can
            // have an invalid iterator or the id is not mapped (system)
            if (!valid()) {
                return;
            }
            return manifest->incrementDiskCount(itr);
        }

        /**
         * This decrement is possible via this CachingReadHandle, which has
         * shared access to the Manifest, because the read-lock only ensures
         * that the underlying collection map doesn't change. Data inside the
         * collection entry maybe mutable, such as the item count, hence this
         * method is marked const because the manifest is const.
         *
         * decrement the key's collection item count by 1
         */
        void decrementDiskCount() const {
            // We may be flushing keys written to a dropped collection so can
            // have an invalid iterator or the id is not mapped (system)
            if (!valid()) {
                return;
            }
            return manifest->decrementDiskCount(itr);
        }

        /**
         * This update is possible via this CachingReadHandle, which has
         * shared access to the Manifest, because the read-lock only ensures
         * that the underlying collection map doesn't change. Data inside the
         * collection entry maybe mutable, such as the on disk size, hence this
         * method is marked const because the manifest is const.
         *
         * Adjust the tracked total on disk size for the collection by the
         * given delta.
         */
        void updateDiskSize(ssize_t delta) const {
            // We may be flushing keys written to a dropped collection so can
            // have an invalid iterator or the id is not mapped (system)
            if (!valid()) {
                return;
            }
            return manifest->updateDiskSize(itr, delta);
        }

        /**
         * This set is possible via this CachingReadHandle, which has shared
         * access to the Manifest, because the read-lock only ensures that
         * the underlying collection map doesn't change. Data inside the
         * collection entry maybe mutable, such as the item count, hence this
         * method is marked const because the manifest is const.
         *
         * set the high seqno of the collection if the new value is
         * higher
         */
        void setHighSeqno(uint64_t value) const {
            // We may be flushing keys written to a dropped collection so can
            // have an invalid iterator or the id is not mapped (system)
            if (!valid()) {
                return;
            }
            manifest->setHighSeqno(itr, value);
        }

        /**
         * This set is possible via this CachingReadHandle, which has shared
         * access to the Manifest, because the read-lock only ensures that
         * the underlying collection map doesn't change. Data inside the
         * collection entry maybe mutable, such as the item count, hence this
         * method is marked const because the manifest is const.
         *
         * set the persisted high seqno of the collection if the new value is
         * higher
         */
        void setPersistedHighSeqno(uint64_t value) const {
            // 1) We may be flushing keys written to a dropped collection so can
            //    have an invalid iterator
            if (!valid()) {
                return;
            }
            manifest->setPersistedHighSeqno(itr, value);
        }

        /**
         * This set is possible via this CachingReadHandle, which has shared
         * access to the Manifest, because the read-lock only ensures that
         * the underlying collection map doesn't change. Data inside the
         * collection entry maybe mutable, such as the item count, hence this
         * method is marked const because the manifest is const.
         *
         * reset the persisted high seqno of the collection to the new value,
         * regardless of if it is greater than the current value
         */
        void resetPersistedHighSeqno(uint64_t value) const {
            manifest->resetPersistedHighSeqno(itr, value);
        }

        /**
         * Check the Item's exptime against its collection config.
         * If the collection defines a maxTTL and the Item has no expiry or
         * an exptime which exceeds the maxTTL, set the expiry of the Item
         * based on the collection maxTTL.
         *
         * @param itm The reference to the Item to check and change if needed
         * @param bucketTtl the value of the bucket's maxTTL, 0 being none
         */
        void processExpiryTime(Item& itm,
                               std::chrono::seconds bucketTtl) const {
            manifest->processExpiryTime(itr, itm, bucketTtl);
        }

        /**
         * t represents an absolute expiry time and this method returns t or a
         * limited expiry time, based on the values of the bucketTtl and the
         * collection's maxTTL.
         *
         * @param t an expiry time to process
         * @param bucketTtl the value of the bucket's maxTTL, 0 being none
         * @returns t or now + appropriate limit
         */
        time_t processExpiryTime(time_t t,
                                 std::chrono::seconds bucketTtl) const {
            return manifest->processExpiryTime(itr, t, bucketTtl);
        }

        /**
         * @return the scopeID of the collection associated with the handle
         */
        ScopeID getScopeID() const {
            return itr->second.getScopeID();
        }

        void incrementOpsStore() const {
            if (!valid()) {
                return;
            }
            return itr->second.incrementOpsStore();
        }

        void incrementOpsDelete() const {
            if (!valid()) {
                return;
            }
            return itr->second.incrementOpsDelete();
        }

        void incrementOpsGet() const {
            if (!valid()) {
                return;
            }
            return itr->second.incrementOpsGet();
        }

        /**
         * Dump this VB::Manifest to std::cerr
         */
        void dump() {
            std::cerr << *manifest << std::endl;
        }

    protected:
        friend std::ostream& operator<<(
                std::ostream& os,
                const Manifest::CachingReadHandle& readHandle);

        /**
         * An iterator for the key's collection, or end() if the key has no
         * valid collection.
         */
        container::const_iterator itr;

        /**
         * The key used in construction of this handle.
         */
        DocKey key;
    };

    /**
     * RAII read locking for access to the manifest stats
     */
    class StatsReadHandle : private ReadHandle {
    public:
        StatsReadHandle(const Manifest* m, mutex_type& lock, CollectionID cid)
            : ReadHandle(m, lock), itr(m->getManifestIterator(cid)) {
        }

        bool valid() const {
            return itr != manifest->end();
        }

        PersistedStats getPersistedStats() const {
            return {itr->second.getDiskCount(),
                    itr->second.getPersistedHighSeqno(),
                    itr->second.getDiskSize()};
        }

        void dump() {
            std::cerr << *manifest << std::endl;
        }

    protected:
        friend std::ostream& operator<<(
                std::ostream& os,
                const Manifest::CachingReadHandle& readHandle);

        /**
         * An iterator for the key's collection, or end() if the key has no
         * valid collection.
         */
        container::const_iterator itr;
    };

    /**
     * RAII write locking for access and updates to the Manifest.
     */
    class WriteHandle {
    public:
        WriteHandle(Manifest& m, mutex_type& lock)
            : writeLock(lock), manifest(m) {
        }

        WriteHandle(WriteHandle&& rhs)
            : writeLock(std::move(rhs.writeLock)), manifest(rhs.manifest) {
        }

        WriteHandle(Manifest& m, mutex_type::UpgradeHolder&& upgradeHolder)
            : writeLock(std::move(upgradeHolder)), manifest(m) {
        }

        /**
         * Add a collection for a replica VB, this is for receiving
         * collection updates via DCP and the collection already has a start
         * seqno assigned.
         *
         * @param vb The vbucket to add the collection to.
         * @param manifestUid the uid of the manifest which made the change
         * @param identifiers ScopeID and CollectionID pair
         * @param collectionName name of the added collection
         * @param maxTtl An optional maxTtl for the collection
         * @param startSeqno The start-seqno assigned to the collection.
         */
        void replicaAdd(::VBucket& vb,
                        ManifestUid manifestUid,
                        ScopeCollectionPair identifiers,
                        std::string_view collectionName,
                        cb::ExpiryLimit maxTtl,
                        int64_t startSeqno) {
            manifest.addCollection(*this,
                                   vb,
                                   manifestUid,
                                   identifiers,
                                   collectionName,
                                   maxTtl,
                                   OptionalSeqno{startSeqno});
        }

        /**
         * Drop collection for a replica VB, this is for receiving
         * collection updates via DCP and the collection already has an end
         * seqno assigned.
         *
         * @param vb The vbucket to drop collection from
         * @param manifestUid the uid of the manifest which made the change
         * @param cid CollectionID to drop
         * @param endSeqno The end-seqno assigned to the end collection.
         */
        void replicaDrop(::VBucket& vb,
                         ManifestUid manifestUid,
                         CollectionID cid,
                         int64_t endSeqno) {
            manifest.dropCollection(
                    *this, vb, manifestUid, cid, OptionalSeqno{endSeqno});
        }

        /**
         * Add a scope for a replica VB
         *
         * @param vb The vbucket to add the scope to
         * @param manifestUid the uid of the manifest which made the change
         * @param sid ScopeID of the new scope
         * @param scopeName name of the added scope
         * @param startSeqno The start-seqno assigned to the scope
         */
        void replicaAddScope(::VBucket& vb,
                             ManifestUid manifestUid,
                             ScopeID sid,
                             std::string_view scopeName,
                             int64_t startSeqno) {
            manifest.addScope(*this,
                              vb,
                              manifestUid,
                              sid,
                              scopeName,
                              OptionalSeqno{startSeqno});
        }

        /**
         * Drop a scope for a replica VB
         *
         * @param vb The vbucket to drop the scope from
         * @param manifestUid the uid of the manifest which made the change
         * @param sid ScopeID to drop
         * @param endSeqno The end-seqno assigned to the scope drop
         */
        void replicaDropScope(::VBucket& vb,
                              ManifestUid manifestUid,
                              ScopeID sid,
                              int64_t endSeqno) {
            manifest.dropScope(
                    *this, vb, manifestUid, sid, OptionalSeqno{endSeqno});
        }

        /**
         * When we create system events we do so under a WriteHandle. To
         * properly increment the high seqno of the collection for a given
         * system event we need to be able to do so using this handle.
         *
         * Function is const as constness refers to the state of the manifest,
         * not the state of the manifest entries within it.
         *
         * @param collection the collection ID of the manifest entry to update
         * @param value the new high seqno
         */
        void setHighSeqno(CollectionID collection, uint64_t value) const {
            manifest.setHighSeqno(collection, value);
        }

        /// @return iterator to the beginning of the underlying collection map
        container::iterator begin() {
            return manifest.begin();
        }

        /// @return iterator to the end of the underlying collection map
        container::iterator end() {
            return manifest.end();
        }

        /**
         * Dump this VB::Manifest to std::cerr
         */
        void dump() {
            std::cerr << manifest << std::endl;
        }

    private:
        mutex_type::WriteHolder writeLock;
        Manifest& manifest;
    };

    friend ReadHandle;
    friend CachingReadHandle;
    friend WriteHandle;

    /**
     * Construct a VBucket::Manifest in the default state
     *
     * The manifest will initialise with the following.
     * - Default Collection enabled.
     * - uid of 0
     */
    Manifest();

    /**
     * Construct a VBucket::Manifest from KVStore::Manifest
     *
     * Empty data allows for construction where no persisted data was found i.e.
     * an upgrade occurred and this is the first construction of a VB::Manifest
     * for a VBucket which has persisted data, but no VB::Manifest data. When
     * empty data is used, the manifest will initialise with default settings.
     * - Default Collection enabled.
     * - uid of 0
     *
     * A non-empty object must contain valid flatbuffers VB::Manifest which is
     * used to define the new object.
     *
     * @param data object storing flatbuffer manifest data (or empty)
     */
    Manifest(const KVStore::Manifest& data);

    ReadHandle lock() const {
        return {this, rwlock};
    }

    CachingReadHandle lock(DocKey key, bool allowSystem = false) const {
        return {this, rwlock, key, allowSystem};
    }

    /**
     * Read lock and return a StatsHandle - lookup only requires a collection-ID
     * @return StatsReadHandle object with read lock on the manifest
     */
    StatsReadHandle lock(CollectionID cid) const {
        return {this, rwlock, cid};
    }

    // Explicitly delete rvalue StoredDocKey usage. A CachingReadHandle wants to
    // create a view of the original key, so that key must have a life-span
    // which is longer than the handle. Only test-code trips over this as they
    // may create keys at the point of calling a method.
    CachingReadHandle lock(StoredDocKey&&,
                           bool allowSystem = false) const = delete;

    WriteHandle wlock() {
        return {*this, rwlock};
    }

    /**
     * Update from a Collections::Manifest
     *
     * Update compares the current collection set against the manifest and
     * triggers collection creation and collection deletion.
     *
     * Creation and deletion of a collection are pushed into the VBucket and
     * the seqno of updates is recorded in the manifest.
     *
     * @param vb The VBucket to update (queue data into).
     * @param manifest The incoming manifest to compare this object with.
     * @return UpdateStatus describing outcome (success or failed reason)
     */
    UpdateStatus update(::VBucket& vb, const Collections::Manifest& manifest);

    /**
     * Get the system event collection create data from a SystemEvent
     * Item's value.
     *
     * @param flatbufferData buffer storing flatbuffer Collections.VB.Collection
     * @returns CreateEventData which carries all of the data which needs to be
     *          marshalled into a DCP system event message.
     */
    static CreateEventData getCreateEventData(std::string_view flatbufferData);

    /**
     * Get the system event collection drop data from a SystemEvent
     * Item's value.
     *
     * @param flatbufferData buffer storing flatbuffer
     *        Collections.VB.DroppedCollection
     * @returns DropEventData which carries all of the data which needs to be
     *          marshalled into a DCP system event message.
     */
    static DropEventData getDropEventData(std::string_view flatbufferData);

    /**
     * Get the system event scope create data from a SystemEvent Item's value.
     *
     * @param flatbufferData buffer storing flatbuffer Collections.VB.Scope
     * @returns CreateScopeEventData which carries all of the data which needs #
     *          to be marshalled into a DCP system event message.
     */
    static CreateScopeEventData getCreateScopeEventData(
            std::string_view flatbufferData);

    /**
     * Get the system event scope drop data from a SystemEvent Item's value.
     *
     * @param flatbufferData buffer storing flatbuffer
     *        Collections.VB.DropScope
     * @returns DropScopeEventData which carries all of the data which needs to
     *          be marshalled into a DCP system event message.
     */
    static DropScopeEventData getDropScopeEventData(
            std::string_view flatbufferData);

    /**
     * @return an Item that represent a collection create or delete
     */
    static std::unique_ptr<Item> makeCollectionSystemEvent(
            ManifestUid uid,
            CollectionID cid,
            std::string_view collectionName,
            const ManifestEntry& entry,
            bool deleted,
            OptionalSeqno seq);

    bool operator==(const Manifest& rhs) const;

    bool operator!=(const Manifest& rhs) const;

    // local struct for managing collection addition
    struct CollectionAddition {
        ScopeCollectionPair identifiers;
        std::string name;
        cb::ExpiryLimit maxTtl;
    };

    // local struct for managing scope addition
    struct ScopeAddition {
        ScopeID sid;
        std::string name;
    };

protected:
    /**
     * @return an update status by testing if this can be updated to manifest
     */
    UpdateStatus canUpdate(const Collections::Manifest& manifest) const;

    /**
     * The changes that we need to make to the vBucket manifest derived from
     * the bucket manifest.
     */
    struct ManifestChanges {
        explicit ManifestChanges(ManifestUid uid) : uid(uid) {
        }
        ManifestUid uid{0};
        std::vector<ScopeAddition> scopesToAdd;
        std::vector<ScopeID> scopesToRemove;
        std::vector<CollectionAddition> collectionsToAdd;
        std::vector<CollectionID> collectionsToRemove;

        bool empty() const {
            return scopesToAdd.empty() && scopesToRemove.empty() &&
                   collectionsToAdd.empty() && collectionsToRemove.empty();
        }
    };

    /**
     * Complete the update of this from a manifest - will take the upgradeLock
     * and switch to exclusive mode to process the required changes.
     *
     * @param upgradeLock rvalue upgrade holder, which will be moved to a write
     *        handle.
     * @param vb Vbucket to apply update to
     * @param changes Set of changes to make
     */
    void completeUpdate(mutex_type::UpgradeHolder&& upgradeLock,
                        ::VBucket& vb,
                        ManifestChanges& changes);

    /**
     * Sub-functions used by update
     * Removes the last ID of the changes vector and then calls 'update' on
     * every remaining ID (using the current manifest ManifestUid).
     * So if the vector has 1 element, it returns that element and does nothing.
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param update a function to call (either addCollection or
     *        beginCollectionDelete)
     * @param changes a vector of CollectionIDs to add/delete (based on update)
     * @return the last element of the changes vector
     */
    std::optional<CollectionAddition> applyCreates(
            const WriteHandle& wHandle,
            ::VBucket& vb,
            std::vector<CollectionAddition>& changes);

    std::optional<CollectionID> applyDeletions(
            const WriteHandle& wHandle,
            ::VBucket& vb,
            std::vector<CollectionID>& changes);

    std::optional<ScopeAddition> applyScopeCreates(
            const WriteHandle& wHandle,
            ::VBucket& vb,
            std::vector<ScopeAddition>& changes);

    std::optional<ScopeID> applyScopeDrops(const WriteHandle& wHandle,
                                           ::VBucket& vb,
                                           std::vector<ScopeID>& changes);

    /**
     * Add a collection to the manifest.
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to add the collection to.
     * @param manifestUid the uid of the manifest which made the change
     * @param identifiers ScopeID and CollectionID pair
     * @param collectionName Name of the added collection
     * @param maxTtl An optional maxTTL for the collection
     * @param optionalSeqno Either a seqno to assign to the new collection or
     *        none (none means the checkpoint will assign a seqno).
     */
    void addCollection(const WriteHandle& wHandle,
                       ::VBucket& vb,
                       ManifestUid manifestUid,
                       ScopeCollectionPair identifiers,
                       std::string_view collectionName,
                       cb::ExpiryLimit maxTtl,
                       OptionalSeqno optionalSeqno);

    /**
     * Drop the collection from the manifest
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to drop the collection from
     * @param manifestUid the uid of the manifest which made the change
     * @param cid CollectionID to drop
     * @param optionalSeqno Either a seqno to assign to the delete of the
     *        collection or none (none means the checkpoint assigns the seqno).
     */
    void dropCollection(const WriteHandle& wHandle,
                        ::VBucket& vb,
                        ManifestUid manifestUid,
                        CollectionID cid,
                        OptionalSeqno optionalSeqno);

    /**
     * Add a scope to the manifest.
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to add the collection to.
     * @param manifestUid the uid of the manifest which made the change
     * @param sid ScopeID
     * @param scopeName Name of the added scope
     * @param optionalSeqno Either a seqno to assign to the new collection or
     *        none (none means the checkpoint will assign a seqno).
     */
    void addScope(const WriteHandle& wHandle,
                  ::VBucket& vb,
                  ManifestUid manifestUid,
                  ScopeID sid,
                  std::string_view scopeName,
                  OptionalSeqno optionalSeqno);

    /**
     * Drop a scope
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to drop the scope from
     * @param manifestUid the uid of the manifest which made the change
     * @param sid ScopeID to drop
     * @param optionalSeqno Either a seqno to assign to the drop of the
     *        scope or none (none means the checkpoint will assign the seqno)
     */
    void dropScope(const WriteHandle& wHandle,
                   ::VBucket& vb,
                   ManifestUid manifestUid,
                   ScopeID sid,
                   OptionalSeqno optionalSeqno);

    /**
     * Does the key contain a valid collection?
     *
     * - If the key applies to the default collection, the default collection
     *   must exist.
     *
     * - If the key applies to a collection, the collection must exist and must
     *   not be in the process of deletion.
     */
    bool doesKeyContainValidCollection(const DocKey& key) const;

    /**
     * Does the manifest contain the scope?
     *
     * @param scopeID the scopeID
     * @return true if the scope is known
     */
    bool isScopeValid(ScopeID scopeID) const;

    /**
     * Given a key and it's seqno, the manifest can determine if that key
     * is logically deleted - that is part of a collection which is in the
     * process of being erased.
     *
     * @return true if the key belongs to a deleted collection.
     */
    bool isLogicallyDeleted(const DocKey& key, int64_t seqno) const;

    /**
     * Perform the job of isLogicallyDeleted, but with an iterator for the
     * manifest container instead of a key. This means no map lookup is
     * performed.
     *
     *  @return true if the seqno/entry represents a logically deleted
     *          collection.
     */
    bool isLogicallyDeleted(const container::const_iterator entry,
                            int64_t seqno) const;

    void incrementDiskCount(const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.incrementDiskCount();
    }

    void decrementDiskCount(const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.decrementDiskCount();
    }

    void updateDiskSize(const container::const_iterator entry,
                        ssize_t delta) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.updateDiskSize(delta);
    }

    void setHighSeqno(const container::const_iterator entry,
                      uint64_t value) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.setHighSeqno(value);
    }

    void setPersistedHighSeqno(const container::const_iterator entry,
                               uint64_t value) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.setPersistedHighSeqno(value);
    }

    void resetPersistedHighSeqno(const container::const_iterator entry,
                                 uint64_t value) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.resetPersistedHighSeqno(value);
    }

    /// see comment on CachingReadHandle
    void processExpiryTime(const container::const_iterator entry,
                           Item& item,
                           std::chrono::seconds bucketTtl) const;

    /// see comment on CachingReadHandle
    time_t processExpiryTime(const container::const_iterator entry,
                             time_t t,
                             std::chrono::seconds bucketTtl) const;

    /**
     * @returns true/false if _default exists
     */
    bool doesDefaultCollectionExist() const {
        return exists(CollectionID::Default);
    }

    /**
     * Get the collections associated with a given scope
     * @param identifier scopeID
     * @return optional vector of CollectionIDs. Returns uninitialized if the
     *         scope does not exist
     */
    std::optional<std::vector<CollectionID>> getCollectionsForScope(
            ScopeID identifier) const;

    /**
     * @return true if the collection exists in the internal container
     */
    bool exists(CollectionID identifier) const {
        return map.count(identifier) > 0;
    }

    /**
     * @return scope-id of the collection if it exists in the internal container
     */
    std::optional<ScopeID> getScopeID(CollectionID identifier) const;

    /**
     * @return the number of items stored for collection
     */
    uint64_t getItemCount(CollectionID collection) const;

    /**
     * @return the highest seqno for this collection
     */
    uint64_t getHighSeqno(CollectionID collection) const;

    /**
     * Set the high seqno of the given collection to the given value. Allowed
     * to be const as the only constness we care about here is the state of
     * the map (not the ManifestEntries within it).
     */
    void setHighSeqno(CollectionID collection, uint64_t value) const;

    /**
     * @return the highest seqno that has been persisted for this collection
     */
    uint64_t getPersistedHighSeqno(CollectionID collection) const;

    /**
     * Set the high seqno of the given collection to the given value
     *
     * @param collection The collection to update
     * @param value The value to update the collection persisted high seqno to
     * @param noThrow Should we suppress exceptions if we cannot find the
     *                collection?
     */
    void setPersistedHighSeqno(CollectionID collection, uint64_t value,
            bool noThrow = false) const;

    /**
     * Increment the item count for the given collection. Const and can be
     * called via a ReadHandle because the read lock only ensures the map
     * does not change.
     */
    void incrementDiskCount(CollectionID collection) const;

    /**
     * Decrement the item count for the given collection. Const and can be
     * called via a ReadHandle because the read lock only ensures the map
     * does not change.
     */
    void decrementDiskCount(CollectionID collection) const;

    container::const_iterator end() const {
        return map.end();
    }

    /**
     * @return iterator for the collections map
     */
    container::iterator begin() {
        return map.begin();
    }

    /**
     * @return end iterator for the collections map
     */
    container::iterator end() {
        return map.end();
    }

    /**
     * Get a manifest entry for the collection associated with the key. Can
     * return map.end() for unknown collections.
     */
    container::const_iterator getManifestEntry(const DocKey& key,
                                               bool allowSystem) const;

    /**
     * Get a map iterator for the collection. Can return map.end() for unknown
     * collections
     */
    container::const_iterator getManifestIterator(CollectionID id) const;

    /// @return the manifest UID that last updated this vb::manifest
    ManifestUid getManifestUid() const {
        return manifestUid;
    }

    /**
     * Detailed stats for this VB::Manifest
     * @return true if addCollectionStats was successful, false if failed.
     */
    bool addCollectionStats(Vbid vbid,
                            const void* cookie,
                            const AddStatFn& add_stat) const;

    /**
     * Detailed stats for the scopes in this VB::Manifest
     * @return true if addScopeStats was successful, false if failed.
     */
    bool addScopeStats(Vbid vbid,
                       const void* cookie,
                       const AddStatFn& add_stat) const;

    void updateSummary(Summary& summary) const;

    /**
     * Add a collection entry to the manifest specifing the revision that it was
     * seen in and the sequence number span covering it.
     * @param identifiers ScopeID and CollectionID pair
     * @param maxTtl The maxTTL that if defined will be applied to new items of
     *        the collection (overriding bucket maxTTL)
     * @param startSeqno The seqno where the collection begins. Defaults to 0.
     * @return a non const reference to the new ManifestEntry so the caller can
     *         set the correct seqno.
     */
    ManifestEntry& addNewCollectionEntry(ScopeCollectionPair identifiers,
                                         cb::ExpiryLimit maxTtl,
                                         int64_t startSeqno = 0);

    /**
     * Get the ManifestEntry for the given collection. Throws an
     * std::logic_error if the collection was not found.
     *
     * @param collectionID CollectionID of the collection to lookup
     * @return a const reference to the ManifestEntry
     */
    const ManifestEntry& getManifestEntry(CollectionID collectionID) const;



    /**
     * Process a Collections::Manifest to determine if collections need adding
     * or removing.
     *
     * @param manifest The Manifest to compare with.
     * @returns An struct containing the scopes and collections that need
     *          adding or removing from this vBucket manifest. If
     *          uninitialized, the manifest cannot be applied and update must
     *          be aborted. This is the case when we are attempting to add a
     *          deleting collection.
     */
    ManifestChanges processManifest(
            const Collections::Manifest& manifest) const;

    /**
     * Create an Item that carries a collection system event and queue it to the
     * vb checkpoint.
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket onto which the Item is queued.
     * @param cid The collection ID added/removed
     * @param collectionName Name of the collection (only used by create)
     * @param entry The ManifestEntry added or removed
     * @param deleted If the Item created should be marked as deleted.
     * @param seqno An optional seqno which if set will be assigned to the
     *        system event.
     *
     * @returns The sequence number of the queued Item.
     */
    int64_t queueCollectionSystemEvent(const WriteHandle& wHandle,
                                       ::VBucket& vb,
                                       CollectionID cid,
                                       std::string_view collectionName,
                                       const ManifestEntry& entry,
                                       bool deleted,
                                       OptionalSeqno seq) const;

    /**
     * @return true if a collection drop is in-progress, at least 1 collection
     *         is in the state isDeleting
     */
    bool isDropInProgress() const;

    /**
     * Return a string for use in throwException, returns:
     *   "VB::Manifest::<thrower>:<error>, this:<ostream *this>"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and useful data
     * @returns string as per description above
     */
    std::string getExceptionString(const std::string& thrower,
                                   const std::string& error) const;

    /**
     * throw exception with the following error string:
     *   "VB::Manifest::<thrower>:<error>, this:<ostream *this>"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and useful data
     * @throws exception
     */
    template <class exception>
    [[noreturn]] void throwException(const std::string& thrower,
                                     const std::string& error) const {
        throw exception(getExceptionString(thrower, error));
    }

    /**
     * The current set of collections
     */
    container map;

    /**
     * The current scopes.
     */
    std::unordered_set<ScopeID> scopes;

    /// The manifest UID which updated this vb::manifest
    ManifestUid manifestUid{0};

    /// Does this vbucket need collection purging triggering
    bool dropInProgress{false};

    /**
     * shared lock to allow concurrent readers and safe updates
     */
    mutable mutex_type rwlock;

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

    static constexpr char const* UidKey = "uid";
};

/// Note that the VB::Manifest << operator does not obtain the rwlock
/// it is used internally in the object for exception string generation so must
/// not double lock.
std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

/// This is the locked version for printing the manifest
std::ostream& operator<<(std::ostream& os,
                         const Manifest::ReadHandle& readHandle);

std::string to_string(Manifest::UpdateStatus);
} // end namespace VB
} // end namespace Collections