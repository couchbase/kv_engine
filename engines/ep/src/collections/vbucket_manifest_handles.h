/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "collections/vbucket_manifest.h"

class StatCollector;

namespace Collections::VB {

struct PersistedStats;

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

    ReadHandle(const Manifest* m, Manifest::mutex_type& lock)
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

    void incrementItemCount(CollectionID collection) const {
        manifest->incrementItemCount(collection);
    }

    void decrementItemCount(CollectionID collection) const {
        manifest->decrementItemCount(collection);
    }

    bool addCollectionStats(Vbid vbid, const StatCollector& collector) const {
        return manifest->addCollectionStats(vbid, collector);
    }

    bool addScopeStats(Vbid vbid, const StatCollector& collector) const {
        return manifest->addScopeStats(vbid, collector);
    }

    /**
     * Update the 'summary' object with all the 'summary' stats for all
     * collections
     */
    void updateSummary(Summary& summary) const {
        manifest->updateSummary(summary);
    }

    /**
     * Update the 'summary' object with all the 'summary' stats for the given
     * collections
     */
    void accumulateStats(const std::vector<CollectionEntry> collections,
                         Summary& summary) const {
        manifest->accumulateStats(collections, summary);
    }
    /**
     * @return true if a collection drop is in-progress, at least 1
     * collection is in the state isDeleting
     */
    bool isDropInProgress() const {
        return manifest->isDropInProgress();
    }

    // @returns all of the statistics that need to be updated during flushing
    StatsForFlush getStatsForFlush(CollectionID collection,
                                   uint64_t seqno) const {
        return manifest->getStatsForFlush(collection, seqno);
    }

    /**
     * Dump this VB::Manifest to std::cerr
     */
    void dump() const;

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

    /**
     * @return the number of system events that exist (as items)
     */
    size_t getSystemEventItemCount() const {
        return manifest->getSystemEventItemCount();
    }

protected:
    friend std::ostream& operator<<(std::ostream& os,
                                    const ReadHandle& readHandle);
    Manifest::mutex_type::ReadHolder readLock{nullptr};
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
     * @param m Manifest object to use for lookups
     * @param lock which to pass to parent ReadHandle
     * @param key the key to use for lookups, if the key is a system key
     *        the collection-id is extracted from the key
     * @param tag to differentiate from more common construction below
     */
    CachingReadHandle(const Manifest* m,
                      Manifest::mutex_type& lock,
                      DocKey key,
                      Manifest::AllowSystemKeys tag)
        : ReadHandle(m, lock), itr(m->getManifestEntry(key, tag)), key(key) {
    }

    CachingReadHandle(const Manifest* m, Manifest::mutex_type& lock, DocKey key)
        : ReadHandle(m, lock), itr(m->getManifestEntry(key)), key(key) {
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
    void incrementItemCount() const {
        // We may be flushing keys written to a dropped collection so can
        // have an invalid iterator or the id is not mapped (system)
        if (!valid()) {
            return;
        }
        return manifest->incrementItemCount(itr);
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
    void decrementItemCount() const {
        // We may be flushing keys written to a dropped collection so can
        // have an invalid iterator or the id is not mapped (system)
        if (!valid()) {
            return;
        }
        return manifest->decrementItemCount(itr);
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
    bool setPersistedHighSeqno(uint64_t value) const {
        // We may be flushing keys written to a dropped collection so can
        // have an invalid iterator
        if (!valid()) {
            return false;
        }
        return manifest->setPersistedHighSeqno(itr, value);
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
    void processExpiryTime(Item& itm, std::chrono::seconds bucketTtl) const {
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
    time_t processExpiryTime(time_t t, std::chrono::seconds bucketTtl) const {
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
    void dump();

protected:
    friend std::ostream& operator<<(std::ostream& os,
                                    const CachingReadHandle& readHandle);

    /**
     * An iterator for the key's collection, or end() if the key has no
     * valid collection.
     */
    Manifest::container::const_iterator itr;

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
    StatsReadHandle(const Manifest* m,
                    Manifest::mutex_type& lock,
                    CollectionID cid)
        : ReadHandle(m, lock), itr(m->getManifestIterator(cid)) {
    }

    PersistedStats getPersistedStats() const;
    uint64_t getHighSeqno() const;

    bool valid() const {
        return itr != manifest->end();
    }

    bool isLogicallyDeleted(uint64_t seqno) const {
        if (!valid()) {
            return true;
        }
        return manifest->isLogicallyDeleted(itr, seqno);
    }

    void updateItemCount(ssize_t delta) const {
        manifest->updateItemCount(itr, delta);
    }

    void setPersistedHighSeqno(uint64_t highSeqno) const {
        manifest->setPersistedHighSeqno(itr, highSeqno);
    }

    void updateDiskSize(ssize_t delta) const {
        manifest->updateDiskSize(itr, delta);
    }

    void dump();

protected:
    friend std::ostream& operator<<(std::ostream& os,
                                    const CachingReadHandle& readHandle);

    /**
     * An iterator for the key's collection, or end() if the key has no
     * valid collection.
     */
    Manifest::container::const_iterator itr;
};

/**
 * RAII write locking for access and updates to the Manifest.
 */
class WriteHandle {
public:
    WriteHandle(Manifest& m, Manifest::mutex_type& lock)
        : writeLock(lock), manifest(m) {
    }

    WriteHandle(WriteHandle&& rhs)
        : writeLock(std::move(rhs.writeLock)), manifest(rhs.manifest) {
    }

    WriteHandle(Manifest& m,
                Manifest::mutex_type::UpgradeHolder&& upgradeHolder)
        : writeLock(std::move(upgradeHolder)), manifest(m) {
    }

    /**
     * Create a collection for a replica VB, this is for receiving
     * collection updates via DCP when the collection already has a start
     * seqno assigned (assigned by the active).
     *
     * @param vb The vbucket to create the collection in
     * @param manifestUid the uid of the manifest which made the change
     * @param identifiers ScopeID and CollectionID pair for the new collection
     * @param collectionName name of the new collection
     * @param maxTtl An optional maxTtl for the collection
     * @param startSeqno The start-seqno assigned to the collection.
     */
    void replicaCreate(::VBucket& vb,
                       ManifestUid manifestUid,
                       ScopeCollectionPair identifiers,
                       std::string_view collectionName,
                       cb::ExpiryLimit maxTtl,
                       int64_t startSeqno) {
        manifest.createCollection(*this,
                                  vb,
                                  manifestUid,
                                  identifiers,
                                  collectionName,
                                  maxTtl,
                                  OptionalSeqno{startSeqno},
                                  false);
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
                *this, vb, manifestUid, cid, OptionalSeqno{endSeqno}, false);
    }

    /**
     * Create a scope in the replica VB
     *
     * @param vb The vbucket to create the scope in
     * @param manifestUid the uid of the manifest which made the change
     * @param sid ScopeID of the new scope
     * @param scopeName name of the new scope
     * @param startSeqno The start-seqno assigned to the scope
     */
    void replicaCreateScope(::VBucket& vb,
                            ManifestUid manifestUid,
                            ScopeID sid,
                            std::string_view scopeName,
                            int64_t startSeqno) {
        manifest.createScope(*this,
                             vb,
                             manifestUid,
                             sid,
                             scopeName,
                             OptionalSeqno{startSeqno},
                             false);
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
                *this, vb, manifestUid, sid, OptionalSeqno{endSeqno}, false);
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
    Manifest::container::iterator begin() {
        return manifest.begin();
    }

    /// @return iterator to the end of the underlying collection map
    Manifest::container::iterator end() {
        return manifest.end();
    }

    void saveDroppedCollection(CollectionID cid,
                               const ManifestEntry& droppedEntry,
                               uint64_t droppedSeqno) {
        manifest.saveDroppedCollection(cid, droppedEntry, droppedSeqno);
    }

    /**
     * Dump this VB::Manifest to std::cerr
     */
    void dump();

private:
    Manifest::mutex_type::WriteHolder writeLock;
    Manifest& manifest;
};

} // namespace Collections::VB
