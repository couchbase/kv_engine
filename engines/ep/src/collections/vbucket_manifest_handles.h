/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/vbucket_manifest.h"
#include <folly/synchronization/Lock.h>

#include <utility>

class CookieIface;
class EventuallyPersistentEngine;
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

    /// @return iterator to the beginning of the underlying collection map
    Manifest::container::const_iterator begin() const {
        return manifest->begin();
    }

    /// @return iterator to the end of the underlying collection map
    Manifest::container::const_iterator end() const {
        return manifest->end();
    }

    /// @return iterator to the beginning of the underlying collection map
    Manifest::scopesContainer::const_iterator beginScopes() const {
        return manifest->beginScopes();
    }

    /// @return iterator to the end of the underlying collection map
    Manifest::scopesContainer::const_iterator endScopes() const {
        return manifest->endScopes();
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
    bool doesKeyContainValidCollection(DocKeyView key) const {
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
    bool isLogicallyDeleted(DocKeyView key, int64_t seqno) const {
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

    Visibility getScopeVisibility(ScopeID sid) const {
        if (sid.isDefaultScope()) {
            // Shortcut the lookup, this is not a system scope
            return Visibility::User;
        }
        return manifest->getScopeVisibility(sid);
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

    uint64_t getDefaultCollectionMaxVisibleSeqno() const {
        return manifest->getDefaultCollectionMaxVisibleSeqno();
    }

    uint64_t getDefaultCollectionMaxLegacyDCPSeqno() const {
        return manifest->getDefaultCollectionMaxLegacyDCPSeqno();
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

    /**
     * Set the high seqno (!persisted) of the given collection.
     * @param collection The collection to update
     * @param value The value to update the persisted high seqno to
     * @param type An identifying type for the "event @ value" to distinguish a
     *        prepare from a mutation/commit from a system event.
     */
    void setHighSeqno(CollectionID collection,
                      uint64_t value,
                      HighSeqnoType type) const {
        manifest->setHighSeqno(collection, value, type);
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
    void accumulateStats(const std::vector<CollectionMetaData> collections,
                         Summary& summary) const {
        manifest->accumulateStats(collections, summary);
    }

    // @returns all of the statistics that need to be updated during flushing
    StatsForFlush getStatsForFlush(CollectionID collection,
                                   uint64_t seqno) const {
        return manifest->getStatsForFlush(collection, seqno);
    }

    /// @return the metering state of the collection, throw for unknown cid
    Metered isMetered(CollectionID cid) const;

    /// @return in one lookup data that the VB::Filter needs
    std::optional<DcpFilterMeta> getMetaForDcpFilter(CollectionID cid) const {
        return manifest->getMetaForDcpFilter(cid);
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

    /**
     * @throw if collection does not exist
     * @return the CanDeduplicate status for the collection
     */
    CanDeduplicate getCanDeduplicate(CollectionID cid) const {
        return manifest->getCanDeduplicate(cid);
    }

    cb::ExpiryLimit getMaxTtl(CollectionID cid) const {
        return manifest->getMaxTtl(cid);
    }

protected:
    friend std::ostream& operator<<(std::ostream& os,
                                    const ReadHandle& readHandle);
    std::shared_lock<Manifest::mutex_type> readLock;
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
                      DocKeyView key,
                      Manifest::AllowSystemKeys tag)
        : ReadHandle(m, lock), itr(m->getManifestEntry(key, tag)), key(key) {
    }

    CachingReadHandle(const Manifest* m,
                      Manifest::mutex_type& lock,
                      DocKeyView key)
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
     * handle 'write-status' of the collection associated with this handle.
     * If the collection doesn't exist - invoke setUnknownCollectionErrorContext
     * @param engine The engine so we can call setUnknownCollectionErrorContext
     * @param cookie Cookie for the command
     * @return the status - success and the write can go ahead
     */
    cb::engine_errc handleWriteStatus(EventuallyPersistentEngine& engine,
                                      CookieIface* cookie);

    /**
     * @return the key used in construction
     */
    DocKeyView getKey() const {
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
     * Set the high seqno (!persisted) of the collection of this handle
     *
     * This set is possible via this CachingReadHandle, which has shared
     * access to the Manifest, because the read-lock only ensures that
     * the underlying collection map doesn't change. Data inside the
     * collection entry maybe mutable, such as the item count, hence this
     * method is marked const because the manifest is const.
     *
     * @param value The value to update the persisted high seqno to
     * @param type An identifying type for the "event @ value" to distinguish a
     *        prepare from a mutation/commit from a system event.
     */
    void setHighSeqno(uint64_t value, HighSeqnoType type) const {
        // We may be flushing keys written to a dropped collection so can
        // have an invalid iterator or the id is not mapped (system)
        if (!valid()) {
            return;
        }
        manifest->setHighSeqno(itr, value, type);
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

    Metered isMetered() const {
        return itr->second.isMetered();
    }

    CanDeduplicate getCanDeduplicate() const {
        if (!valid()) {
            return CanDeduplicate::Yes;
        }
        return itr->second.getCanDeduplicate();
    }

    /**
     * Dump this VB::Manifest to std::cerr
     */
    void dump() const;

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
    DocKeyView key;
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

    void setItemCount(size_t value) const {
        manifest->setItemCount(itr, value);
    }

    void setPersistedHighSeqno(uint64_t highSeqno) const {
        manifest->setPersistedHighSeqno(itr, highSeqno);
    }

    void updateDiskSize(ssize_t delta) const {
        manifest->updateDiskSize(itr, delta);
    }

    void setDiskSize(size_t newValue) const {
        manifest->setDiskSize(itr, newValue);
    }

    size_t getDiskSize() const {
        return manifest->getDiskSize(itr);
    }

    size_t getItemCount() const;

    uint64_t getPersistedHighSeqno() const {
        return manifest->getPersistedHighSeqno(itr);
    }

    // @return the manifest UID that last updated this vb::manifest
    ManifestUid getManifestUid() const {
        return manifest->getManifestUid();
    }

    /// @return the scope of the collection that locked this (must be valid())
    ScopeID getScopeID() const {
        return itr->second.getScopeID();
    }

    Metered isMetered() const {
        return itr->second.isMetered();
    }

    /// @return collection name
    std::string_view getName() const {
        return itr->second.getName();
    }

    uint64_t getStartSeqno() const {
        return itr->second.getStartSeqno();
    }

    void dump() const;

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
    WriteHandle(Manifest& m,
                // NOLINTNEXTLINE(modernize-pass-by-value)
                VBucketStateLockRef vbStateLock,
                Manifest::mutex_type& lock)
        : vbStateLock(vbStateLock), writeLock(lock), manifest(m) {
    }

    WriteHandle(WriteHandle&& rhs)
        : vbStateLock(rhs.vbStateLock),
          writeLock(std::move(rhs.writeLock)),
          manifest(rhs.manifest) {
    }

    WriteHandle(Manifest& m,
                // NOLINTNEXTLINE(modernize-pass-by-value)
                VBucketStateLockRef vbStateLock,
                folly::upgrade_lock<Manifest::mutex_type>&& upgradeHolder)
        : vbStateLock(vbStateLock),
          writeLock(folly::transition_lock<std::unique_lock>(upgradeHolder)),
          manifest(m) {
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
     * @param metered Is the collection metered or "free"?
     * @param canDeduplicate Can the collection do any deduplication?
     * @param flushUid The flush-uid assigned to the collection.
     * @param startSeqno The start-seqno assigned to the collection.
     */
    void replicaCreate(::VBucket& vb,
                       ManifestUid manifestUid,
                       ScopeCollectionPair identifiers,
                       std::string_view collectionName,
                       cb::ExpiryLimit maxTtl,
                       Metered metered,
                       CanDeduplicate canDeduplicate,
                       ManifestUid flushUid,
                       int64_t startSeqno) {
        manifest.beginCollection(vbStateLock,
                                 *this,
                                 vb,
                                 manifestUid,
                                 identifiers,
                                 collectionName,
                                 maxTtl,
                                 metered,
                                 canDeduplicate,
                                 flushUid,
                                 OptionalSeqno{startSeqno});
    }

    /**
     * Modify a collection for a replica VB, this is for receiving
     * collection updates via DCP when the collection already has a start
     * seqno assigned (assigned by the active).
     *
     * @param vb The vbucket to create the collection in
     * @param manifestUid the uid of the manifest which made the change
     * @param cid The collection id
     * @param maxTtl An optional maxTtl for the collection
     * @param metered Is the collection metered or "free"?
     * @param canDeduplicate Can the collection do any deduplication?
     * @param startSeqno The start-seqno assigned to the collection.
     */
    void replicaModifyCollection(::VBucket& vb,
                                 ManifestUid manifestUid,
                                 CollectionID cid,
                                 cb::ExpiryLimit maxTtl,
                                 Metered metered,
                                 CanDeduplicate canDeduplicate,
                                 int64_t startSeqno) {
        manifest.modifyCollection(vbStateLock,
                                  *this,
                                  vb,
                                  manifestUid,
                                  cid,
                                  maxTtl,
                                  metered,
                                  canDeduplicate,
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
     * @param isSystemCollection true if dropping a system collection
     * @param endSeqno The end-seqno assigned to the end collection.
     */
    void replicaDrop(::VBucket& vb,
                     ManifestUid manifestUid,
                     CollectionID cid,
                     bool isSystemCollection,
                     int64_t endSeqno) {
        manifest.dropCollection(vbStateLock,
                                *this,
                                vb,
                                manifestUid,
                                cid,
                                isSystemCollection,
                                OptionalSeqno{endSeqno});
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
        manifest.createScope(vbStateLock,
                             *this,
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
     * @param isSystemScope true if dropping a system scope
     * @param endSeqno The end-seqno assigned to the scope drop
     */
    void replicaDropScope(::VBucket& vb,
                          ManifestUid manifestUid,
                          ScopeID sid,
                          bool isSystemScope,
                          int64_t endSeqno) {
        manifest.dropScope(vbStateLock,
                           *this,
                           vb,
                           manifestUid,
                           sid,
                           isSystemScope,
                           OptionalSeqno{endSeqno});
    }

    /**
     * Set the high-seqno of the given collection
     *
     * Function is const as constness refers to the state of the manifest,
     * not the state of the manifest entries within it.
     *
     * @param collection the collection ID of the manifest entry to update
     * @param value the new high seqno
     * @param type An identifying type for the "event @ value" to distinguish a
     *        prepare from a mutation/commit from a system event.
     */
    void setHighSeqno(CollectionID collection,
                      uint64_t value,
                      HighSeqnoType type) const {
        manifest.setHighSeqno(collection, value, type);
    }

    /// @return iterator to the beginning of the underlying collection map
    Manifest::container::iterator begin() {
        return manifest.begin();
    }

    /// @return iterator to the end of the underlying collection map
    Manifest::container::iterator end() {
        return manifest.end();
    }

    /// @return iterator to the beginning of the underlying collection map
    Manifest::scopesContainer::iterator beginScopes() {
        return manifest.beginScopes();
    }

    /// @return iterator to the end of the underlying collection map
    Manifest::scopesContainer::iterator endScopes() {
        return manifest.endScopes();
    }

    void saveDroppedCollection(CollectionID cid,
                               const ManifestEntry& droppedEntry,
                               uint64_t droppedSeqno) {
        manifest.saveDroppedCollection(cid, droppedEntry, droppedSeqno);
    }

    void setDiskSize(CollectionID cid, size_t size) {
        manifest.setDiskSize(cid, size);
    }

    CanDeduplicate getCanDeduplicate(CollectionID cid) const;

    /**
     * Dump this VB::Manifest to std::cerr
     */
    void dump() const;

private:
    VBucketStateLockRef vbStateLock;
    std::unique_lock<Manifest::mutex_type> writeLock;
    Manifest& manifest;
};

} // namespace Collections::VB
