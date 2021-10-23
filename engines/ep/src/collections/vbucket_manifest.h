/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "collections/system_event_types.h"
#include "collections/vbucket_manifest_entry.h"
#include "collections/vbucket_manifest_scope_entry.h"
#include "ep_types.h"
#include "storeddockey_fwd.h"

#include <folly/SharedMutex.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>

#include <optional>
#include <unordered_map>

class Item;
class VBucket;
class StatCollector;

namespace flatbuffers {
class FlatBufferBuilder;
}

namespace Collections {

class Manager;

namespace KVStore {
struct Manifest;
}

namespace VB {

class CachingReadHandle;
class ReadHandle;
class StatsReadHandle;
class WriteHandle;

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
    using container = folly::F14NodeMap<CollectionID, ManifestEntry>;
    using scopesContainer = std::unordered_map<ScopeID, ScopeEntry>;
#ifdef THREAD_SANITIZER
    // SharedMutexReadPriority has no TSAN annotations, so use WritePrioity
    using mutex_type = folly::SharedMutexWritePriority;
#else
    using mutex_type = folly::SharedMutexReadPriority;
#endif
    struct AllowSystemKeys {};

    friend Collections::VB::ReadHandle;
    friend Collections::VB::CachingReadHandle;
    friend Collections::VB::StatsReadHandle;
    friend Collections::VB::WriteHandle;

    /**
     * Construct a VBucket::Manifest in the default state
     *
     * The manifest will initialise with the following.
     * - Default Collection enabled.
     * - uid of 0
     * @param manager this object needs access to the manager so that it can
     *        access collection metadata. This is a shared reference as a
     *        VBucket can outlive the KVBucket.
     */
    explicit Manifest(std::shared_ptr<Manager> manager);

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
     * @param manager this object needs access to the manager so that it can
     *        access collection metadata. This is a shared reference as a
     *        VBucket can outlive the KVBucket.
     * @param data object storing flatbuffer manifest data (or empty)
     */
    Manifest(std::shared_ptr<Manager> manager, const KVStore::Manifest& data);

    /**
     * Destruct the object, which releases all references to data in the manager
     */
    ~Manifest();

    /**
     * Copy ctor - more of a logical copy as we can't copy the lock guarding
     * the manifest. Used by NexusKVStore to generate a manifest for the
     * secondary KVStore to update stats against when flushing.
     *
     * Interestingly this isn't const because we want to take the write lock
     * (wlock()) to prevent updates to the manifest. It's probably fine to
     * take a read lock instead but because the read handles can update various
     * stats which will be copied it's better to err on the side of caution and
     * take the write lock.
     */
    Manifest(Manifest& other);

    /**
     * @return ReadHandle, no iterator is held on the collection container
     */
    ReadHandle lock() const;

    /*
     * @param key The key to use in look-ups. Will call getCollectionID on the
     *        key and use that value in the map find.
     * @return CachingReadHandle, an iterator is held on the collection
     *         container
     */
    CachingReadHandle lock(DocKey key) const;

    /**
     * @param key The key to use in look-ups. This variant of 'lock' accepts
     *        keys of system-event Items (which embed the collection-ID). Thus
     *        when a system key is used, the event's collection should be
     *        looked-up.
     * @param tag differentiate this special lock call from the more commonly
     *         used lock (above).
     * @return CachingReadHandle, an iterator is held on the collection
     *         container so no further lookups are required. This call accepts
     *         'system-event' keys which will be 'split' to get the collection
     *         of the event.
     */
    CachingReadHandle lock(DocKey key, AllowSystemKeys tag) const;

    /**
     * Read lock and return a StatsHandle - lookup only requires a collection-ID
     * @return StatsReadHandle object with read lock on the manifest
     */
    StatsReadHandle lock(CollectionID cid) const;

    // Explicitly delete rvalue StoredDocKey usage. A CachingReadHandle wants to
    // create a view of the original key, so that key must have a life-span
    // which is longer than the handle. Only test-code trips over this as they
    // may create keys at the point of calling a method.
    CachingReadHandle lock(StoredDocKey&&) const = delete;

    CachingReadHandle lock(StoredDocKey&&, AllowSystemKeys tag) const = delete;

    WriteHandle wlock();

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
     * @return ManifestUpdateStatus describing outcome (success or failed
     * reason)
     */
    ManifestUpdateStatus update(VBucket& vb,
                                const Collections::Manifest& manifest);

    /**
     * Callback from flusher that the drop of collection with the given seqno
     * was successfully persisted. This method does not need the wlock/rlock
     * as a separate lock manages the dropped collection structures
     * @param cid Collection ID of the collection event
     * @param seqno The seqno of the event that was successfully stored
     */
    void collectionDropPersisted(CollectionID cid, uint64_t seqno);

    /**
     * @return true if a collection drop is in-progress, at least 1 collection
     *         is in the state isDeleting
     */
    bool isDropInProgress() const {
        return dropInProgress.load();
    }

    /**
     * Set the value of 'drop in progress'.
     */
    void setDropInProgress(bool value) {
        dropInProgress.store(value);
    }

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

    // local struct for managing collection creation
    struct CollectionCreation {
        ScopeCollectionPair identifiers;
        std::string name;
        cb::ExpiryLimit maxTtl;
    };

    // local struct for managing scope creation
    struct ScopeCreation {
        ScopeID sid;
        std::string name;
        DataLimit dataLimit;
    };

    // struct for managing scope modification
    struct ScopeModified {
        ScopeID sid;
        ScopeEntry& entry;
        DataLimit dataLimit;
    };

    /**
     * The changes that we need to make to the VB:Manifest manifest derived from
     * the bucket Collections::Manifest.
     */
    struct ManifestChanges {
        explicit ManifestChanges(ManifestUid uid) : uid(uid) {
        }
        std::vector<ScopeCreation> scopesToCreate;
        std::vector<ScopeID> scopesToDrop;
        std::vector<ScopeModified> scopesToModify;
        std::vector<CollectionCreation> collectionsToCreate;
        std::vector<CollectionID> collectionsToDrop;
        const ManifestUid uid{0};

        bool empty() const {
            return scopesToCreate.empty() && scopesToModify.empty() &&
                   scopesToDrop.empty() && collectionsToCreate.empty() &&
                   collectionsToDrop.empty();
        }

        bool onlyScopesModified() const {
            return scopesToCreate.empty() && scopesToDrop.empty() &&
                   collectionsToCreate.empty() && collectionsToDrop.empty() &&
                   !scopesToModify.empty();
        }
    };

protected:
    /**
     * @return an update status by testing if this can be updated to manifest
     */
    ManifestUpdateStatus canUpdate(const Collections::Manifest& manifest) const;

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
     * Sub-functions used by update.
     * applyCreates/applyDrops and applyScopeCreates/applyScopeDrops follow
     * a similar pattern as follows.
     *
     * Given a 'changeset' (vector of changes) remove the last entry of the
     * changes vector and then call an 'update' function on every remaining
     * entry (using the current manifest ManifestUid).
     *
     * If the vector has 1 element, it returns that element and does nothing.
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param update a function to call (either addCollection or
     *        beginCollectionDelete)
     * @param changes a vector of CollectionIDs to add/delete (based on update)
     * @return the last element which has been removed from the changes vector
     */
    std::optional<CollectionCreation> applyCreates(
            const WriteHandle& wHandle,
            ::VBucket& vb,
            std::vector<CollectionCreation>& changes);

    std::optional<CollectionID> applyDrops(WriteHandle& wHandle,
                                           ::VBucket& vb,
                                           std::vector<CollectionID>& changes);

    std::optional<ScopeCreation> applyScopeCreates(
            const WriteHandle& wHandle,
            ::VBucket& vb,
            std::vector<ScopeCreation>& changes);

    std::optional<ScopeID> applyScopeDrops(const WriteHandle& wHandle,
                                           ::VBucket& vb,
                                           std::vector<ScopeID>& scopesToDrop);

    /**
     * Create a collection in the vbucket
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to create the collection in.
     * @param newManUid the uid of the manifest which made the change
     * @param identifiers ScopeID and CollectionID pair
     * @param collectionName Name of the created collection
     * @param maxTtl An optional maxTTL for the collection
     * @param optionalSeqno Either a seqno to assign to the new collection or
     *        none (none means the checkpoint will assign a seqno).
     */
    void createCollection(const WriteHandle& wHandle,
                          ::VBucket& vb,
                          ManifestUid newManUid,
                          ScopeCollectionPair identifiers,
                          std::string_view collectionName,
                          cb::ExpiryLimit maxTtl,
                          OptionalSeqno optionalSeqno);

    /**
     * Drop the collection from the vbucket
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to drop the collection from
     * @param newManUid the uid of the manifest which made the change
     * @param cid CollectionID to drop
     * @param optionalSeqno Either a seqno to assign to the delete of the
     *        collection or none (none means the checkpoint assigns the seqno).
     */
    void dropCollection(WriteHandle& wHandle,
                        ::VBucket& vb,
                        ManifestUid newManUid,
                        CollectionID cid,
                        OptionalSeqno optionalSeqno);

    /**
     * Create a scope in the vbucket.
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to create the scope in.
     * @param newManUid the uid of the manifest which made the change
     * @param sid ScopeID
     * @param scopeName Name of the added scope
     * @param dataLimit An optional limit on the data stored in the scope
     * @param optionalSeqno Either a seqno to assign to the new collection or
     *        none (none means the checkpoint will assign a seqno).
     */
    void createScope(const WriteHandle& wHandle,
                     ::VBucket& vb,
                     ManifestUid newManUid,
                     ScopeID sid,
                     std::string_view scopeName,
                     DataLimit dataLimit,
                     OptionalSeqno optionalSeqno);

    /**
     * Modiffy a scope in the vbucket.
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked.
     * @param vb The vbucket to modify the scope in.
     * @param newManUid the uid of the manifest which made the change
     * @param modification sid/dataLimit of the modification
     */
    void modifyScope(const WriteHandle& wHandle,
                     ::VBucket& vb,
                     ManifestUid newManUid,
                     const ScopeModified& modification);

    /**
     * Drop a scope
     *
     * @param wHandle The manifest write handle under which this operation is
     *        currently locked. Required to ensure we lock correctly around
     *        VBucket::notifyNewSeqno
     * @param vb The vbucket to drop the scope from
     * @param newManUid the uid of the manifest which made the change
     * @param sid ScopeID to drop
     * @param optionalSeqno Either a seqno to assign to the drop of the
     *        scope or none (none means the checkpoint will assign the seqno)
     */
    void dropScope(const WriteHandle& wHandle,
                   ::VBucket& vb,
                   ManifestUid newManUid,
                   ScopeID sid,
                   OptionalSeqno optionalSeqno);

    /**
     * Update the manifestUid
     * @param uid new value
     * @param reset Use reset so uid can go backwards
     */
    void updateUid(ManifestUid uid, bool reset);

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

    void incrementItemCount(const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.incrementItemCount();
    }

    void decrementItemCount(const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.decrementItemCount();
    }

    void updateItemCount(const container::const_iterator entry,
                         ssize_t delta) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.updateItemCount(delta);
    }

    void updateDiskSize(const container::const_iterator entry,
                        ssize_t delta) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.updateDiskSize(delta);
    }

    /**
     * Update a collection's scope data size
     *
     * @param entry iterator from the collection map
     * @param delta the value (+/-) that is added to the data size
     */
    void updateScopeDataSize(const container::const_iterator entry,
                             ssize_t delta) const;

    void setDiskSize(const container::const_iterator entry,
                     size_t newValue) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.setDiskSize(newValue);
    }

    void setDiskSize(CollectionID cid, size_t newValue) const;

    /**
     * Update the data size of the scope with the +/- delta
     * @param sid Scope to update
     * @param delta the value (+/-) that is added to the data size
     */
    void updateDataSize(ScopeID sid, ssize_t delta) const;

    size_t getDiskSize(const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        return entry->second.getDiskSize();
    }

    /// @return the data size for the scope
    size_t getDataSize(ScopeID sid) const;

    void setHighSeqno(const container::const_iterator entry,
                      uint64_t value) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        entry->second.setHighSeqno(value);
    }

    uint64_t getPersistedHighSeqno(
            const container::const_iterator entry) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        return entry->second.getPersistedHighSeqno();
    }

    bool setPersistedHighSeqno(const container::const_iterator entry,
                               uint64_t value) const {
        if (entry == map.end()) {
            throwException<std::invalid_argument>(__FUNCTION__,
                                                  "iterator is invalid");
        }

        return entry->second.setPersistedHighSeqno(value);
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
    void incrementItemCount(CollectionID collection) const;

    /**
     * Decrement the item count for the given collection. Const and can be
     * called via a ReadHandle because the read lock only ensures the map
     * does not change.
     */
    void decrementItemCount(CollectionID collection) const;

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
     * @return iterator for the scopes map
     */
    scopesContainer::iterator beginScopes() {
        return scopes.begin();
    }

    /**
     * @return end iterator for the scopes map
     */
    scopesContainer::iterator endScopes() {
        return scopes.end();
    }

    /**
     * Get a manifest entry for the collection associated with the key. Can
     * return map.end() for unknown collections.
     */
    container::const_iterator getManifestEntry(const DocKey& key,
                                               AllowSystemKeys tag) const;

    /**
     * Get a manifest entry for the collection associated with the key. Can
     * return map.end() for unknown collections.
     */
    container::const_iterator getManifestEntry(const DocKey& key) const;

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
    bool addCollectionStats(Vbid vbid, const StatCollector& collector) const;

    /**
     * Detailed stats for the scopes in this VB::Manifest
     * @return true if addScopeStats was successful, false if failed.
     */
    bool addScopeStats(Vbid vbid, const StatCollector& collector) const;

    void updateSummary(Summary& summary) const;

    void accumulateStats(const std::vector<CollectionEntry>& collections,
                         Summary& summary) const;

    /**
     * Add a collection entry to the manifest. This will fail if the collection
     * already exists.
     *
     * @param identifiers ScopeID and CollectionID pair
     * @param collectionName The name of the collection
     * @param maxTtl The maxTTL that if defined will be applied to new items of
     *        the collection (overriding bucket maxTTL)
     * @param startSeqno The seqno where the collection begins.
     * @return a non const reference to the new ManifestEntry so the caller can
     *         make any changes that are needed post construction.
     */
    ManifestEntry& addNewCollectionEntry(ScopeCollectionPair identifiers,
                                         std::string_view collectionName,
                                         cb::ExpiryLimit maxTtl,
                                         int64_t startSeqno);

    /**
     * Add a scope to the manifest. This will fail if the scope already exists.
     *
     * @param sid is of the new scope
     * @param name The name of the scope
     * @param limit The scope's data limit
     * @return The new scope entry
     */
    ScopeEntry& addNewScopeEntry(ScopeID sid,
                                 std::string_view name,
                                 DataLimit limit);

    /**
     * Add a scope to the manifest. This will fail if the scope already exists.
     *
     * @param sid is of the new scope
     * @param sharedMeta The name of the scope (the shared view)
     * @return The new scope entry
     */
    ScopeEntry& addNewScopeEntry(
            ScopeID sid, SingleThreadedRCPtr<ScopeSharedMetaData> sharedMeta);

    /**
     * Get the ManifestEntry for the given collection. Throws a
     * std::logic_error if the collection was not found.
     *
     * @param collectionID CollectionID of the collection to lookup
     * @return a const reference to the ManifestEntry
     */
    const ManifestEntry& getManifestEntry(CollectionID collectionID) const;

    /**
     * Get the ScopeEntry of the scope, stores all stats/metadata of the scope
     *
     * Throws a std::logic_error if the scope was not found.
     *
     * @param sid id of the scope to lookup
     * @return a const reference to the scope's ScopeEntry
     */
    const ScopeEntry& getScopeEntry(ScopeID sid) const;

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
    ManifestChanges processManifest(const Collections::Manifest& manifest);

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
     * @param assignedSeqnoCallback a function that queueDirty will call with
     *        the seqno of the system event.
     *
     * @returns The sequence number of the queued Item.
     */
    uint64_t queueCollectionSystemEvent(
            const WriteHandle& wHandle,
            ::VBucket& vb,
            CollectionID cid,
            std::string_view collectionName,
            const ManifestEntry& entry,
            bool deleted,
            OptionalSeqno seq,
            std::function<void(int64_t)> assignedSeqnoCallback) const;

    /**
     * @return the number of system events that exist (as items)
     */
    size_t getSystemEventItemCount() const;

    /**
     * For the collection save the droppedEntry and droppedSeqno into the
     * droppedCollections container. This allows the flusher to find out about
     * the collection whilst the drop event is still in-flight.
     * @param cid ID of dropped collection
     * @param droppedEntry reference to entry which is going away
     * @param droppedSeqno seqno of drop event
     */
    void saveDroppedCollection(CollectionID cid,
                               const ManifestEntry& droppedEntry,
                               uint64_t droppedSeqno);

    /**
     * Return statistics about the collection for the seqno
     * @param cid ID to lookup
     * @param seqno The seqno to use in lookup
     */
    StatsForFlush getStatsForFlush(CollectionID cid, uint64_t seqno) const;

    /**
     * Return the status of the data limit if we were to try and write nBytes
     * to the collection's scope.
     * @param itr iterator to a collection in the map
     * @param nBytes size of the data we want to add to the collection
     * @return status code success if can write otherwise the error
     */
    cb::engine_errc getScopeDataLimitStatus(const container::const_iterator itr,
                                            size_t nBytes) const;

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
     * All the data we store about a scope and a map of scope to the data
     */
    scopesContainer scopes;

    // Information we need to retain for a collection that is dropped but the
    // drop event has not been persisted by the flusher.
    struct DroppedCollectionInfo {
        DroppedCollectionInfo(uint64_t start,
                              uint64_t end,
                              uint64_t itemCount,
                              uint64_t diskSize,
                              uint64_t highSeqno)
            : start(start),
              end(end),
              itemCount(itemCount),
              diskSize(diskSize),
              highSeqno(highSeqno) {
        }
        bool addStats(Vbid vbid,
                      CollectionID cid,
                      const StatCollector& collector) const;

        uint64_t start{0};
        uint64_t end{0};
        uint64_t itemCount{0};
        uint64_t diskSize{0};
        uint64_t highSeqno{0};
    };

    // collections move from the Manifest::map to this "container" when they
    // are dropped and are removed from this container once the drop system
    // event is persisted.
    class DroppedCollections {
    public:
        void insert(CollectionID cid, const DroppedCollectionInfo& info);
        void remove(CollectionID cid, uint64_t seqno);
        StatsForFlush get(CollectionID cid, uint64_t seqno) const;
        bool addStats(Vbid vbid, const StatCollector& collector) const;

        /// @return size of the map
        size_t size() const;
        /// @return size of the mapped value
        std::optional<size_t> size(CollectionID cid) const;

    private:
        std::unordered_map<CollectionID, std::vector<DroppedCollectionInfo>>
                droppedCollections;
        friend std::ostream& operator<<(std::ostream&,
                                        const DroppedCollections&);
    };
    // droppedCollections is managed separately, we don't want to exclusively
    // lock the entire 'map' when removing elements
    folly::Synchronized<DroppedCollections> droppedCollections;

    /**
     * shared lock to allow concurrent readers and safe updates
     */
    mutable mutex_type rwlock;

    /// The manifest UID which updated this vb::manifest
    ManifestUid manifestUid{0};

    /// Manager of collections
    const std::shared_ptr<Manager> manager;

    /**
     * Flag value which indicates if dropped collections exist, that includes
     * documents and metadata. This is atomic because we don't require the
     * 'manifest' rwlock and this value is written by compaction and read by
     * the flusher.
     *
     * Value is true if dropped collection data exists
     */
    std::atomic<bool> dropInProgress{false};

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);
    friend std::ostream& operator<<(std::ostream&,
                                    const DroppedCollectionInfo&);
    friend std::ostream& operator<<(std::ostream&, const DroppedCollections&);
};

/// Note that the VB::Manifest << operator does not obtain the rwlock
/// it is used internally in the object for exception string generation so must
/// not double lock.
std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

/// This is the locked version for printing the manifest
std::ostream& operator<<(std::ostream& os, const ReadHandle& readHandle);

std::ostream& operator<<(std::ostream&, const Manifest::DroppedCollectionInfo&);

std::ostream& operator<<(std::ostream&, const Manifest::DroppedCollections&);

} // end namespace VB
} // end namespace Collections
