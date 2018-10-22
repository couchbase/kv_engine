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

#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest_entry.h"
#include "systemevent.h"

#include <platform/non_negative_counter.h>
#include <platform/rwlock.h>
#include <platform/sized_buffer.h>

#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

class VBucket;

namespace flatbuffers {
class FlatBufferBuilder;
}

namespace Collections {
namespace VB {

/**
 * Collections::VB::Manifest is a container for all of the collections a VBucket
 * knows about.
 *
 * Each collection is represented by a Collections::VB::ManifestEntry and all of
 * the collections are stored in an unordered_map. The map is implemented to
 * allow look-up by collection-name without having to allocate a std::string,
 * callers only need a cb::const_char_buffer for look-ups.
 *
 * The Manifest allows for an external manager to drive the lifetime of each
 * collection - adding, begin/complete of the deletion phase.
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
    using container = ::std::unordered_map<CollectionID, ManifestEntry>;

    /**
     * RAII read locking for access to the Manifest.
     */
    class ReadHandle {
    public:
        ReadHandle(const Manifest& m, cb::RWLock& lock)
            : readLock(lock), manifest(m) {
        }

        ReadHandle(ReadHandle&& rhs)
            : readLock(std::move(rhs.readLock)), manifest(rhs.manifest) {
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
            return manifest.doesKeyContainValidCollection(key);
        }

        /**
         * Does the key belong to the specified scope?
         *
         * @param key the document key
         * @param scopeID the scopeID
         * @return true if it belongs to the given scope, false if not
         */
        bool doesKeyBelongToScope(DocKey key, ScopeID scopeID) const {
            return manifest.doesKeyBelongToScope(key, scopeID);
        }

        /**
         * Given a key and it's seqno, the manifest can determine if that key
         * is logically deleted - that is part of a collection which is in the
         * process of being erased.
         *
         * @return true if the key belongs to a deleted collection.
         */
        bool isLogicallyDeleted(DocKey key, int64_t seqno) const {
            return manifest.isLogicallyDeleted(key, seqno);
        }

        /**
         * @returns true/false if $default exists
         */
        bool doesDefaultCollectionExist() const {
            return manifest.doesDefaultCollectionExist();
        }

        /**
         * @returns true if collection is open, false if not or unknown
         */
        bool isCollectionOpen(CollectionID identifier) const {
            return manifest.isCollectionOpen(identifier);
        }

        /**
         * @return true if the collection exists in the internal container
         */
        bool exists(CollectionID identifier) const {
            return manifest.exists(identifier);
        }

        /// @return the manifest UID that last updated this vb::manifest
        ManifestUid getManifestUid() const {
            return manifest.getManifestUid();
        }

        uint64_t getItemCount(CollectionID collection) const {
            return manifest.getItemCount(collection);
        }

        bool addStats(Vbid vbid, const void* cookie, ADD_STAT add_stat) const {
            return manifest.addStats(vbid, cookie, add_stat);
        }

        void updateSummary(Summary& summary) const {
            manifest.updateSummary(summary);
        }

        /**
         * Dump this VB::Manifest to std::cerr
         */
        void dump() const {
            std::cerr << manifest << std::endl;
        }

    protected:
        friend std::ostream& operator<<(std::ostream& os,
                                        const Manifest::ReadHandle& readHandle);
        std::unique_lock<cb::ReaderLock> readLock;
        const Manifest& manifest;
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
        CachingReadHandle(const Manifest& m,
                          cb::RWLock& lock,
                          DocKey key,
                          bool allowSystem)
            : ReadHandle(m, lock),
              itr(m.getManifestEntry(key, allowSystem)),
              key(key) {
        }

        /**
         * @return true if the key used in construction is associated with a
         *         valid and open collection.
         */
        bool valid() const {
            if (iteratorValid()) {
                return itr->second.isOpen();
            }
            return false;
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
            return manifest.isLogicallyDeleted(itr, seqno);
        }

        /// @return the manifest UID that last updated this vb::manifest
        ManifestUid getManifestUid() const {
            return manifest.getManifestUid();
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
            // We don't include system events when counting
            if (key.getDocNamespace() == DocNamespace::System) {
                return;
            }
            return manifest.incrementDiskCount(itr);
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
            // We don't include system events when counting
            if (key.getDocNamespace() == DocNamespace::System) {
                return;
            }
            return manifest.decrementDiskCount(itr);
        }

        /**
         * Function intended for use by the KVBucket collection's eraser code.
         *
         * @return if the key@seqno indicates that we've now hit the end of
         *         a deleted collection and should call completeDeletion, then
         *         the return value is initialised with the collection which is
         *         to be deleted. If the key does not indicate the end, the
         *         return value is an empty char buffer.
         */
        boost::optional<CollectionID> shouldCompleteDeletion(
                int64_t bySeqno) const {
            return manifest.shouldCompleteDeletion(key, bySeqno, itr);
        }

        /**
         * Dump this VB::Manifest to std::cerr
         */
        void dump() {
            std::cerr << manifest << std::endl;
        }

    protected:
        bool iteratorValid() const {
            return itr != manifest.end();
        }

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
     * RAII write locking for access and updates to the Manifest.
     */
    class WriteHandle {
    public:
        WriteHandle(Manifest& m, cb::RWLock& lock)
            : writeLock(lock), manifest(m) {
        }

        WriteHandle(WriteHandle&& rhs)
            : writeLock(std::move(rhs.writeLock)), manifest(rhs.manifest) {
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
         * @return true if the update was applied
         */
        bool update(::VBucket& vb, const Collections::Manifest& newManifest) {
            return manifest.update(vb, newManifest);
        }

        /**
         * Complete the deletion of a collection, that is all data has been
         * erased and now the collection meta data can be erased
         *
         * @param vb The VBucket in which the deletion is occurring.
         * @param identifier The collection that has finished being deleted.
         */
        void completeDeletion(::VBucket& vb, CollectionID identifier) {
            manifest.completeDeletion(vb, identifier);
        }

        /**
         * Add a collection for a replica VB, this is for receiving
         * collection updates via DCP and the collection already has a start
         * seqno assigned.
         *
         * @param vb The vbucket to add the collection to.
         * @param manifestUid the uid of the manifest which made the change
         * @param identifiers ScopeID and CollectionID pair
         * @param startSeqno The start-seqno assigned to the collection.
         */
        void replicaAdd(::VBucket& vb,
                        ManifestUid manifestUid,
                        ScopeCollectionPair identifiers,
                        int64_t startSeqno) {
            manifest.addCollection(
                    vb, manifestUid, identifiers, OptionalSeqno{startSeqno});
        }

        /**
         * Begin a delete collection for a replica VB, this is for receiving
         * collection updates via DCP and the collection already has an end
         * seqno assigned.
         *
         * @param vb The vbucket to begin collection deletion on.
         * @param manifestUid the uid of the manifest which made the change
         * @param identifiers ScopeID and CollectionID pair
         * @param endSeqno The end-seqno assigned to the end collection.
         */
        void replicaBeginDelete(::VBucket& vb,
                                ManifestUid manifestUid,
                                ScopeCollectionPair identifiers,
                                int64_t endSeqno) {
            manifest.beginCollectionDelete(
                    vb, manifestUid, identifiers, OptionalSeqno{endSeqno});
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
        std::unique_lock<cb::WriterLock> writeLock;
        Manifest& manifest;
    };

    friend ReadHandle;
    friend CachingReadHandle;
    friend WriteHandle;

    /**
     * Construct a VBucket::Manifest optionally using serialised (flatbuffers)
     * data.
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
    Manifest(const PersistedManifest& data);

    ReadHandle lock() const {
        return {*this, rwlock};
    }

    CachingReadHandle lock(DocKey key, bool allowSystem = false) const {
        return {*this, rwlock, key, allowSystem};
    }

    // Explictly delete rvalue StoredDocKey usage. A CachingReadHandle wants to
    // create a view of the original key, so that key must have a life-span
    // which is longer than the handle. Only test-code trips over this as they
    // may create keys at the point of calling a method.
    CachingReadHandle lock(StoredDocKey&&,
                           bool allowSystem = false) const = delete;

    WriteHandle wlock() {
        return {*this, rwlock};
    }

    /**
     * Return a patched PersistedManifest object cloned and mutated from the
     * specified Item.
     *
     * The reason this is done is because when the Item was created and assigned
     * flatbuffer data, the seqno for the start/end of the collection was not
     * known, this call will patch the correct seqno into the created or dropped
     * collection entry.
     *
     * @param collectionsEventItem an Item created to represent a collection
     *        event. .
     * @return The patched PersistedManifest which can be stored to disk.
     */
    static PersistedManifest patchSerialisedData(
            const Item& collectionsEventItem);

    /**
     * Get the system event collection create/delete data from a SystemEvent
     * Item's value.
     *
     * @param serialisedManifest Serialised manifest data created by
     *        ::populateWithSerialisedData
     * @returns SystemEventData which carries all of the data which needs to be
     *          marshalled into a DCP system event message. Inside the returned
     *          object maybe sized_buffer objects which point into
     *          serialisedManifest.
     */
    static SystemEventData getSystemEventData(
            cb::const_char_buffer serialisedManifest);

    /**
     * For creation of collection SystemEvents - The SystemEventFactory
     * glues the CollectionID into the event key (so create of x doesn't
     * collide with create of y). This method basically reverses
     * makeCollectionIdIntoString so we can get a CollectionID from a
     * SystemEvent key
     *
     * @param key DocKey from a SystemEvent
     * @return the ID which was in the event
     */
    static CollectionID getCollectionIDFromKey(const DocKey& key);

    /**
     * Create a SystemEvent Item, the Item's value will contain data for later
     * consumption by patchSerialisedData/getSystemEventData
     *
     * @param se SystemEvent to create.
     * @param identifiers ScopeID and CollectionID pair
     * @param deleted If the Item should be marked deleted.
     * @param seqno An optional sequence number. If a seqno is specified, the
     *        returned item will have its bySeqno set to seqno.
     *
     * @returns unique_ptr to a new Item that represents the requested
     *          SystemEvent.
     */
    std::unique_ptr<Item> createSystemEvent(SystemEvent se,
                                            ScopeCollectionPair identifiers,
                                            bool deleted,
                                            OptionalSeqno seqno) const;

protected:

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
     * @return true if the update was applied
     */
    bool update(::VBucket& vb, const Collections::Manifest& manifest);

    /**
     * Sub-function used by update
     * Removes the last ID of the changes vector and then calls 'update' on
     * every remaining ID (using the current manifest ManifestUid).
     * So if the vector has 1 element, it returns that element and does nothing.
     *
     * @param update a function to call (either addCollection or
     *        beginCollectionDelete)
     * @param changes a vector of CollectionIDs to add/delete (based on update)
     * @return the last element of the changes vector
     */
    boost::optional<ScopeCollectionPair> applyChanges(
            std::function<void(ManifestUid, ScopeCollectionPair, OptionalSeqno)>
                    update,
            std::vector<ScopeCollectionPair>& changes);

    /**
     * Add a collection to the manifest.
     *
     * @param vb The vbucket to add the collection to.
     * @param manifestUid the uid of the manifest which made the change
     * @param identifiers ScopeID and CollectionID pair
     * @param optionalSeqno Either a seqno to assign to the new collection or
     *        none (none means the checkpoint will assign a seqno).
     */
    void addCollection(::VBucket& vb,
                       ManifestUid manifestUid,
                       ScopeCollectionPair identifiers,
                       OptionalSeqno optionalSeqno);

    /**
     * Begin a delete of the collection.
     *
     * @param vb The vbucket to begin collection deletion on.
     * @param manifestUid the uid of the manifest which made the change
     * @param identifiers ScopeID and CollectionID pair
     * @param revision manifest revision which started the deletion.
     * @param optionalSeqno Either a seqno to assign to the delete of the
     *        collection or none (none means the checkpoint assigns the seqno).
     */
    void beginCollectionDelete(::VBucket& vb,
                               ManifestUid manifestUid,
                               ScopeCollectionPair identifiers,
                               OptionalSeqno optionalSeqno);

    /**
     * Complete the deletion of a collection, that is all data has been
     * erased and now the collection meta data can be erased
     *
     * @param vb The VBucket in which the deletion is occurring.
     * @param identifier The collection that has finished being deleted.
     */
    void completeDeletion(::VBucket& vb, CollectionID identifier);

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
     * Does the key belong to the specified scope?
     *
     * @param key the document key
     * @param scopeID the scopeID
     * @return true if it belongs to the given scope, false if not
     */
    bool doesKeyBelongToScope(const DocKey& key, ScopeID scopeID) const;

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

    /**
     * Function intended for use by the collection eraser code, checking
     * keys@seqno status as we walk the by-seqno index.
     *
     * @param key Collections::DocKey
     * @param bySeqno Seqno assigned to the key
     * @param entry the manifest entry associated with key
     * @return if the key@seqno indicates that we've now hit the real end of a
     *         deleted collection (because the key is a system event) and the
     *         manifest entry determines we should call completeDeletion, then
     *         the return value is initialised with the collection which is to
     *         be deleted. If the key does not indicate the end, the return
     *         value is an empty buffer.
     */
    boost::optional<CollectionID> shouldCompleteDeletion(
            const DocKey& key,
            int64_t bySeqno,
            const container::const_iterator entry) const;

    /**
     * @returns true/false if $default exists
     */
    bool doesDefaultCollectionExist() const {
        return defaultCollectionExists;
    }

    /**
     * @returns true if the collection isOpen - false if not (or doesn't exist)
     */
    bool isCollectionOpen(CollectionID identifier) const {
        auto itr = map.find(identifier);
        if (itr != map.end()) {
            return itr->second.isOpen();
        }
        return false;
    }

    /**
     * @return true if the collection exists in the internal container
     */
    bool exists(CollectionID identifier) const {
        return map.count(identifier) > 0;
    }

    /**
     * @return the number of items stored for collection
     */
    uint64_t getItemCount(CollectionID collection) const;

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

    /// @return the manifest UID that last updated this vb::manifest
    ManifestUid getManifestUid() const {
        return manifestUid;
    }

    /**
     * Detailed stats for this VB::Manifest
     * @return true if addStats was successful, false if failed.
     */
    bool addStats(Vbid vbid, const void* cookie, ADD_STAT add_stat) const;

    void updateSummary(Summary& summary) const;

    /**
     * Add a collection entry to the manifest specifing the revision that it was
     * seen in and the sequence number span covering it.
     * @param identifiers ScopeID and CollectionID pair
     * @param startSeqno The seqno where the collection begins. Defaults to 0.
     * @param endSeqno The seqno where it ends (can be the special open
     * marker). Defaults to the collection open state (-6).
     * @return a non const reference to the new ManifestEntry so the caller can
     *         set the correct seqno.
     */
    ManifestEntry& addNewCollectionEntry(
            ScopeCollectionPair identifiers,
            int64_t startSeqno = 0,
            int64_t endSeqno = StoredValue::state_collection_open);

    /**
     * Get the ManifestEntry for the given collection. Throws an
     * std::logic_error if the collection was not found.
     *
     * @param collectionID CollectionID of the collection to lookup
     * @return a reference to the ManifestEntry
     */
    ManifestEntry& getManifestEntry(CollectionID collectionID);

    /**
     * Process a Collections::Manifest to determine if collections need adding
     * or removing.
     *
     * This function returns two sets of collections. Those which are being
     * added and those which are being deleted.
     *
     * @param manifest The Manifest to compare with.
     * @returns An optional containing a std::pair of vectors, if successful
     *          first contains ScopeCollectionPairs that need adding whilst
     *          second contains those which should be deleted. If the
     *          optional is uninitialised than the manifest could
     *          not be processed against this, the only error is trying to
     *          add a deleting collection-ID.
     */
    using processResult =
            boost::optional<std::pair<std::vector<ScopeCollectionPair>,
                                      std::vector<ScopeCollectionPair>>>;
    processResult processManifest(const Collections::Manifest& manifest) const;

    /**
     * Create an Item that carries a system event and queue it to the vb
     * checkpoint.
     *
     * @param vb The vbucket onto which the Item is queued.
     * @param se The SystemEvent to create and queue.
     * @param identifiers ScopeID and CollectionID pair
     * @param deleted If the Item created should be marked as deleted.
     * @param seqno An optional seqno which if set will be assigned to the
     *        system event.
     *
     * @returns The sequence number of the queued Item.
     */
    int64_t queueSystemEvent(::VBucket& vb,
                             SystemEvent se,
                             ScopeCollectionPair identifiers,
                             bool deleted,
                             OptionalSeqno seqno) const;

    /**
     * Populate a buffer with the serialised state of the manifest. The
     * serialised state also always places the mutated CollectionEntry as the
     * last element of the entries Vector. The mutated entry represents the
     * created collection or the one we've dropped.
     *
     * @param builder FlatBuffer builder for serialisation.
     * @param identifiers ScopeID/CollectionID pair of the mutated collection
     */
    void populateWithSerialisedData(flatbuffers::FlatBufferBuilder& builder,
                                    ScopeCollectionPair identifiers) const;

    /**
     * Update greatestEndSeqno if the seqno is larger
     * @param seqno an endSeqno for a deleted collection
     */
    void trackEndSeqno(int64_t seqno);

    /**
     * For creation of collection SystemEvents - The SystemEventFactory
     * glues the CollectionID into the event key (so create of x doesn't
     * collide with create of y). This method yields the 'keyExtra' parameter
     *
     * @param collection The value to turn into a string
     * @return the keyExtra parameter to be passed to SystemEventFactory
     */
    static std::string makeCollectionIdIntoString(CollectionID collection);

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
     * Does the current set contain the default collection?
     */
    bool defaultCollectionExists;

    /**
     * A key below this seqno might be logically deleted and should be checked
     * against the manifest. This member will be set to
     * StoredValue::state_collection_open when no collections are being deleted
     * and the greatestEndSeqno has no use (all collections exclusively open)
     */
    int64_t greatestEndSeqno;

    /**
     * The manifest tracks how many collections are being deleted so we know
     * when greatestEndSeqno can be set to StoredValue::state_collection_open
     */
    cb::NonNegativeCounter<size_t, cb::ThrowExceptionUnderflowPolicy>
            nDeletingCollections;

    /// The manifest UID which updated this vb::manifest
    ManifestUid manifestUid{0};

    /**
     * shared lock to allow concurrent readers and safe updates
     */
    mutable cb::RWLock rwlock;

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

    static constexpr char const* CollectionsKey = "collections";
    static constexpr nlohmann::json::value_t CollectionsType =
            nlohmann::json::value_t::array;
    static constexpr char const* UidKey = "uid";
    static constexpr nlohmann::json::value_t UidType =
            nlohmann::json::value_t::string;
    static constexpr char const* SidKey = "sid";
    static constexpr nlohmann::json::value_t SidType =
            nlohmann::json::value_t::string;
    static constexpr char const* StartSeqnoKey = "startSeqno";
    static constexpr nlohmann::json::value_t StartSeqnoType =
            nlohmann::json::value_t::string;
    static constexpr char const* EndSeqnoKey = "endSeqno";
    static constexpr nlohmann::json::value_t EndSeqnoType =
            nlohmann::json::value_t::string;
};

/// Note that the VB::Manifest << operator does not obtain the rwlock
/// it is used internally in the object for exception string generation so must
/// not double lock.
std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

/// This is the locked version for printing the manifest
std::ostream& operator<<(std::ostream& os,
                         const Manifest::ReadHandle& readHandle);

} // end namespace VB
} // end namespace Collections
