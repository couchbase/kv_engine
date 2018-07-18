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

#include <mutex>
#include <unordered_map>

class VBucket;

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
         * Function intended for use by the collection's eraser code, i.e. when
         * processing the by-seqno index
         *
         * @return if the key indicates that we've now hit the end of
         *         a deleted collection seqno-span the return value tells the
         *         caller to call completeDeletion, an ID is returned. else
         *         return uninitialised
         */
        boost::optional<CollectionID> shouldCompleteDeletion(DocKey key) const {
            return manifest.shouldCompleteDeletion(key);
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
        uid_t getManifestUid() const {
            return manifest.getManifestUid();
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
        uid_t getManifestUid() const {
            return manifest.getManifestUid();
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
         */
        void update(::VBucket& vb, const Collections::Manifest& newManifest) {
            manifest.update(vb, newManifest);
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
         * @param identifier CID for the collection being added.
         * @param startSeqno The start-seqno assigned to the collection.
         */
        void replicaAdd(::VBucket& vb,
                        uid_t manifestUid,
                        CollectionID identifier,
                        int64_t startSeqno) {
            manifest.addCollection(
                    vb, manifestUid, identifier, OptionalSeqno{startSeqno});
        }

        /**
         * Begin a delete collection for a replica VB, this is for receiving
         * collection updates via DCP and the collection already has an end
         * seqno assigned.
         *
         * @param vb The vbucket to begin collection deletion on.
         * @param manifestUid the uid of the manifest which made the change
         * @param identifier CID for the collection being removed.
         * @param endSeqno The end-seqno assigned to the end collection.
         */
        void replicaBeginDelete(::VBucket& vb,
                                uid_t manifestUid,
                                CollectionID identifier,
                                int64_t endSeqno) {
            manifest.beginCollectionDelete(
                    vb, manifestUid, identifier, OptionalSeqno{endSeqno});
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
     * Construct a VBucket::Manifest from a JSON string or an empty string.
     *
     * Empty string allows for construction where no JSON data was found i.e.
     * an upgrade occurred and this is the first construction of a manifest
     * for a VBucket which has persisted data, but no manifest data. When an
     * empty string is used, the manifest will initialise with default settings.
     * - Default Collection enabled.
     * - Separator defined as Collections::DefaultSeparator
     *
     * A non-empty string must be a valid JSON manifest that determines which
     * collections to instantiate.
     *
     * @param manifest A std::string containing a JSON manifest. An empty string
     *        indicates no manifest and is valid.
     */
    Manifest(const std::string& manifest);

    ReadHandle lock() const {
        return {*this, rwlock};
    }

    CachingReadHandle lock(DocKey key, bool allowSystem = false) const {
        return {*this, rwlock, key, allowSystem};
    }

    WriteHandle wlock() {
        return {*this, rwlock};
    }

    /**
     * Return a std::string containing a JSON representation of a
     * VBucket::Manifest. The input is an Item previously created for an event
     * with the value being a serialised binary blob which is turned into JSON.
     *
     * When the Item was created it did not have a seqno for the collection
     * entry being modified, this function will return JSON data with the
     * missing seqno 'patched' with the 'collectionsEventItem's seqno.
     *
     * @param collectionsEventItem an Item created to represent a collection
     *        event. The value of which is converted to JSON.
     * @return JSON representation of the Item's value.
     */
    static std::string serialToJson(const Item& collectionsEventItem);

    /**
     * Get the system event collection create/delete data from a SystemEvent
     * Item's value. This returns the information that DCP needs to create a
     * DCPSystemEvent packet for the create/delete.
     *
     * @param serialisedManifest Serialised manifest data created by
     *        ::populateWithSerialisedData
     * @returns SystemEventData which carries all of the data which needs to be
     *          marshalled into a DCP system event message. Inside the returned
     *          object maybe sized_buffer objects which point into
     *          serialisedManifest.
     */
    static SystemEventDcpData getSystemEventDcpData(
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
     * consumption by serialToJson
     *
     * @param se SystemEvent to create.
     * @param identifier The CollectionID of the collection which is changing.
     * @param deleted If the Item should be marked deleted.
     * @param seqno An optional sequence number. If a seqno is specified, the
     *        returned item will have its bySeqno set to seqno.
     *
     * @returns unique_ptr to a new Item that represents the requested
     *          SystemEvent.
     */
    std::unique_ptr<Item> createSystemEvent(SystemEvent se,
                                            CollectionID identifier,
                                            bool deleted,
                                            OptionalSeqno seqno) const;

private:

    /**
     * Return a std::string containing a JSON representation of a
     * VBucket::Manifest. The input data should be a previously serialised
     * object, i.e. the input to this function is the output of
     * populateWithSerialisedData(cb::char_buffer out)
     *
     * @param buffer The raw data to process.
     * @returns std::string containing a JSON representation of the manifest
     */
    static std::string serialToJson(cb::const_char_buffer buffer);

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
     */
    void update(::VBucket& vb, const Collections::Manifest& manifest);

    /**
     * Add a collection to the manifest.
     *
     * @param vb The vbucket to add the collection to.
     * @param manifestUid the uid of the manifest which made the change
     * @param identifier CollectionID of the new collection.
     * @param optionalSeqno Either a seqno to assign to the new collection or
     *        none (none means the checkpoint will assign a seqno).
     */
    void addCollection(::VBucket& vb,
                       uid_t manifestUid,
                       CollectionID identifier,
                       OptionalSeqno optionalSeqno);

    /**
     * Begin a delete of the collection.
     *
     * @param vb The vbucket to begin collection deletion on.
     * @param manifestUid the uid of the manifest which made the change
     * @param identifier CollectionID of the deleted collection.
     * @param revision manifest revision which started the deletion.
     * @param optionalSeqno Either a seqno to assign to the delete of the
     *        collection or none (none means the checkpoint assigns the seqno).
     */
    void beginCollectionDelete(::VBucket& vb,
                               uid_t manifestUid,
                               CollectionID identifier,
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

    /**
     * Function intended for use by the collection's eraser code.
     *
     * @return if the key indicates that we've now hit the end of
     *         a deleted collection and should call completeDeletion the
     *         collectionId, else return uninitialised
     */
    boost::optional<CollectionID> shouldCompleteDeletion(
            const DocKey& key) const;

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

    container::const_iterator end() const {
        return map.end();
    }

    /**
     * Get a manifest entry for the collection associated with the key. Can
     * return map.end() for unknown collections.
     */
    container::const_iterator getManifestEntry(const DocKey& key,
                                               bool allowSystem) const;

    /// @return the manifest UID that last updated this vb::manifest
    uid_t getManifestUid() const {
        return manifestUid;
    }

protected:
    /**
     * Add a collection entry to the manifest specifing the revision that it was
     * seen in and the sequence number for the point in 'time' it was created.
     *
     * @param identifier CollectionID of the collection to add.
     * @return a non const reference to the new/updated ManifestEntry so the
     *         caller can set the correct seqno.
     */
    ManifestEntry& addCollectionEntry(CollectionID identifier);

    /**
     * Add a collection entry to the manifest specifing the revision that it was
     * seen in and the sequence number span covering it.
     *
     * @param identifier CollectionID of the collection to add.
     * @param startSeqno The seqno where the collection begins
     * @param endSeqno The seqno where it ends (can be the special open marker)
     * @return a non const reference to the new ManifestEntry so the caller can
     *         set the correct seqno.
     */
    ManifestEntry& addNewCollectionEntry(CollectionID identifier,
                                         int64_t startSeqno,
                                         int64_t endSeqno);

    /**
     * Begin the deletion process by marking the collection entry with the seqno
     * that represents its end.
     *
     * After "begin" delete a collection can be added again or fully deleted
     * by the completeDeletion method.
     *
     * @param identifier CollectionID of the collection to delete.
     * @return a reference to the updated ManifestEntry
     */
    ManifestEntry& beginDeleteCollectionEntry(CollectionID identifier);

    /**
     * Process a Collections::Manifest to determine if collections need adding
     * or removing.
     *
     * This function returns two sets of collections. Those which are being
     * added and those which are being deleted.
     *
     * @param manifest The Manifest to compare with.
     * @returns A pair of vectors containing the required changes, first
     *          contains collections that need adding whilst second contains
     *          those which should be deleted.
     */
    using processResult =
            std::pair<std::vector<CollectionID>, std::vector<CollectionID>>;
    processResult processManifest(const Collections::Manifest& manifest) const;

    /**
     * Create an Item that carries a system event and queue it to the vb
     * checkpoint.
     *
     * @param vb The vbucket onto which the Item is queued.
     * @param se The SystemEvent to create and queue.
     * @param identifier The CollectionID of the collection being added/deleted.
     * @param deleted If the Item created should be marked as deleted.
     * @param seqno An optional seqno which if set will be assigned to the
     *        system event.
     *
     * @returns The sequence number of the queued Item.
     */
    int64_t queueSystemEvent(::VBucket& vb,
                             SystemEvent se,
                             CollectionID identifier,
                             bool deleted,
                             OptionalSeqno seqno) const;

    /**
     * Obtain how many bytes of storage are needed for a serialised copy
     * of this object including the size of the modified collection.
     *
     * @param identifier The ID of the collection being changed.
     * @returns how many bytes will be needed to serialise the manifest and
     *          the collection being changed.
     */
    size_t getSerialisedDataSize(CollectionID identifier) const;

    /**
     * Obtain how many bytes of storage are needed for a serialised copy
     * of this object.
     *
     * @returns how many bytes will be needed to serialise the manifest.
     */
    size_t getSerialisedDataSize() const;

    /**
     * Populate a buffer with the serialised state of the manifest and one
     * additional entry that is the collection being changed, i.e. the addition
     * or deletion.
     *
     * @param out A buffer for the data to be written into.
     * @param revision The Manifest revision we are processing
     * @param identifier The CollectionID of the collection being added/deleted
     */
    void populateWithSerialisedData(cb::char_buffer out,
                                    CollectionID identifier) const;

    /**
     * @returns the string for the given key from the cJSON object.
     */
    const char* getJsonEntry(cJSON* cJson, const char* key);

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
    uid_t manifestUid;

    /**
     * shared lock to allow concurrent readers and safe updates
     */
    mutable cb::RWLock rwlock;

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);
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
