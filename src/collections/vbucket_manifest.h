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

#include "collections/collections_dockey.h"
#include "collections/collections_types.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest_entry.h"
#include "systemevent.h"

#include <platform/sized_buffer.h>
#include <platform/rwlock.h>

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
 * Access to the class is peformed by the ReadHandle and WriteHandle classes
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
        bool doesKeyContainValidCollection(::DocKey key) {
            return manifest.doesKeyContainValidCollection(key);
        }

    private:
        std::unique_lock<cb::ReaderLock> readLock;
        const Manifest& manifest;
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
         * Complete the deletion of a collection.
         *
         * Lookup the collection name and determine the deletion actions.
         * A collection could of been added again during a background delete so
         * completeDeletion may just update the state or fully drop all
         * knowledge of the collection.
         *
         * @param vb The VBucket in which the deletion is occuring.
         * @param collection The collection that is being deleted.
         * @param revision The Manifest revision which initiated the delete.
         */
        void completeDeletion(::VBucket& vb,
                              cb::const_char_buffer collection,
                              uint32_t revision) {
            manifest.completeDeletion(vb, collection, revision);
        }

    private:
        std::unique_lock<cb::WriterLock> writeLock;
        Manifest& manifest;
    };

    friend ReadHandle;
    friend WriteHandle;

    /**
     * Map from a 'string_view' to an entry.
     * The key points to data stored in the value (which is actually a pointer
     * to a value to remove issues where objects move).
     * Using the string_view as the key allows faster lookups, the caller
     * need not heap allocate.
     */
    using container = ::std::unordered_map<cb::const_char_buffer,
                                           std::unique_ptr<ManifestEntry>>;

    /**
     * Construct with the default settings for collections
     * - The default separator ::
     * - The $default collection exists (at revision 0)
     */
    Manifest() : defaultCollectionExists(false), separator(DefaultSeparator) {
        addCollection(DefaultCollectionIdentifier,
                      0,
                      0,
                      StoredValue::state_collection_open);
    }

    /**
     * Construct a VBucket::Manifest from json created by serialToJson
     * @param manifest A JSON string previously generated by serialToJson.
     */
    Manifest(const std::string& manifest);

    ReadHandle lock() const {
        return {*this, rwlock};
    }

    WriteHandle wlock() {
        return {*this, rwlock};
    }

    /**
     * Return a std::string containing a JSON representation of a
     * VBucket::Manifest. The input data should be a previously serialised
     * object, i.e. the input to this function is the output of
     * populateWithSerialisedData.
     *
     * The function also corrects the seqno of the entry which initiated a
     * manifest update (i.e. a collection create or delete). This is because at
     * the time of serialisation, the collection SystemEvent Item did not have a
     * seqno.
     *
     * @param se The SystemEvent triggering the JSON generation.
     * @param buffer The raw data to process.
     * @param finalEntrySeqno The correct seqno to use for the final entry of
     *        the serialised data.
     */
    static std::string serialToJson(SystemEvent se,
                                    cb::const_char_buffer buffer,
                                    int64_t finalEntrySeqno);

private:
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
     * Complete the deletion of a collection.
     *
     * Lookup the collection name and determine the deletion actions.
     * A collection could of been added again during a background delete so
     * completeDeletion may just update the state or fully drop all knowledge of
     * the collection.
     *
     * @param vb The VBucket in which the deletion is occuring.
     * @param collection The collection that is being deleted.
     * @param revision The Manifest revision which initiated the delete.
     */
    void completeDeletion(::VBucket& vb,
                          cb::const_char_buffer collection,
                          uint32_t revision);

    /**
     * Does the key contain a valid collection?
     *
     * - If the key applies to the default collection, the default collection
     *   must exist.
     *
     * - If the key applies to a collection, the collection must exist and must
     *   not be in the process of deletion.
     */
    bool doesKeyContainValidCollection(const ::DocKey& key) const;

protected:
    /**
     * Add a collection to the manifest specifing the Collections::Manifest
     * revision that it was seen in and the sequence number for the point in
     * 'time' it was created.
     *
     * @param collection Name of the collection to add.
     * @param revision The revision of the Collections::Manifest triggering this
     *        add.
     * @param seqno The seqno of an Item which represents the creation event.
     */
    void addCollection(cb::const_char_buffer collection,
                       uint32_t revision,
                       int64_t startSeqno,
                       int64_t endSeqno);

    /**
     * Begin the deletion process by marking the collection with the seqno that
     * represents its end.
     *
     * After "begin" delete a collection can be added again or fully deleted
     * by the completeDeletion method.
     *
     * @param collection Name of the collection to delete.
     * @param revision Revision of the Manifest triggering the delete.
     * @param seqno The seqno of the deleted event mutation for the collection
     *        deletion,
     */
    void beginDelCollection(cb::const_char_buffer collection,
                            uint32_t revision,
                            int64_t seqno);

    /**
     * Processs a Collections::Manifest
     *
     * This function returns two sets of collections. Those which are being
     * added and those which are being deleted.
     *
     * @param manifest The Manifest to compare with.
     * @returns A pair of vector containing the required changes, first contains
     *          collections that need adding whilst second contains those which
     *          should be deleted.
     */
    using processResult =
            std::pair<std::vector<std::string>, std::vector<std::string>>;
    processResult processManifest(const Collections::Manifest& manifest) const;

    /**
     * Create a SystemEvent Item, the Item's value will contain data for later
     * consumption by serialToJson
     *
     * @param se SystemEvent to create.
     * @param collection The collection which is changing,
     * @param revisionForKey the revision which will be appended to the events
     *        DocKey
     * @param revision Manifest revision triggering the update, this value will
     *        be used for the JSON manifest update (as part of flushing).
     */
    std::unique_ptr<Item> createSystemEvent(SystemEvent se,
                                            cb::const_char_buffer collection,
                                            uint32_t revisionForKey,
                                            uint32_t revision) const;

    /**
     * Create an Item that carries a system event and queue it.
     * @param vb The Vbucket to update.
     * @param se The SystemEvent to create and queue.
     */
    int64_t queueSystemEvent(::VBucket& vb,
                             SystemEvent se,
                             cb::const_char_buffer collection,
                             uint32_t revisionForKey,
                             uint32_t revision) const;

    /**
     * Obtain how many bytes of storage are needed for a serialised copy
     * of this object including if required, the size of the changing
     * collection.
     * @param collection The name of the collection being changed. It's size is
     *        included in the returned value.
     */
    size_t getSerialisedDataSize(cb::const_char_buffer collection) const;

    /**
     * Populate a buffer with the serialised state of the manifest and one
     * additional entry that is the collection being changed, i.e. the addition
     * or deletion.
     *
     * @param out The location and size we wish to update
     * @param revision The Manifest revision we are processing
     * @param collection The collection being added/deleted
     * @param se The SystemEvent we're working on
     */
    void populateWithSerialisedData(cb::char_buffer out,
                                    cb::const_char_buffer collection,
                                    uint32_t revision,
                                    SystemEvent se) const;

    /**
     * Return the string for the key
     */
    const char* getJsonEntry(cJSON* cJson, const char* key);

    /**
     * The current set of collections
     */
    container map;

    /**
     * Does the current set contain the default collection?
     */
    bool defaultCollectionExists;

    /**
     * The collection separator
     */
    const std::string separator;

    /**
     * shared lock to allow concurrent readers and safe updates
     */
    mutable cb::RWLock rwlock;

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);
};

std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

} // end namespace VB
} // end namespace Collections
