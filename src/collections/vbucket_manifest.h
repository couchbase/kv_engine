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
#include "rwlock.h"

#include <mutex>
#include <unordered_map>

namespace Collections {
namespace VB {

/**
 * Collections::VB::Manifest is a container for all of the collections a VBucket
 * knows about.
 *
 * Each collection is represented by a Collections::VB::ManifestEntry and all of
 * the collections are stored in an unordered_map. The map is implemented to
 * allow look-up by collection-name without having to allocate a string as the
 * data-path will need to do collection validity checks with minimal penalty.
 *
 * The Manifest allows for an external manager to drive the lifetime of each
 * collection - adding, begin/complete of the deletion phase.
 *
 * This class is intended to be thread safe - the useage pattern is likely that
 * we will have many threads reading the collection state (i.e. policing
 * incoming database operations) whilst occasionally the collection state will
 * be changed by another thread calling "update" or "completeDeletion".
 *
 */
class Manifest {
public:
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
     * Update from a Collections::Manifest
     *
     * Update compares the current collection set against the manifest and
     * triggers collection creation and collection deletion.
     */
    void update(const Collections::Manifest& manifest);

    /**
     * Complete the deletion of a collection.
     *
     * Lookup the collection name and determine the deletion actions.
     * A collection could of been added again during a background delete so
     * completeDeletion may just update the state or fully drop all knowledge of
     * the collection.
     *
     * @param collection The collection that is being deleted.
     */
    void completeDeletion(const std::string& collection);

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
    void addCollection(const std::string& collection,
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
    void beginDelCollection(const std::string& collection,
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
    mutable RWLock lock;

    /**
     * For testing without a real checkpointManager we use this variable to
     * obtain sequence numbers for events. To be removed when VBucket
     * integration is done.
     */
    int64_t fakeSeqno{1};

    friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest);
};

std::ostream& operator<<(std::ostream& os, const Manifest& manifest);

} // end namespace VB
} // end namespace Collections
