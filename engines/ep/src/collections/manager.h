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

#include "collections/manifest.h"

#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <folly/Synchronized.h>
#include <memory>
#include <mutex>

class KVBucket;
class VBucket;

namespace Collections {

/**
 * Collections::Manager provides some bucket level management functions
 * such as the code which enables the MCBP set_collections command.
 */
class Manager {
public:
    Manager();

    /**
     * Update the bucket with the latest JSON collections manifest.
     *
     * Locks the Manager and prevents concurrent updates, concurrent updates
     * are failed with TMPFAIL as in reality there should be 1 admin connection.
     *
     * @param bucket the bucket receiving a set-collections command.
     * @param manifest the json manifest form a set-collections command.
     * @returns engine_error indicating why the update failed.
     */
    cb::engine_error update(KVBucket& bucket, std::string_view manifest);

    /**
     * Retrieve the current manifest
     * @return pair with status and if success JSON version of the current
     *         manifest
     */
    std::pair<cb::mcbp::Status, std::string> getManifest() const;

    /**
     * Lookup collection id from path
     *
     * @throws cb::engine_error
     * @return EngineErrorGetCollectionIDResult which is status, manifest-uid
     *  and collection-cid
     */
    cb::EngineErrorGetCollectionIDResult getCollectionID(
            std::string_view path) const;

    /**
     * Lookup scope id from path
     *
     * @throws cb::engine_error
     * @return EngineErrorGetScopeIDResult which is status, manifest-uid
     *  and scope-id
     */
    cb::EngineErrorGetScopeIDResult getScopeID(std::string_view path) const;

    /**
     * Lookup scope id from DocKey
     *
     * @return optional scope-ID initialised if the lookup was successful
     */
    std::pair<uint64_t, boost::optional<ScopeID>> getScopeID(
            const DocKey& key) const;

    /**
     * Update the vbucket's manifest with the current Manifest
     * The Manager is locked to prevent current changing whilst this update
     * occurs.
     */
    void update(VBucket& vb) const;

    /**
     * Do 'add_stat' calls for the bucket to retrieve summary collection stats
     */
    void addCollectionStats(const void* cookie,
                            const AddStatFn& add_stat) const;

    /**
     * Do 'add_stat' calls for the bucket to retrieve summary scope stats
     */
    void addScopeStats(const void* cookie, const AddStatFn& add_stat) const;

    /**
     * Perform actions for a completed warmup - currently check if any
     * collections are 'deleting' and require erasing retriggering.
     */
    void warmupCompleted(KVBucket& bucket) const;

    /**
     * For development, log as much collections stuff as we can
     */
    void logAll(KVBucket& bucket) const;

    /**
     * Write to std::cerr this
     */
    void dump() const;

    /**
     * Perform the gathering of collection stats for the bucket.
     */
    static ENGINE_ERROR_CODE doCollectionStats(KVBucket& bucket,
                                               const void* cookie,
                                               const AddStatFn& add_stat,
                                               const std::string& statKey);

    /**
     * Perform the gathering of scope stats for the bucket.
     */
    static ENGINE_ERROR_CODE doScopeStats(KVBucket& bucket,
                                          const void* cookie,
                                          const AddStatFn& add_stat,
                                          const std::string& statKey);

private:
    /**
     * Apply newManifest to all active vbuckets
     * @return uninitialized if success, else the vbid which triggered failure.
     */
    boost::optional<Vbid> updateAllVBuckets(KVBucket& bucket,
                                            const Manifest& newManifest);

    /**
     * validate the path is correctly formed for get_collection_id.
     *
     * A correctly formed path has exactly 1 separator.
     *
     * path components are not validated here as the path is not broken into
     * scope/collection components.
     * @returns true if path is correctly formed, false is not
     */
    static bool validateGetCollectionIDPath(std::string_view path);

    /**
     * validate the path is correctly formed for get_scope_id.
     *
     * A correctly formed path has 0 or 1 separator.
     *
     * path components are not validated here as the path is not broken into
     * scope/collection components
     * @returns true if path is correctly formed, false is not
     */
    static bool validateGetScopeIDPath(std::string_view path);

    friend std::ostream& operator<<(std::ostream& os, const Manager& manager);

    /// Store the most recent (current) manifest received
    folly::Synchronized<Manifest> currentManifest;
};

std::ostream& operator<<(std::ostream& os, const Manager& manager);
} // namespace Collections
