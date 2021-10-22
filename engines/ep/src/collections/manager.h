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

#include "collections/manifest.h"
#include "collections/shared_metadata_table.h"

#include <folly/Synchronized.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <testing_hook.h>
#include <memory>
#include <mutex>

class EPBucket;
class EventuallyPersistentEngine;
class StatCollector;
class BucketStatCollector;
class KVBucket;
class VBucket;

namespace Collections {

/**
 * Copy of per-collection stats which may be expensive to collect repeatedly
 * (e.g., may require vbucket visiting). After collecting the stats once
 * (see Collections::Manager::getPerCollectionStats) they can be used to
 * format stats for multiple collections or scopes.
 */
class CachedStats {
public:
    /**
     * @param colMemUsed a map of collection to mem_used, object takes ownership
     * @param accumulatedStats a map of collection to AccumulatedStats, object
     * takes ownership
     */
    CachedStats(std::unordered_map<CollectionID, size_t>&& colMemUsed,
                std::unordered_map<CollectionID, AccumulatedStats>&&
                        accumulatedStats);
    /**
     * Add stats for a single collection.
     * @param scopeName the name of the collections scope
     * @param collection The entry object
     * @param collector stat collector to which stats will be added
     */
    void addStatsForCollection(std::string_view scopeName,
                               const CollectionEntry& collection,
                               const BucketStatCollector& collector);

    /**
     * Add stats for a single scope, by aggregating over all collections in the
     * scope.
     * @param sid ID of the scope generating stats for
     * @param scopeName Name of the scope
     * @param scopeCollections All collections in the scope
     * @param collector stat collector to which stats will be added
     */
    void addStatsForScope(
            ScopeID sid,
            std::string_view scopeName,
            const std::vector<Collections::CollectionEntry>& scopeCollections,
            const BucketStatCollector& collector);

private:
    /**
     * Add stats aggregated over a number of collections.
     *
     * @param cids collections to aggregate over
     * @param collector collector to add stats to. Should be a scope or
     * collection collector.
     */
    void addAggregatedCollectionStats(const std::vector<CollectionID>& cids,
                                      const StatCollector& collector);
    std::unordered_map<CollectionID, size_t> colMemUsed;
    std::unordered_map<CollectionID, AccumulatedStats> accumulatedStats;
};

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
     * For persistent buckets, this will store the manifest first and then use
     * IO complete success to perform the apply the new manifest. Ephemeral
     * buckets the update is 'immediate'.
     *
     * Note that a mutex ensures that this update method works serially, no
     * concurrent admin updates allowed.
     *
     * @param bucket the bucket receiving a set-collections command.
     * @param manifest the json manifest form a set-collections command.
     * @returns engine_error indicating why the update failed.
     */
    cb::engine_error update(KVBucket& bucket,
                            std::string_view manifest,
                            const CookieIface* cookie);

    /**
     * Function used to provide a status when any update PersistManifestTask
     * completes.
     * @param engine The task's engine
     * @param cookie Cookie of the command that triggered the task
     * @param status The final status of the task execution
     */
    void updatePersistManifestTaskDone(EventuallyPersistentEngine& engine,
                                       const CookieIface* cookie,
                                       cb::engine_errc status);

    /**
     * Retrieve the current manifest
     * @param isVisible function for determining what parts of the manifest the
     *        caller is allowed to see.
     * @return pair with status and if success JSON object of the current
     *         manifest
     */
    std::pair<cb::mcbp::Status, nlohmann::json> getManifest(
            const Collections::IsVisibleFunction& isVisible) const;

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
    std::pair<uint64_t, std::optional<ScopeID>> getScopeID(CollectionID) const;

    /**
     * Method to check if a ScopeID is valid
     * @param sid scoped id to validate
     * @return cb::EngineErrorGetScopeIDResult containing the status of success
     * with the sid and manifest uid if the scope exists otherwise the status
     * will be unknown_scope and the manifest uid will also be set.
     */
    cb::EngineErrorGetScopeIDResult isScopeIDValid(ScopeID sid) const;

    /**
     * Method to check if the vbucket needs collection state updating from the
     * current manifest
     * @param vb Vbucket to check
     * @return true if the VB will need updating to the current manifest
     */
    bool needsUpdating(const VBucket& vb) const;

    /**
     * Update the vbucket's manifest with the current Manifest. This is a no-op
     * if they are equal.
     * The Manager is locked to prevent current changing whilst this update
     * occurs.
     */
    void maybeUpdate(VBucket& vb) const;

    /**
     * Do 'add_stat' calls for the bucket to retrieve summary collection stats
     */
    void addCollectionStats(KVBucket& bucket,
                            const BucketStatCollector& collector) const;

    /**
     * Do 'add_stat' calls for the bucket to retrieve summary scope stats
     */
    void addScopeStats(KVBucket& bucket,
                       const BucketStatCollector& collector) const;

    /**
     * Called from bucket warmup - see if we have a manifest to resume from
     *
     * @return false if the manifest was found but cannot be loaded (e.g.
     * corruption or system error)
     */
    bool warmupLoadManifest(const std::string& dbpath);

    /**
     * Perform actions for a completed warmup - currently check if any
     * collections are 'deleting' and require erasing retriggering.
     */
    void warmupCompleted(EPBucket& bucket) const;

    /**
     * For development, log as much collections stuff as we can
     */
    void logAll(KVBucket& bucket) const;

    /**
     * Write to std::cerr this
     */
    void dump() const;

    /**
     * Added for testing, get a reference to the 'locked' manifest
     */
    folly::Synchronized<Manifest>& getCurrentManifest() {
        return currentManifest;
    }

    /**
     * When vbuckets create a collection (from new manifest of over DCP) all
     * names are shared (so we don't store per vbucket copies).
     *
     * This method gets shared "handle" for the given collection name.
     * @param cid Collection the metadata belongs to
     * @param view The view of the metadata to use in lookup
     */
    SingleThreadedRCPtr<const VB::CollectionSharedMetaData>
    createOrReferenceMeta(CollectionID cid,
                          const VB::CollectionSharedMetaDataView& view);

    /**
     * When vbuckets drop a collection (from new manifest of over DCP) or the
     * vbucket is deleted( VB::Manifest destructs) the vbucket must dereference
     * all names it previously retrieved from createOrReferenceMeta
     *
     * @param cid Collection the name belongs to
     * @param meta Reference to the meta that was originally given out by
     *        createOrReferenceMeta.
     */
    void dereferenceMeta(
            CollectionID cid,
            SingleThreadedRCPtr<const VB::CollectionSharedMetaData>&& meta);

    /**
     * When vbuckets create a scope (from new manifest of over DCP) all
     * names are shared (so we don't store per vbucket copies).
     *
     * This method gets shared "handle" for the given scope name.
     * @param sid Scope the meta belongs to
     * @param view The view of the metadata to use in lookup
     */
    SingleThreadedRCPtr<VB::ScopeSharedMetaData> createOrReferenceMeta(
            ScopeID sid, const VB::ScopeSharedMetaDataView& view);

    /**
     * When vbuckets drop a scope (from new manifest of over DCP) or the
     * vbucket is deleted( VB::Manifest destructs) the vbucket must dereference
     * all names it previously retrieved from createOrReferenceMeta
     *
     * @param sid Scope the name belongs to
     * @param meta Reference to the meta that was originally given out by
     *        createOrReferenceMeta.
     */
    void dereferenceMeta(ScopeID sid,
                         SingleThreadedRCPtr<VB::ScopeSharedMetaData>&& meta);

    /**
     * Perform the gathering of collection stats for the bucket.
     */
    static cb::EngineErrorGetCollectionIDResult doCollectionStats(
            KVBucket& bucket,
            const BucketStatCollector& collector,
            const std::string& statKey);

    /**
     * Perform the gathering of scope stats for the bucket.
     */
    static cb::EngineErrorGetScopeIDResult doScopeStats(
            KVBucket& bucket,
            const BucketStatCollector& collector,
            const std::string& statKey);

    static cb::engine_errc doPrometheusCollectionStats(
            KVBucket& bucket, const BucketStatCollector& collector);

private:
    /**
     * Apply newManifest to all active vbuckets
     * @return uninitialized if success, else the vbid which triggered failure.
     */
    std::optional<Vbid> updateAllVBuckets(KVBucket& bucket,
                                          const Manifest& newManifest);

    /**
     * This method handles the IO complete path and allows ::update to
     * correctly call applyNewManifest
     */
    cb::engine_error updateFromIOComplete(KVBucket& bucket,
                                          std::unique_ptr<Manifest> newManifest,
                                          const CookieIface* cookie);

    /**
     * Final stage of the manifest update is to roll the new manifest out to
     * the active vbuckets.
     *
     * @param bucket The bucket to work on
     * @param current The locked, current manifest (which will be replaced)
     * @param newManifest The new manifest to apply
     */
    cb::engine_error applyNewManifest(
            KVBucket& bucket,
            folly::Synchronized<Manifest>::UpgradeLockedPtr& current,
            std::unique_ptr<Manifest> newManifest);

    /**
     * Get a copy of stats which are relevant at a per-collection level.
     * The copied stats can then be used to format stats for one or more
     * collections (e.g., when aggregating over a scope) without repeatedly
     * aggregating over CoreStores/vBuckets.
     *
     * The stats collected here are either tracked per-collection, or are
     * tracked per-collection per-vbucket but can be meaningfully aggregated
     * across vbuckets. e.g., high seqnos are not meaningful outside the context
     * of the vbucket, but memory usage can easily be summed.
     * @param bucket bucket to collect stats for
     * @return copy of the stats to use to format stats for a request
     */
    static CachedStats getPerCollectionStats(KVBucket& bucket);

    /**
     * Get a copy of the stats for the given collections. This requires
     * vbucket visiting to accumulate various counters. The returned CachedStats
     * object stores the accumulated values for active vbuckets for all of the
     * collections specified.
     *
     * @param collections Collection entries to to collect stats for
     * @param bucket bucket to collect stats for
     * @return copy of the stats to use to format stats for a request
     */
    static CachedStats getPerCollectionStats(
            const std::vector<CollectionEntry>& collections, KVBucket& bucket);

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

    // handler for "collection-details"
    static cb::EngineErrorGetCollectionIDResult doCollectionDetailStats(
            KVBucket& bucket,
            const BucketStatCollector& collector,
            std::optional<std::string> arg);

    // handler for "collections"
    static cb::EngineErrorGetCollectionIDResult doAllCollectionsStats(
            KVBucket& bucket, const BucketStatCollector& collector);

    // handler for "collections name" or "collections byid id"
    static cb::EngineErrorGetCollectionIDResult doOneCollectionStats(
            KVBucket& bucket,
            const BucketStatCollector& collector,
            const std::string& arg,
            const std::string& statKey);

    // handler for "scope-details"
    static cb::EngineErrorGetScopeIDResult doScopeDetailStats(
            KVBucket& bucket,
            const BucketStatCollector& collector,
            std::optional<std::string> arg);

    // handler for "scopes"
    static cb::EngineErrorGetScopeIDResult doAllScopesStats(
            KVBucket& bucket, const BucketStatCollector& collector);

    // handler for "scopes name" or "scopes byid id"
    static cb::EngineErrorGetScopeIDResult doOneScopeStats(
            KVBucket& bucket,
            const BucketStatCollector& collector,
            const std::string& arg,
            const std::string& statKey);

    friend std::ostream& operator<<(std::ostream& os, const Manager& manager);

    /**
     * All of the currently known collection names for a CollectionID, which
     * vbuckets refer back to.
     * This is Synchronized because multiple connections/threads can be updating
     * the data.
     */
    using CollectionsSharedMetaDataTable =
            SharedMetaDataTable<CollectionID,
                                const VB::CollectionSharedMetaData>;
    folly::Synchronized<CollectionsSharedMetaDataTable> collectionSMT;

    /**
     * All of the currently known collection names for a ScopeID, which
     * vbuckets refer back to.
     * This is Synchronized because multiple connections/threads can be updating
     * the data.
     */
    using ScopesSharedMetaDataTable =
            SharedMetaDataTable<ScopeID, VB::ScopeSharedMetaData>;
    folly::Synchronized<ScopesSharedMetaDataTable> scopeSMT;

    /// Store the most recent (current) manifest received - this default
    /// constructs as the 'epoch' Manifest
    folly::Synchronized<Manifest> currentManifest;

    /// Serialise updates to the manifest (set_collections core)
    folly::Synchronized<const CookieIface*> updateInProgress{nullptr};
};

std::ostream& operator<<(std::ostream& os, const Manager& manager);
} // namespace Collections
