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

#include "collections/manager.h"
#include "bucket_logger.h"
#include "collections/flush.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "string_utils.h"
#include "vb_visitors.h"
#include "vbucket.h"

#include <folly/container/F14Map.h>
#include <memcached/collections.h>
#include <nlohmann/json.hpp>
#include <serverless/config.h>
#include <spdlog/fmt/ostr.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>
#include <optional>
#include <utility>

Collections::Manager::Manager() = default;

cb::engine_error Collections::Manager::update(
        const VBucketStateRLockMap& vbStateLocks,
        KVBucket& bucket,
        std::string_view manifestString) {
    // Construct a new Manifest (ctor will throw if JSON was illegal)
    Manifest newManifest;
    try {
        newManifest = Manifest(manifestString);
    } catch (std::exception& e) {
        EP_LOG_WARN(
                "Collections::Manager::update can't construct manifest "
                "e.what:{}",
                e.what());
        return {cb::engine_errc::invalid_arguments,
                "Collections::Manager::update manifest json invalid:" +
                        std::string(manifestString)};
    }

    // Next compare with current
    // First get an upgrade lock (which is initially read)
    // upgrade from read to write locking and do the update
    auto current = currentManifest.ulock();
    auto isSuccessorResult = current->isSuccessor(newManifest);
    if (isSuccessorResult.code() != cb::engine_errc::success) {
        return isSuccessorResult;
    }

    return applyNewManifest(
            std::move(vbStateLocks), bucket, current, std::move(newManifest));
}

cb::engine_error Collections::Manager::updateFromIOComplete(
        const VBucketStateRLockMap& vbStateLocks,
        KVBucket& bucket,
        Manifest&& newManifest,
        CookieIface* cookie) {
    auto current = currentManifest.ulock(); // Will update to newManifest
    return applyNewManifest(
            std::move(vbStateLocks), bucket, current, std::move(newManifest));
}

// common to ephemeral/persistent, this does the update
cb::engine_error Collections::Manager::applyNewManifest(
        const VBucketStateRLockMap& vbStateLocks,
        KVBucket& bucket,
        folly::Synchronized<Manifest>::UpgradeLockedPtr& current,
        Manifest&& newManifest) {
    auto updated =
            updateAllVBuckets(std::move(vbStateLocks), bucket, newManifest);
    if (updated.has_value()) {
        return {cb::engine_errc::cannot_apply_collections_manifest,
                "Collections::Manager::update aborted on " +
                        updated->to_string() + ", cannot apply to vbuckets"};
    }

    // Now switch to write locking and change the manifest. The lock is
    // released after this statement.
    *current.moveFromUpgradeToWrite() = std::move(newManifest);
    return {cb::engine_errc::success,
            "Collections::Manager::update applied new manifest"};
}

std::optional<Vbid> Collections::Manager::updateAllVBuckets(
        const VBucketStateRLockMap& vbStateLocks,
        KVBucket& bucket,
        const Manifest& newManifest) {
    for (Vbid::id_type i = 0; i < bucket.getVBuckets().getSize(); i++) {
        auto vb = bucket.getVBuckets().getBucket(Vbid(i));
        auto vbStateLockIt = vbStateLocks.find(Vbid(i));

        // We should have a vbstatelock iff the corresponding vbucket is present
        bool hasVbStateLock = vbStateLockIt != vbStateLocks.end();
        if (bool(vb) != hasVbStateLock) {
            throw std::logic_error(
                    fmt::format("updateAllVBuckets(): {} is {}present, but the "
                                "corresponding lock is{}",
                                Vbid(i),
                                vb ? "" : "not ",
                                hasVbStateLock ? "" : " not"));
        }

        // We took a lock on the vbsetMutex (all vBucket states) to guard state
        // changes here) in KVBucket::setCollections.
        if (vb && vb->getState() == vbucket_state_active) {
            bool abort = false;
            auto status =
                    vb->updateFromManifest(vbStateLockIt->second, newManifest);
            using namespace Collections;
            switch (status) {
            case VB::ManifestUpdateStatus::EqualUidWithDifferences:
            case VB::ManifestUpdateStatus::ImmutablePropertyModified:

                // This error is unexpected and the best action is not to
                // continue applying it
                abort = true;
                [[fallthrough]];
            case VB::ManifestUpdateStatus::Behind:
                // Applying a manifest which is 'behind' the vbucket is
                // expected (certainly for newly promoted replica), however
                // still log it for now.
                EP_LOG_WARN(
                        "Collections::Manager::updateAllVBuckets: error:{} {}",
                        to_string(status),
                        vb->getId());
            case VB::ManifestUpdateStatus::Success:
                break;
            }
            if (abort) {
                return vb->getId();
            }
        }
    }
    return {};
}

std::pair<cb::mcbp::Status, nlohmann::json> Collections::Manager::getManifest(
        const Collections::IsVisibleFunction& isVisible) const {
    return {cb::mcbp::Status::Success,
            currentManifest.rlock()->to_json(isVisible)};
}

bool Collections::Manager::validateGetCollectionIDPath(std::string_view path) {
    return std::ranges::count(path, '.') == 1;
}

bool Collections::Manager::validateGetScopeIDPath(std::string_view path) {
    return std::ranges::count(path, '.') <= 1;
}

cb::EngineErrorGetCollectionIDResult Collections::Manager::getCollectionID(
        std::string_view path) const {
    if (!validateGetCollectionIDPath(path)) {
        return cb::EngineErrorGetCollectionIDResult{
                cb::engine_errc::invalid_arguments};
    }

    auto current = currentManifest.rlock();
    auto scope = current->getScopeID(path);
    if (!scope) {
        return {cb::engine_errc::unknown_scope, current->getUid()};
    }

    auto collection = current->getCollectionID(scope.value(), path);
    if (!collection) {
        return {cb::engine_errc::unknown_collection, current->getUid()};
    }
    // From scope.collection move to collection
    path.remove_prefix(path.find_first_of(".") + 1);
    return {current->getUid(),
            scope.value(),
            collection.value(),
            isSystemCollection(path, collection.value())};
}

cb::EngineErrorGetScopeIDResult Collections::Manager::getScopeID(
        std::string_view path) const {
    if (!validateGetScopeIDPath(path)) {
        return cb::EngineErrorGetScopeIDResult{
                cb::engine_errc::invalid_arguments};
    }
    auto current = currentManifest.rlock();
    auto scope = current->getScopeID(path);
    if (!scope) {
        return cb::EngineErrorGetScopeIDResult{current->getUid()};
    }

    return {current->getUid(), *scope, isSystemScope(path, *scope)};
}

std::pair<uint64_t, std::optional<ScopeID>> Collections::Manager::getScopeID(
        CollectionID cid) const {
    // 'shortcut' For the default collection, just return the default scope.
    // If the default collection was deleted the vbucket will have the final say
    // but for this interface allow this without taking the rlock.
    if (cid.isDefaultCollection()) {
        // Allow the default collection in the default scope...
        return std::make_pair<uint64_t, std::optional<ScopeID>>(
                0, ScopeID{ScopeID::Default});
    }

    auto current = currentManifest.rlock();
    return std::make_pair<uint64_t, std::optional<ScopeID>>(
            current->getUid(), current->getScopeID(cid));
}

std::pair<uint64_t, std::optional<Collections::CollectionMetaData>>
Collections::Manager::getCollectionEntry(CollectionID cid) const {
    // 'shortcut' For the default collection, just return the default scope.
    // If the default collection was deleted the vbucket will have the final say
    // but for this interface allow this without taking the rlock.
    // Note: That this can only work safely for !serverless where we don't care
    // for the metering value. When serverless, lookup from the manifest and
    // read the current metering state.
    if (cid.isDefaultCollection() && !cb::serverless::isEnabled()) {
        // Allow the default collection in the default scope...
        return std::make_pair<uint64_t,
                              std::optional<Collections::CollectionMetaData>>(
                0, Collections::CollectionMetaData{});
    }

    auto current = currentManifest.rlock();
    return std::make_pair<uint64_t,
                          std::optional<Collections::CollectionMetaData>>(
            current->getUid(), current->getCollectionEntry(cid));
}

cb::EngineErrorGetScopeIDResult Collections::Manager::isScopeIDValid(
        ScopeID sid) const {
    auto manifestLocked = currentManifest.rlock();
    const auto itr = manifestLocked->findScope(sid);
    if (itr != manifestLocked->endScopes()) {
        return cb::EngineErrorGetScopeIDResult{
                manifestLocked->getUid(),
                sid,
                isSystemScope(itr->second.name, sid)};
    }
    // Returns unknown_scope + manifestUid
    return cb::EngineErrorGetScopeIDResult{manifestLocked->getUid()};
}

bool Collections::Manager::needsUpdating(const VBucket& vb) const {
    // If the currentUid is ahead or equal, requires an update
    return currentManifest.rlock()->getUid() >
           vb.getManifest().lock().getManifestUid();
}

void Collections::Manager::maybeUpdate(VBucketStateLockRef vbStateLock,
                                       VBucket& vb) const {
    // Lock manager updates, errors are logged by VB::Manifest
    currentManifest.withRLock([&vb, vbStateLock](const auto& manifest) {
        vb.updateFromManifest(vbStateLock, manifest);
    });
}

// This method is really to aid development and allow the dumping of the VB
// collection data to the logs.
void Collections::Manager::logAll(KVBucket& bucket) const {
    EP_LOG_INFO("{}", *this);
    for (Vbid::id_type i = 0; i < bucket.getVBuckets().getSize(); i++) {
        Vbid vbid = Vbid(i);
        auto vb = bucket.getVBuckets().getBucket(vbid);
        if (vb) {
            EP_LOG_INFO("{}: {} {}",
                        vbid,
                        VBucket::toString(vb->getState()),
                        vb->lockCollections());
        }
    }
}

void Collections::Manager::addCollectionStats(
        KVBucket& bucket, const BucketStatCollector& collector) const {
    currentManifest.rlock()->addCollectionStats(bucket, collector);
}

void Collections::Manager::addScopeStats(
        KVBucket& bucket, const BucketStatCollector& collector) const {
    currentManifest.rlock()->addScopeStats(bucket, collector);
}

void Collections::Manager::setInitialCollectionManifest(
        const nlohmann::json& payload) {
    Manifest manifest(payload.dump());
    EP_LOG_INFO(
            "Collections::Manager::setInitialCollectionManifest: starting at "
            "uid:{:#x}",
            manifest.getUid());
    *currentManifest.wlock() = std::move(manifest);
}

/**
 * Perform actions for a completed warmup - currently check if any
 * collections are 'deleting' and require erase re-triggering.
 */
void Collections::Manager::warmupCompleted(EPBucket& bucket) const {
    for (Vbid::id_type i = 0; i < bucket.getVBuckets().getSize(); i++) {
        Vbid vbid = Vbid(i);
        auto vb = bucket.getVBuckets().getBucket(vbid);
        if (vb) {
            if (vb->getManifest().isDropInProgress()) {
                Collections::VB::Flush::triggerPurge(vbid, bucket);
            }
        }
    }
}

SingleThreadedRCPtr<Collections::VB::CollectionSharedMetaData>
Collections::Manager::createOrReferenceMeta(
        CollectionID cid,
        const Collections::VB::CollectionSharedMetaDataView& view) {
    return collectionSMT.wlock()->createOrReference(cid, view);
}

void Collections::Manager::dereferenceMeta(
        CollectionID cid,
        SingleThreadedRCPtr<VB::CollectionSharedMetaData>&& meta) {
    collectionSMT.wlock()->dereference(cid, std::move(meta));
}

Collections::OperationCounts Collections::Manager::getOperationCounts(
        CollectionID cid) const {
    OperationCounts counts;
    collectionSMT.rlock()->forEach(cid, [&counts](const auto& entry) {
        counts += entry->getOperationCounts();
    });
    return counts;
}

SingleThreadedRCPtr<Collections::VB::ScopeSharedMetaData>
Collections::Manager::createOrReferenceMeta(
        ScopeID sid, const Collections::VB::ScopeSharedMetaDataView& view) {
    return scopeSMT.wlock()->createOrReference(sid, view);
}

void Collections::Manager::dereferenceMeta(
        ScopeID sid, SingleThreadedRCPtr<VB::ScopeSharedMetaData>&& meta) {
    scopeSMT.wlock()->dereference(sid, std::move(meta));
}

/// VbucketVisitor that gathers stats for all collections
class AllCollectionsGetStatsVBucketVisitor : public VBucketVisitor {
public:
    void visitBucket(VBucket& vb) override {
        if (vb.getState() == vbucket_state_active) {
            vb.lockCollections().updateSummary(summary);
        }
    }
    Collections::Summary summary;
};

/// VbucketVisitor that gathers stats for the given collections
class CollectionsGetStatsVBucketVisitor : public VBucketVisitor {
public:
    explicit CollectionsGetStatsVBucketVisitor(
            const std::vector<Collections::CollectionMetaData>& collections)
        : collections(collections) {
        for (const auto& entry : collections) {
            summary.emplace(entry.cid, Collections::AccumulatedStats{});
        }
    }

    void visitBucket(VBucket& vb) override {
        if (vb.getState() == vbucket_state_active) {
            vb.lockCollections().accumulateStats(collections, summary);
        }
    }

    const std::vector<Collections::CollectionMetaData>& collections;
    Collections::Summary summary;
};

class CollectionDetailedVBucketVisitor : public VBucketVisitor {
public:
    CollectionDetailedVBucketVisitor(const BucketStatCollector& collector)
        : collector(collector) {
    }

    void visitBucket(VBucket& vb) override {
        success = vb.lockCollections().addCollectionStats(vb.getId(),
                                                          collector) ||
                  success;
    }

    bool getSuccess() const {
        return success;
    }

private:
    const BucketStatCollector& collector;
    bool success = true;
};

class ScopeDetailedVBucketVisitor : public VBucketVisitor {
public:
    ScopeDetailedVBucketVisitor(const BucketStatCollector& collector)
        : collector(collector) {
    }

    void visitBucket(VBucket& vb) override {
        success = vb.lockCollections().addScopeStats(vb.getId(), collector) ||
                  success;
    }

    bool getSuccess() const {
        return success;
    }

private:
    const BucketStatCollector& collector;
    bool success = true;
};

// collections-details
//   - return top level stats (manager/manifest)
//   - iterate vbuckets returning detailed VB stats
// collections-details n
//   - return detailed VB stats for n only
// collections
//   - return top level stats (manager/manifest)
//   - return per collection item counts from all active VBs
cb::EngineErrorGetCollectionIDResult Collections::Manager::doCollectionStats(
        KVBucket& bucket,
        const BucketStatCollector& collector,
        const std::string& statKey) {
    std::optional<std::string> arg;
    if (auto pos = statKey.find_first_of(' '); pos != std::string::npos) {
        arg = statKey.substr(pos + 1);
    }

    if (statKey.starts_with("collections-details")) {
        return doCollectionDetailStats(bucket, collector, arg);
    }

    if (!arg) {
        return doAllCollectionsStats(bucket, collector);
    }
    return doOneCollectionStats(bucket, collector, arg.value(), statKey);
}

// handle key "collections-details"
cb::EngineErrorGetCollectionIDResult
Collections::Manager::doCollectionDetailStats(
        KVBucket& bucket,
        const BucketStatCollector& collector,
        std::optional<std::string> arg) {
    bool success = false;
    if (arg) {
        // VB may be encoded in statKey
        uint16_t id;
        try {
            id = std::stoi(*arg);
        } catch (const std::logic_error& e) {
            EP_LOG_WARN(
                    "Collections::Manager::doCollectionDetailStats invalid "
                    "vbid:{}, exception:{}",
                    *arg,
                    e.what());
            return cb::EngineErrorGetCollectionIDResult{
                    cb::engine_errc::invalid_arguments};
        }

        Vbid vbid = Vbid(id);
        VBucketPtr vb = bucket.getVBucket(vbid);
        if (!vb) {
            return cb::EngineErrorGetCollectionIDResult{
                    cb::engine_errc::not_my_vbucket};
        }

        success = vb->lockCollections().addCollectionStats(vbid, collector);

    } else {
        bucket.getCollectionsManager().addCollectionStats(bucket, collector);
        CollectionDetailedVBucketVisitor visitor(collector);
        bucket.visit(visitor);
        success = visitor.getSuccess();
    }
    return {success ? cb::engine_errc::success : cb::engine_errc::failed,
            cb::EngineErrorGetCollectionIDResult::allowSuccess{}};
}

// handle key "collections"
cb::EngineErrorGetCollectionIDResult
Collections::Manager::doAllCollectionsStats(
        KVBucket& bucket, const BucketStatCollector& collector) {
    // no collection ID was provided

    // Do the high level stats (includes global count)
    bucket.getCollectionsManager().addCollectionStats(bucket, collector);
    auto cachedStats = getPerCollectionStats(bucket);
    auto current = bucket.getCollectionsManager().currentManifest.rlock();
    // do stats for every collection
    for (const auto& entry : *current) {
        // Access check for SimpleStats + the optional system privilege.
        // Use testPrivilege as it won't log
        if (collector.testPrivilegeForStat(
                    getRequiredSystemPrivilege(entry.second),
                    entry.second.sid,
                    entry.first) != cb::engine_errc::success) {
            continue; // skip this collection
        }

        const auto scopeItr = current->findScope(entry.second.sid);
        Expects(scopeItr != current->endScopes());
        cachedStats.addStatsForCollection(
                bucket, scopeItr->second.name, entry.second, collector);
    }
    return {cb::engine_errc::success,
            cb::EngineErrorGetCollectionIDResult::allowSuccess{}};
}

// handle key "collections <path>" or "collections-byid"
cb::EngineErrorGetCollectionIDResult Collections::Manager::doOneCollectionStats(
        KVBucket& bucket,
        const BucketStatCollector& collector,
        const std::string& arg,
        const std::string& statKey) {
    cb::EngineErrorGetCollectionIDResult res{cb::engine_errc::failed};
    // An argument was provided, maybe an id or a 'path'
    if (statKey.starts_with("collections-byid")) {
        CollectionID cid;
        // provided argument should be a hex collection ID N, 0xN or 0XN
        try {
            cid = std::stoi(arg, nullptr, 16);
        } catch (const std::logic_error& e) {
            EP_LOG_WARN(
                    "Collections::Manager::doOneCollectionStats invalid "
                    "collection arg:{}, exception:{}",
                    arg,
                    e.what());
            return cb::EngineErrorGetCollectionIDResult{
                    cb::engine_errc::invalid_arguments};
        }
        // Collection's scope and system state are needed for privilege check
        auto [manifestUid, meta] =
                bucket.getCollectionsManager().getCollectionEntry(cid);
        if (meta) {
            res = {manifestUid,
                   meta->sid,
                   cid,
                   Collections::isSystemCollection(meta->name, cid)};
        } else {
            return {cb::engine_errc::unknown_collection, manifestUid};
        }
    } else {
        // provided argument should be a collection path
        res = bucket.getCollectionsManager().getCollectionID(arg);
        if (res.result != cb::engine_errc::success) {
            EP_LOG_WARN(
                    "Collections::Manager::doOneCollectionStats could not "
                    "find "
                    "collection arg:{} error:{}",
                    arg,
                    res.result);
            return res;
        }
    }

    // Access check for SimpleStats + if required SystemCollectionLookup
    std::optional<cb::rbac::Privilege> extraPriv;
    if (res.isSystemCollection) {
        extraPriv = cb::rbac::Privilege::SystemCollectionLookup;
    }
    res.result = collector.testPrivilegeForStat(
            extraPriv, res.getScopeId(), res.getCollectionId());
    if (res.result != cb::engine_errc::success) {
        return res;
    }

    // Take a copy of the two items needed from the manifest and then release
    // the lock before vbucket visiting.
    std::string scopeName;
    Collections::CollectionMetaData entry;
    {
        auto current = bucket.getCollectionsManager().currentManifest.rlock();
        auto collectionItr = current->findCollection(res.getCollectionId());

        if (collectionItr == current->end()) {
            EP_LOG_WARN(
                    "Collections::Manager::doOneCollectionStats unknown "
                    "collection arg:{} cid:{}",
                    arg,
                    res.getCollectionId().to_string());
            return {cb::engine_errc::unknown_collection, current->getUid()};
        }
        auto scopeItr = current->findScope(collectionItr->second.sid);
        Expects(scopeItr != current->endScopes());
        scopeName = scopeItr->second.name;
        entry = collectionItr->second;
    }

    // Visit the vbuckets and generate the stat payload
    auto cachedStats = getPerCollectionStats({entry}, bucket);
    cachedStats.addStatsForCollection(bucket, scopeName, entry, collector);
    return res;
}

// scopes-details
//   - return top level stats (manager/manifest)
//   - iterate vbucket returning detailed VB stats
// scopes-details n
//   - return detailed VB stats for n only
// scopes
//   - return top level stats (manager/manifest)
//   - return number of collections from all active VBs
cb::EngineErrorGetScopeIDResult Collections::Manager::doScopeStats(
        KVBucket& bucket,
        const BucketStatCollector& collector,
        const std::string& statKey) {
    std::optional<std::string> arg;
    if (auto pos = statKey.find_first_of(' '); pos != std::string_view::npos) {
        arg = statKey.substr(pos + 1);
    }
    if (statKey.starts_with("scopes-details")) {
        return doScopeDetailStats(bucket, collector, arg);
    }

    if (!arg) {
        return doAllScopesStats(bucket, collector);
    }

    return doOneScopeStats(bucket, collector, arg.value(), statKey);
}

// handler for "scope-details"
cb::EngineErrorGetScopeIDResult Collections::Manager::doScopeDetailStats(
        KVBucket& bucket,
        const BucketStatCollector& collector,
        std::optional<std::string> arg) {
    bool success = true;
    if (arg) {
        // VB may be encoded in statKey
        uint16_t id;
        try {
            id = std::stoi(*arg);
        } catch (const std::logic_error& e) {
            EP_LOG_WARN(
                    "Collections::Manager::doScopeDetailStats invalid "
                    "vbid:{}, exception:{}",
                    *arg,
                    e.what());
            return cb::EngineErrorGetScopeIDResult{
                    cb::engine_errc::invalid_arguments};
        }

        Vbid vbid = Vbid(id);
        VBucketPtr vb = bucket.getVBucket(vbid);
        if (!vb) {
            return cb::EngineErrorGetScopeIDResult{
                    cb::engine_errc::not_my_vbucket};
        }
        success = vb->lockCollections().addScopeStats(vbid, collector);
    } else {
        bucket.getCollectionsManager().addScopeStats(bucket, collector);
        ScopeDetailedVBucketVisitor visitor(collector);
        bucket.visit(visitor);
        success = visitor.getSuccess();
    }
    return {success ? cb::engine_errc::success : cb::engine_errc::failed,
            cb::EngineErrorGetScopeIDResult::allowSuccess{}};
}

// handler for "scopes"
cb::EngineErrorGetScopeIDResult Collections::Manager::doAllScopesStats(
        KVBucket& bucket, const BucketStatCollector& collector) {
    auto cachedStats = getPerCollectionStats(bucket);

    // Do the high level stats (includes number of collections)
    bucket.getCollectionsManager().addScopeStats(bucket, collector);
    auto current = bucket.getCollectionsManager().currentManifest.rlock();
    for (auto itr = current->beginScopes(); itr != current->endScopes();
         ++itr) {
        // Access check for SimpleStats. Use testPrivilege as it won't log
        if (collector.testPrivilegeForStat(
                    getRequiredSystemPrivilege(itr->first, itr->second),
                    itr->first,
                    {}) != cb::engine_errc::success) {
            continue; // skip this scope
        }

        cachedStats.addStatsForScope(bucket,
                                     itr->first,
                                     itr->second.name,
                                     itr->second.collections,
                                     collector);
    }
    return {cb::engine_errc::success,
            cb::EngineErrorGetScopeIDResult::allowSuccess{}};
}

// handler for "scopes name" or "scopes byid id"
cb::EngineErrorGetScopeIDResult Collections::Manager::doOneScopeStats(
        KVBucket& bucket,
        const BucketStatCollector& collector,
        const std::string& arg,
        const std::string& statKey) {
    cb::EngineErrorGetScopeIDResult res{cb::engine_errc::failed};
    if (statKey.starts_with("scopes-byid")) {
        ScopeID scopeID;
        // provided argument should be a hex scope ID N, 0xN or 0XN
        try {
            scopeID = std::stoi(arg, nullptr, 16);
        } catch (const std::logic_error& e) {
            EP_LOG_WARN(
                    "Collections::Manager::doOneScopeStats invalid "
                    "scope arg:{}, exception:{}",
                    arg,
                    e.what());
            return cb::EngineErrorGetScopeIDResult{
                    cb::engine_errc::invalid_arguments};
        }
        res = bucket.getCollectionsManager().isScopeIDValid(scopeID);
    } else {
        // provided argument should be a scope name
        res = bucket.getCollectionsManager().getScopeID(arg);
    }

    if (res.result != cb::engine_errc::success) {
        return res;
    }

    // Access check for SimpleStats and maybe SystemCollectionLookup
    std::optional<cb::rbac::Privilege> extraPriv;
    if (res.isSystemScope()) {
        extraPriv = cb::rbac::Privilege::SystemCollectionLookup;
    }
    res.result =
            collector.testPrivilegeForStat(extraPriv, res.getScopeId(), {});
    if (res.result != cb::engine_errc::success) {
        return res;
    }

    // Take a copy of the two items needed from the manifest and then release
    // the lock before vbucket visiting.
    std::string scopeName;
    std::vector<Collections::CollectionMetaData> scopeCollections;
    {
        auto current = bucket.getCollectionsManager().currentManifest.rlock();
        auto scopeItr = current->findScope(res.getScopeId());

        if (scopeItr == current->endScopes()) {
            EP_LOG_WARN(
                    "Collections::Manager::doOneScopeStats unknown "
                    "scope arg:{} sid:{}",
                    arg,
                    res.getScopeId().to_string());
            return cb::EngineErrorGetScopeIDResult{current->getUid()};
        }

        scopeName = scopeItr->second.name;
        scopeCollections = scopeItr->second.collections;
    }
    auto cachedStats = getPerCollectionStats(scopeCollections, bucket);
    cachedStats.addStatsForScope(
            bucket, res.getScopeId(), scopeName, scopeCollections, collector);
    // add stats for each collection in the scope
    for (const auto& entry : scopeCollections) {
        cachedStats.addStatsForCollection(bucket, scopeName, entry, collector);
    }
    return res;
}

cb::engine_errc Collections::Manager::doPrometheusCollectionStats(
        KVBucket& bucket, const BucketStatCollector& collector) {
    return doAllCollectionsStats(bucket, collector).result;
}

void Collections::Manager::dump() const {
    std::cerr << *this;
}

std::ostream& Collections::operator<<(std::ostream& os,
                                      const Collections::Manager& manager) {
    os << "Collections::Manager current:" << *manager.currentManifest.rlock()
       << "\n";
    os << "collectionMeta:\n" << *manager.collectionSMT.rlock() << "\n";
    os << "scopeMeta:\n" << *manager.scopeSMT.rlock() << "\n";
    return os;
}

Collections::CachedStats Collections::Manager::getPerCollectionStats(
        KVBucket& bucket) {
    auto memUsed = bucket.getEPEngine().getEpStats().getAllCollectionsMemUsed();

    AllCollectionsGetStatsVBucketVisitor visitor;
    bucket.visit(visitor);

    return {std::move(memUsed),
            std::move(visitor.summary) /* accumulated collection stats */};
}

Collections::CachedStats Collections::Manager::getPerCollectionStats(
        const std::vector<Collections::CollectionMetaData>& collections,
        KVBucket& bucket) {
    // Gather per-vbucket stats for the collections of interest
    CollectionsGetStatsVBucketVisitor visitor{collections};
    bucket.visit(visitor);

    // And the mem_used which is stored in EpStats
    std::unordered_map<CollectionID, size_t> memUsed;
    for (const auto& entry : collections) {
        memUsed.emplace(entry.cid,
                        bucket.getEPEngine().getEpStats().getCollectionMemUsed(
                                entry.cid));
    }
    return {std::move(memUsed),
            std::move(visitor.summary) /* accumulated collection stats */};
}

std::optional<cb::rbac::Privilege>
Collections::Manager::getRequiredSystemPrivilege(
        const CollectionMetaData& meta) {
    if (Collections::getCollectionVisibility(meta.name, meta.cid) ==
        Collections::Visibility::System) {
        return cb::rbac::Privilege::SystemCollectionLookup;
    }
    return std::nullopt;
}

std::optional<cb::rbac::Privilege>
Collections::Manager::getRequiredSystemPrivilege(ScopeID sid,
                                                 const Scope& scope) {
    if (Collections::getScopeVisibility(scope.name, sid) ==
        Collections::Visibility::System) {
        return cb::rbac::Privilege::SystemCollectionLookup;
    }
    return std::nullopt;
}

Collections::CachedStats::CachedStats(
        std::unordered_map<CollectionID, size_t>&& colMemUsed,
        std::unordered_map<CollectionID, AccumulatedStats>&& accumulatedStats)
    : colMemUsed(std::move(colMemUsed)),
      accumulatedStats(std::move(accumulatedStats)) {
}

void Collections::CachedStats::addStatsForCollection(
        const KVBucket& bucket,
        std::string_view scopeName,
        const CollectionMetaData& collection,
        const BucketStatCollector& collector) {
    auto collectionC = collector.forScope(scopeName, collection.sid)
                               .forCollection(collection.name, collection.cid);

    addAggregatedCollectionStats(bucket, {collection.cid}, collectionC);

    using namespace cb::stats;
    collectionC.addStat(Key::collection_name, collection.name);
    collectionC.addStat(Key::collection_scope_name, scopeName);

    // add ttl if valid
    if (collection.maxTtl.has_value()) {
        collectionC.addStat(Key::collection_maxTTL,
                            collection.maxTtl.value().count());
    }
}

void Collections::CachedStats::addStatsForScope(
        const KVBucket& bucket,
        ScopeID sid,
        std::string_view scopeName,
        const std::vector<Collections::CollectionMetaData>& scopeCollections,
        const BucketStatCollector& collector) {
    auto scopeC = collector.forScope(scopeName, sid);
    std::vector<CollectionID> collections;
    collections.reserve(scopeCollections.size());

    // get the CollectionIDs - extract the keys from the map
    for (const auto& entry : scopeCollections) {
        collections.push_back(entry.cid);
    }
    addAggregatedCollectionStats(bucket, collections, scopeC);

    using namespace cb::stats;
    // add scope name
    scopeC.addStat(Key::scope_name, scopeName);
    // add scope collection count
    scopeC.addStat(Key::scope_collection_count, scopeCollections.size());
}

void Collections::CachedStats::addAggregatedCollectionStats(
        const KVBucket& bucket,
        const std::vector<CollectionID>& cids,
        const StatCollector& collector) {
    size_t memUsed = 0;
    AccumulatedStats stats;
    OperationCounts operationCounts;

    for (const auto& cid : cids) {
        memUsed += colMemUsed[cid];
        stats += accumulatedStats[cid];

        // Now read the already bucket level operation stats
        operationCounts +=
                bucket.getCollectionsManager().getOperationCounts(cid);
    }

    using namespace cb::stats;

    collector.addStat(Key::collection_mem_used, memUsed);
    collector.addStat(Key::collection_item_count, stats.itemCount);
    collector.addStat(Key::collection_data_size, stats.diskSize);

    collector.addStat(Key::collection_ops_store, operationCounts.opsStore);
    collector.addStat(Key::collection_ops_delete, operationCounts.opsDelete);
    collector.addStat(Key::collection_ops_get, operationCounts.opsGet);
}
