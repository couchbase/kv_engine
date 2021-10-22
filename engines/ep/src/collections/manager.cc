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
#include "collections/persist_manifest_task.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "string_utils.h"
#include "vb_visitors.h"

#include <memcached/collections.h>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/ostr.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>
#include <optional>
#include <utility>

Collections::Manager::Manager() {
}

cb::engine_error Collections::Manager::update(KVBucket& bucket,
                                              std::string_view manifestString,
                                              const CookieIface* cookie) {
    auto lockedUpdateCookie = updateInProgress.wlock();
    if (*lockedUpdateCookie != nullptr && *lockedUpdateCookie != cookie) {
        // log this as it's very unexpected, only ever 1 manager
        return cb::engine_error(
                cb::engine_errc::too_busy,
                "An update is already in-progress for another cookie:" +
                        std::to_string(uintptr_t(*lockedUpdateCookie)));
    }

    // Now getEngineSpecific - if that is null this is a new command, else
    // it's the IO complete command
    void* manifest = bucket.getEPEngine().getEngineSpecific(cookie);

    if (manifest) {
        // I/O complete path?
        if (!*lockedUpdateCookie) {
            // This can occur for a DCP connection, cookie is 'reserved'.
            EP_LOG_WARN(
                    "Collections::Manager::update aborted as we have found a "
                    "manifest:{} but updateInProgress:{}",
                    manifest,
                    static_cast<const void*>(*lockedUpdateCookie));
            return cb::engine_error(cb::engine_errc::failed,
                                    "Collections::Manager::update failure");
        }

        // Final stage of update now happening, clear the cookie and engine
        // specific so the next update can start after this one returns.
        *lockedUpdateCookie = nullptr;
        bucket.getEPEngine().storeEngineSpecific(cookie, nullptr);

        // Take ownership back of the manifest so it destructs/frees on return
        std::unique_ptr<Manifest> newManifest(
                reinterpret_cast<Manifest*>(manifest));
        return updateFromIOComplete(bucket, std::move(newManifest), cookie);
    }

    // Construct a new Manifest (ctor will throw if JSON was illegal)
    std::unique_ptr<Manifest> newManifest;
    try {
        newManifest = std::make_unique<Manifest>(manifestString);
    } catch (std::exception& e) {
        EP_LOG_WARN(
                "Collections::Manager::update can't construct manifest "
                "e.what:{}",
                e.what());
        return cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Collections::Manager::update manifest json invalid:" +
                        std::string(manifestString));
    }

    // Next compare with current
    // First get an upgrade lock (which is initially read)
    // Persistence will schedule a task and drop the lock whereas ephemeral will
    // upgrade from read to write locking and do the update
    auto current = currentManifest.ulock();
    auto isSuccessorResult = current->isSuccessor(*newManifest);
    if (isSuccessorResult.code() != cb::engine_errc::success) {
        return isSuccessorResult;
    }

    // New manifest is a legal successor the update is going ahead.
    // Ephemeral bucket can update now, Persistent bucket on wake-up from
    // successful run of the PeristManifestTask.
    cb::engine_errc status = cb::engine_errc::success;
    if (!bucket.maybeScheduleManifestPersistence(cookie, newManifest)) {
        // Ephemeral case, apply immediately
        return applyNewManifest(bucket, current, std::move(newManifest));
    } else {
        *lockedUpdateCookie = cookie;
        status = cb::engine_errc::would_block;
    }

    return cb::engine_error(status,
                            "Collections::Manager::update part one complete");
}

cb::engine_error Collections::Manager::updateFromIOComplete(
        KVBucket& bucket,
        std::unique_ptr<Manifest> newManifest,
        const CookieIface* cookie) {
    auto current = currentManifest.ulock(); // Will update to newManifest
    return applyNewManifest(bucket, current, std::move(newManifest));
}

// common to ephemeral/persistent, this does the update
cb::engine_error Collections::Manager::applyNewManifest(
        KVBucket& bucket,
        folly::Synchronized<Manifest>::UpgradeLockedPtr& current,
        std::unique_ptr<Manifest> newManifest) {
    auto updated = updateAllVBuckets(bucket, *newManifest);
    if (updated.has_value()) {
        return cb::engine_error(
                cb::engine_errc::cannot_apply_collections_manifest,
                "Collections::Manager::update aborted on " +
                        updated->to_string() + ", cannot apply to vbuckets");
    }

    // Now switch to write locking and change the manifest. The lock is
    // released after this statement.
    *current.moveFromUpgradeToWrite() = std::move(*newManifest);
    return cb::engine_error(
            cb::engine_errc::success,
            "Collections::Manager::update applied new manifest");
}

std::optional<Vbid> Collections::Manager::updateAllVBuckets(
        KVBucket& bucket, const Manifest& newManifest) {
    for (Vbid::id_type i = 0; i < bucket.getVBuckets().getSize(); i++) {
        auto vb = bucket.getVBuckets().getBucket(Vbid(i));

        // We took a lock on the vbsetMutex (all vBucket states) to guard state
        // changes here) in KVBucket::setCollections.
        if (vb && vb->getState() == vbucket_state_active) {
            bool abort = false;
            auto status = vb->updateFromManifest(newManifest);
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

void Collections::Manager::updatePersistManifestTaskDone(
        EventuallyPersistentEngine& engine,
        const CookieIface* cookie,
        cb::engine_errc status) {
    // If !success the command will return to the caller, so must clean-up
    // ready for any further task.
    if (status != cb::engine_errc::success) {
        auto lockedUpdateCookie = updateInProgress.wlock();
        *lockedUpdateCookie = nullptr;
        engine.storeEngineSpecific(cookie, nullptr);
    }
}

std::pair<cb::mcbp::Status, nlohmann::json> Collections::Manager::getManifest(
        const Collections::IsVisibleFunction& isVisible) const {
    return {cb::mcbp::Status::Success,
            currentManifest.rlock()->toJson(isVisible)};
}

bool Collections::Manager::validateGetCollectionIDPath(std::string_view path) {
    return std::count(path.begin(), path.end(), '.') == 1;
}

bool Collections::Manager::validateGetScopeIDPath(std::string_view path) {
    return std::count(path.begin(), path.end(), '.') <= 1;
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

    return {current->getUid(), scope.value(), collection.value()};
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

    return {current->getUid(), scope.value()};
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

cb::EngineErrorGetScopeIDResult Collections::Manager::isScopeIDValid(
        ScopeID sid) const {
    auto manifestLocked = currentManifest.rlock();
    if (manifestLocked->findScope(sid) != manifestLocked->endScopes()) {
        return cb::EngineErrorGetScopeIDResult{manifestLocked->getUid(), sid};
    }
    return cb::EngineErrorGetScopeIDResult{manifestLocked->getUid()};
}

bool Collections::Manager::needsUpdating(const VBucket& vb) const {
    // If the currentUid is ahead or equal, requires an update
    return currentManifest.rlock()->getUid() >
           vb.getManifest().lock().getManifestUid();
}

void Collections::Manager::maybeUpdate(VBucket& vb) const {
    // Lock manager updates, errors are logged by VB::Manifest
    currentManifest.withRLock(
            [&vb](const auto& manifest) { vb.updateFromManifest(manifest); });
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

bool Collections::Manager::warmupLoadManifest(const std::string& dbpath) {
    auto rv = Collections::PersistManifestTask::tryAndLoad(dbpath);
    if (rv.has_value()) {
        EP_LOG_INFO(
                "Collections::Manager::warmupLoadManifest: starting at "
                "uid:{:#x}",
                rv.value().getUid());
        *currentManifest.wlock() = std::move(rv.value());
        return true;
    }
    // else tryAndLoad detected (and logged) some kind of corruption issue.
    // If this corruption occurred at the same time as some issue in the
    // forward flow of the Manifest, KV can't validate that any change to the
    // manifest is a legal successor (Manifest::isSuccessor) - return false
    // so Warmup can fail - holding the node::bucket pending.
    return false;
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

SingleThreadedRCPtr<const Collections::VB::CollectionSharedMetaData>
Collections::Manager::createOrReferenceMeta(
        CollectionID cid,
        const Collections::VB::CollectionSharedMetaDataView& view) {
    return collectionSMT.wlock()->createOrReference(cid, view);
}

void Collections::Manager::dereferenceMeta(
        CollectionID cid,
        SingleThreadedRCPtr<const VB::CollectionSharedMetaData>&& meta) {
    collectionSMT.wlock()->dereference(cid, std::move(meta));
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
            const std::vector<Collections::CollectionEntry>& collections)
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

    const std::vector<Collections::CollectionEntry>& collections;
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

    if (cb_isPrefix(statKey, "collections-details")) {
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
        // Access check for SimpleStats. Use testPrivilege as it won't log
        if (collector.testPrivilegeForStat(entry.second.sid, entry.first) !=
            cb::engine_errc::success) {
            continue; // skip this collection
        }

        const auto scopeItr = current->findScope(entry.second.sid);
        Expects(scopeItr != current->endScopes());
        cachedStats.addStatsForCollection(
                scopeItr->second.name, entry.second, collector);
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
    if (cb_isPrefix(statKey, "collections-byid")) {
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
        // Collection's scope is needed for privilege check
        auto scope = bucket.getCollectionsManager().getScopeID(cid);
        if (scope.second) {
            res = {scope.first, scope.second.value(), cid};
        } else {
            return {cb::engine_errc::unknown_collection, scope.first};
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

    // Access check for SimpleStats
    res.result = collector.testPrivilegeForStat(res.getScopeId(),
                                                res.getCollectionId());
    if (res.result != cb::engine_errc::success) {
        return res;
    }

    // Take a copy of the two items needed from the manifest and then release
    // the lock before vbucket visiting.
    std::string scopeName;
    Collections::CollectionEntry entry;
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
    cachedStats.addStatsForCollection(scopeName, entry, collector);
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
    if (cb_isPrefix(statKey, "scopes-details")) {
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
        if (collector.testPrivilegeForStat(itr->first, {}) !=
            cb::engine_errc::success) {
            continue; // skip this scope
        }

        cachedStats.addStatsForScope(itr->first,
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
    if (cb_isPrefix(statKey, "scopes-byid")) {
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

    // Access check for SimpleStats
    res.result = collector.testPrivilegeForStat(res.getScopeId(), {});
    if (res.result != cb::engine_errc::success) {
        return res;
    }

    // Take a copy of the two items needed from the manifest and then release
    // the lock before vbucket visiting.
    std::string scopeName;
    std::vector<Collections::CollectionEntry> scopeCollections;
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
            res.getScopeId(), scopeName, scopeCollections, collector);
    // add stats for each collection in the scope
    for (const auto& entry : scopeCollections) {
        cachedStats.addStatsForCollection(scopeName, entry, collector);
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
        const std::vector<Collections::CollectionEntry>& collections,
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

Collections::CachedStats::CachedStats(
        std::unordered_map<CollectionID, size_t>&& colMemUsed,
        std::unordered_map<CollectionID, AccumulatedStats>&& accumulatedStats)
    : colMemUsed(std::move(colMemUsed)),
      accumulatedStats(std::move(accumulatedStats)) {
}

void Collections::CachedStats::addStatsForCollection(
        std::string_view scopeName,
        const CollectionEntry& collection,
        const BucketStatCollector& collector) {
    auto collectionC = collector.forScope(scopeName, collection.sid)
                               .forCollection(collection.name, collection.cid);

    addAggregatedCollectionStats({collection.cid}, collectionC);

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
        ScopeID sid,
        std::string_view scopeName,
        const std::vector<Collections::CollectionEntry>& scopeCollections,
        const BucketStatCollector& collector) {
    auto scopeC = collector.forScope(scopeName, sid);
    std::vector<CollectionID> collections;
    collections.reserve(scopeCollections.size());

    // get the CollectionIDs - extract the keys from the map
    for (const auto& entry : scopeCollections) {
        collections.push_back(entry.cid);
    }
    addAggregatedCollectionStats(collections, scopeC);

    using namespace cb::stats;
    // add scope name
    scopeC.addStat(Key::scope_name, scopeName);
    // add scope collection count
    scopeC.addStat(Key::scope_collection_count, scopeCollections.size());
}

void Collections::CachedStats::addAggregatedCollectionStats(
        const std::vector<CollectionID>& cids, const StatCollector& collector) {
    size_t memUsed = 0;
    AccumulatedStats stats;

    for (const auto& cid : cids) {
        memUsed += colMemUsed[cid];
        stats += accumulatedStats[cid];
    }

    using namespace cb::stats;

    collector.addStat(Key::collection_mem_used, memUsed);
    collector.addStat(Key::collection_item_count, stats.itemCount);
    collector.addStat(Key::collection_data_size, stats.diskSize);

    collector.addStat(Key::collection_ops_store, stats.opsStore);
    collector.addStat(Key::collection_ops_delete, stats.opsDelete);
    collector.addStat(Key::collection_ops_get, stats.opsGet);
}
