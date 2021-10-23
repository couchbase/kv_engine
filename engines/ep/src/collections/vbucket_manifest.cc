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

#include "collections/vbucket_manifest.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/events_generated.h"
#include "collections/kvstore.h"
#include "collections/manifest.h"
#include "ep_time.h"
#include "item.h"
#include "systemevent_factory.h"
#include "vbucket.h"

#include <statistics/cbstat_collector.h>

#include <memory>

namespace Collections::VB {

Manifest::Manifest(std::shared_ptr<Manager> manager)
    : manager(std::move(manager)) {
    addNewScopeEntry(ScopeID::Default, DefaultScopeIdentifier, DataLimit{});
    addNewCollectionEntry({ScopeID::Default, CollectionID::Default},
                          DefaultCollectionIdentifier,
                          {} /*ttl*/,
                          0 /*startSeqno*/);
}

Manifest::Manifest(std::shared_ptr<Manager> manager,
                   const KVStore::Manifest& data)
    : manifestUid(data.manifestUid),
      manager(std::move(manager)),
      dropInProgress(data.droppedCollectionsExist) {
    {
        auto bucketManifest = this->manager->getCurrentManifest().rlock();
        for (const auto& scope : data.scopes) {
            addNewScopeEntry(
                    scope.metaData.sid,
                    scope.metaData.name,
                    bucketManifest->getScopeDataLimit(scope.metaData.sid));
        }
    }

    for (const auto& e : data.collections) {
        const auto& meta = e.metaData;
        addNewCollectionEntry(
                {meta.sid, meta.cid}, meta.name, meta.maxTtl, e.startSeqno);
    }
}

Manifest::~Manifest() {
    // Erase our collections/scopes and tell the manager so that metadata can
    // be deleted if no longer in use.
    for (auto itr = map.begin(); itr != map.end();) {
        auto& [id, entry] = *itr;
        manager->dereferenceMeta(id, entry.takeMeta());
        itr = map.erase(itr);
    }

    for (auto itr = scopes.begin(); itr != scopes.end();) {
        auto& [id, entry] = *itr;
        manager->dereferenceMeta(id, entry.takeMeta());
        itr = scopes.erase(itr);
    }
}

Manifest::Manifest(Manifest& other) : manager(other.manager) {
    // Prevent other from being modified while we copy.
    auto wlock = other.wlock();

    // Collection/Scope maps are not trivially copyable, so we do it manually
    for (auto& [cid, entry] : other.map) {
        CollectionSharedMetaDataView meta{
                entry.getName(), entry.getScopeID(), entry.getMaxTtl()};
        auto [itr, inserted] =
                map.try_emplace(cid,
                                other.manager->createOrReferenceMeta(cid, meta),
                                entry.getStartSeqno());
        Expects(inserted);
        itr->second.setItemCount(entry.getItemCount());
        itr->second.setDiskSize(entry.getDiskSize());
        itr->second.setHighSeqno(entry.getHighSeqno());
        itr->second.setPersistedHighSeqno(entry.getPersistedHighSeqno());
    }

    for (auto& [sid, entry] : other.scopes) {
        ScopeSharedMetaDataView meta{entry.getName(), entry.getDataLimit()};
        auto [itr, inserted] = scopes.try_emplace(
                sid,
                entry.getDataSize(),
                other.manager->createOrReferenceMeta(sid, meta));
        Expects(inserted);
    }

    droppedCollections = other.droppedCollections;
    manifestUid = other.manifestUid;
    dropInProgress.store(other.dropInProgress);
}

//
// applyDrops (and applyCreates et'al) all follow a similar pattern and are
// tightly coupled with completeUpdate. As input the function is given a set of
// changes to process (non const reference). The set could have 0, 1 or n
// entries to process. An input 'changes' of zero length means nothing happens.
// The function just skips and returns an empty optional.
//
// For 1 or n, the function starts by copying and removing 1 entry from the
// input set, leaving 0, 1 or n - 1 entries to be processed.
//
// In the case of 0, nothing further happens except returning the 'popped' copy.
//
// For the 1 or n -1  case, the entries are now dropped (or created based on the
// function), importantly the drop is tagged with the current manifest-UID and
// not the new manifest-UID. This is because we have not reached the end of the
// new manifest and cannot yet move the state forward to the new UID.
//
// Finally the changes are cleared and the 0 or 1 we copied earlier is returned.
//
std::optional<CollectionID> Manifest::applyDrops(
        WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<CollectionID>& changes) {
    std::optional<CollectionID> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (CollectionID cid : changes) {
        dropCollection(
                wHandle, vb, manifestUid, cid, OptionalSeqno{/*no-seqno*/});
    }
    changes.clear();
    return rv;
}

std::optional<Manifest::CollectionCreation> Manifest::applyCreates(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<CollectionCreation>& changes) {
    std::optional<CollectionCreation> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& creation : changes) {
        createCollection(wHandle,
                         vb,
                         manifestUid,
                         creation.identifiers,
                         creation.name,
                         creation.maxTtl,
                         OptionalSeqno{/*no-seqno*/});
    }
    changes.clear();
    return rv;
}

std::optional<ScopeID> Manifest::applyScopeDrops(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<ScopeID>& changes) {
    std::optional<ScopeID> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (ScopeID sid : changes) {
        dropScope(wHandle, vb, manifestUid, sid, OptionalSeqno{/*no-seqno*/});
    }

    changes.clear();
    return rv;
}

std::optional<Manifest::ScopeCreation> Manifest::applyScopeCreates(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<ScopeCreation>& changes) {
    std::optional<ScopeCreation> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& creation : changes) {
        createScope(wHandle,
                    vb,
                    manifestUid,
                    creation.sid,
                    creation.name,
                    creation.dataLimit,
                    OptionalSeqno{/*no-seqno*/});
    }
    changes.clear();
    return rv;
}

void Manifest::updateUid(ManifestUid uid, bool reset) {
    if (reset) {
        manifestUid.reset(uid);
    } else {
        manifestUid = uid;
    }
}

// The update is split into two parts
// 1) Input validation and change processing (only need read lock)
// 2) Applying the change(s) (need write lock)
// We utilise the upgrade feature of our mutex so that we can switch from
// read (using UpgradeHolder) to write
ManifestUpdateStatus Manifest::update(VBucket& vb,
                                      const Collections::Manifest& manifest) {
    mutex_type::UpgradeHolder upgradeLock(rwlock);
    auto status = canUpdate(manifest);
    if (status != ManifestUpdateStatus::Success) {
        EP_LOG_WARN(
                "Manifest::update {} error:{} vb-uid:{:#x} manifest-uid:{:#x}",
                vb.getId(),
                to_string(status),
                manifestUid,
                manifest.getUid());
        return status;
    }

    auto changes = processManifest(manifest);

    // If manifest was equal.
    if (manifest.getUid() == manifestUid) {
        if (changes.empty()) {
            return ManifestUpdateStatus::Success;
        } else if (changes.onlyScopesModified()) {
            EP_LOG_WARN("Manifest::update {} equal uid and {} scopes modified",
                        vb.getId(),
                        changes.scopesToModify.size());
        } else {
            // Log verbosely for this case
            EP_LOG_WARN(
                    "Manifest::update {} with equal uid:{:#x} but differences "
                    "scopes+:{}, collections+:{}, scopes-:{}, collections-:{}, "
                    "scopes-modified:{}",
                    vb.getId(),
                    manifestUid,
                    changes.scopesToCreate.size(),
                    changes.collectionsToCreate.size(),
                    changes.scopesToDrop.size(),
                    changes.collectionsToDrop.size(),
                    changes.scopesToModify.size());
            return ManifestUpdateStatus::EqualUidWithDifferences;
        }
    }

    completeUpdate(std::move(upgradeLock), vb, changes);
    return ManifestUpdateStatus::Success;
}

// This function performs the final part of the vbucket collection update.
// The function processes each of the changeset vectors and the ordering is
// important. The ordering of the system-events into the vbucket defines
// the order DCP transmits the events.
//
// 1) First collections are dropped, this must happen before step 4 and 2/5.
//    Before step 4 because a collection can be re-created by force (so it must
//    drop before create). Before step 2/5 because the collection drops could
//    be due to a scope drop, and the collection drop events must happen before
//    the scope drop event.
// 2) Scopes to force-drop must happen next. A scope drop must be seen after
//    the collection drop events and the before any subsequent re-create of the
//    scope.
// 3) Scopes are created. This must happen before step 4, collection creation.
// 4) Collections are created.
// 5) Scopes are dropped, this is last so the scope drop will come after any
//    collections dropped as part of the scope removal.
//
// As the function runs it also has to decide at each point which manifest-UID
// to associate with the system-events. The new manifest UID is only "exposed"
// for the final event of the changeset.
void Manifest::completeUpdate(mutex_type::UpgradeHolder&& upgradeLock,
                              ::VBucket& vb,
                              Manifest::ManifestChanges& changeset) {
    // Write Handle now needed
    WriteHandle wHandle(*this, std::move(upgradeLock));

    // Capture the UID
    if (changeset.empty()) {
        updateUid(changeset.uid, false /*reset*/);
        return;
    }

    auto finalDeletion = applyDrops(wHandle, vb, changeset.collectionsToDrop);
    if (finalDeletion) {
        auto uid = changeset.empty() ? changeset.uid : manifestUid;
        dropCollection(
                wHandle, vb, uid, *finalDeletion, OptionalSeqno{/*no-seqno*/});
    }

    auto finalScopeCreate =
            applyScopeCreates(wHandle, vb, changeset.scopesToCreate);
    if (finalScopeCreate) {
        auto uid = changeset.empty() ? changeset.uid : manifestUid;
        createScope(wHandle,
                    vb,
                    uid,
                    finalScopeCreate.value().sid,
                    finalScopeCreate.value().name,
                    finalScopeCreate.value().dataLimit,
                    OptionalSeqno{/*no-seqno*/});
    }

    auto finalAddition =
            applyCreates(wHandle, vb, changeset.collectionsToCreate);

    if (finalAddition) {
        auto uid = changeset.empty() ? changeset.uid : manifestUid;
        createCollection(wHandle,
                         vb,
                         uid,
                         finalAddition.value().identifiers,
                         finalAddition.value().name,
                         finalAddition.value().maxTtl,
                         OptionalSeqno{/*no-seqno*/});
    }

    // This is done last so the scope deletion follows any collection
    // deletions
    auto finalScopeDrop = applyScopeDrops(wHandle, vb, changeset.scopesToDrop);

    if (finalScopeDrop) {
        dropScope(wHandle,
                  vb,
                  changeset.uid,
                  *finalScopeDrop,
                  OptionalSeqno{/*no-seqno*/});
    }

    // Can do the scope modifications last - these generate no events but will
    // just sync any scope to the manifest
    for (const auto& modified : changeset.scopesToModify) {
        modifyScope(wHandle, vb, changeset.uid, modified);
    }
}

// @return true if newEntry compared to existingEntry shows an immutable
//              property has changed
static bool isImmutablePropertyModified(const CollectionEntry& newEntry,
                                        const ManifestEntry& existingEntry) {
    return newEntry.sid != existingEntry.getScopeID() ||
           newEntry.name != existingEntry.getName();
}

// @return true if newEntry compared to existingEntry shows an immutable
//              property has changed (only compares Scope vs name)
static bool isImmutablePropertyModified(const Collections::Scope& newEntry,
                                        std::string_view name) {
    return newEntry.name != name;
}

ManifestUpdateStatus Manifest::canUpdate(
        const Collections::Manifest& manifest) const {
    // Cannot go backwards
    if (manifest.getUid() < manifestUid) {
        return ManifestUpdateStatus::Behind;
    }

    for (const auto& [cid, entry] : map) {
        auto itr = manifest.findCollection(cid);
        if (itr != manifest.end() &&
            isImmutablePropertyModified(itr->second, entry)) {
            return ManifestUpdateStatus::ImmutablePropertyModified;
        }
    }

    for (const auto& [sid, entry] : scopes) {
        auto itr = manifest.findScope(sid);
        if (itr != manifest.endScopes() &&
            isImmutablePropertyModified(itr->second, entry.getName())) {
            return ManifestUpdateStatus::ImmutablePropertyModified;
        }
    }

    return ManifestUpdateStatus::Success;
}

void Manifest::createCollection(const WriteHandle& wHandle,
                                ::VBucket& vb,
                                ManifestUid newManUid,
                                ScopeCollectionPair identifiers,
                                std::string_view collectionName,
                                cb::ExpiryLimit maxTtl,
                                OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the collections
    // map. The start-seqno is 0 here and is patched up once we've created and
    // queued the system-event Item (step 2 and 3)
    auto& entry = addNewCollectionEntry(identifiers, collectionName, maxTtl, 0);

    // 1.1 record the uid of the manifest which is adding the collection
    updateUid(newManUid, optionalSeqno.has_value());

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueCollectionSystemEvent(wHandle,
                                            vb,
                                            identifiers.second,
                                            collectionName,
                                            entry,
                                            false,
                                            optionalSeqno,
                                            {/*no callback*/});

    EP_LOG_INFO(
            "{} create collection:id:{}, name:{} in scope:{}, seq:{}, "
            "manifest:{:#x}{}{}",
            vb.getId(),
            identifiers.second,
            collectionName,
            identifiers.first,
            seqno,
            newManUid,
            maxTtl.has_value()
                    ? ", maxttl:" + std::to_string(maxTtl.value().count())
                    : "",
            optionalSeqno.has_value() ? ", replica" : "");

    // 3. Now patch the entry with the seqno of the system event, note the copy
    //    of the manifest taken at step 1 gets the correct seqno when the system
    //    event is flushed.
    entry.setStartSeqno(seqno);
}

ManifestEntry& Manifest::addNewCollectionEntry(ScopeCollectionPair identifiers,
                                               std::string_view collectionName,
                                               cb::ExpiryLimit maxTtl,
                                               int64_t startSeqno) {
    CollectionSharedMetaDataView meta{
            collectionName, identifiers.first, maxTtl};
    auto [itr, inserted] = map.try_emplace(
            identifiers.second,
            manager->createOrReferenceMeta(identifiers.second, meta),
            startSeqno);

    if (!inserted) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "The collection already exists: failed adding collection:" +
                        identifiers.second.to_string() +
                        ", name:" + std::string(collectionName) +
                        ", scope:" + identifiers.first.to_string() +
                        ", startSeqno:" + std::to_string(startSeqno));
    }

    return itr->second;
}

ScopeEntry& Manifest::addNewScopeEntry(ScopeID sid,
                                       std::string_view scopeName,
                                       DataLimit dataLimit) {
    return addNewScopeEntry(
            sid,
            manager->createOrReferenceMeta(
                    sid, ScopeSharedMetaDataView{scopeName, dataLimit}));
}

ScopeEntry& Manifest::addNewScopeEntry(
        ScopeID sid, SingleThreadedRCPtr<ScopeSharedMetaData> sharedMeta) {
    auto [itr, emplaced] = scopes.try_emplace(sid, 0, sharedMeta);
    if (!emplaced) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope already exists, scope:" + sid.to_string());
    }

    return itr->second;
}

void Manifest::dropCollection(WriteHandle& wHandle,
                              ::VBucket& vb,
                              ManifestUid newManUid,
                              CollectionID cid,
                              OptionalSeqno optionalSeqno) {
    bool processingTombstone = false;
    // A replica that receives a collection tombstone is required to persist
    // that tombstone, so the replica can switch to active consistently
    if (optionalSeqno.has_value() && map.count(cid) == 0) {
        // Must store an event that replicates what the active had
        processingTombstone = true;

        // Add enough state so we can generate a system event that represents
        // the tombstone. The collection's scopeID, ttl and start-seqno are
        // unknown for a tombstone (and of no use).
        // After adding the entry, we can now proceed to queue a system event
        // as normal, the system event we generate can now be used to re-trigger
        // DCP delete collection if the replica is itself DCP streamed (or made
        // active)
        addNewCollectionEntry({ScopeID::Default, cid},
                              "tombstone",
                              cb::ExpiryLimit{},
                              0 /*startSeq*/);
    }

    auto itr = map.find(cid);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "did not find collection:" + cid.to_string());
    }

    // record the uid of the manifest which removed the collection
    updateUid(newManUid, optionalSeqno.has_value());

    auto seqno = queueCollectionSystemEvent(
            wHandle,
            vb,
            cid,
            {/*no name*/},
            itr->second,
            true /*delete*/,
            optionalSeqno,
            vb.getSaveDroppedCollectionCallback(cid, wHandle, itr->second));

    EP_LOG_INFO(
            "{} drop collection:id:{} from scope:{}, seq:{}, manifest:{:#x}"
            "{}{}",
            vb.getId(),
            cid,
            itr->second.getScopeID(),
            seqno,
            newManUid,
            processingTombstone ? ", tombstone" : "",
            optionalSeqno.has_value() ? ", replica" : "");

    manager->dereferenceMeta(cid, itr->second.takeMeta());
    map.erase(itr);
}

void Manifest::collectionDropPersisted(CollectionID cid, uint64_t seqno) {
    // As soon as we get notification that a dropped collection was flushed
    // successfully, mark this flag so subsequent flushes can maintain stats
    // correctly.
    dropInProgress.store(true);

    droppedCollections.wlock()->remove(cid, seqno);
}

const ManifestEntry& Manifest::getManifestEntry(CollectionID identifier) const {
    auto itr = map.find(identifier);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "did not find collection:" + identifier.to_string());
    }

    return itr->second;
}

const ScopeEntry& Manifest::getScopeEntry(ScopeID sid) const {
    auto itr = scopes.find(sid);
    if (itr == scopes.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "did not find scope:" + sid.to_string());
    }

    return itr->second;
}

void Manifest::createScope(const WriteHandle& wHandle,
                           ::VBucket& vb,
                           ManifestUid newManUid,
                           ScopeID sid,
                           std::string_view scopeName,
                           DataLimit dataLimit,
                           OptionalSeqno optionalSeqno) {
    auto& entry = addNewScopeEntry(sid, scopeName, dataLimit);

    bool dataLimitModified{false};
    if (!optionalSeqno) {
        // This case may correct the data limit of this scope, why? The active
        // vbucket is creating a scope yet it is referencing name and dataLimit
        // from the SharedMetaDataTable. If the SharedMetaDataTable entry was
        // first created by a replicaCreateScope (where dataLimit is not known)
        // then the first active createScope will need to correct the value in
        // the SharedMetaDataTable.
        dataLimitModified = entry.updateDataLimitIfDifferent(dataLimit);
    }

    // record the uid of the manifest which added the scope
    updateUid(newManUid, optionalSeqno.has_value());

    flatbuffers::FlatBufferBuilder builder;
    auto scope = CreateScope(
            builder,
            getManifestUid(),
            uint32_t(sid),
            builder.CreateString(scopeName.data(), scopeName.size()));
    builder.Finish(scope);

    auto item = SystemEventFactory::makeScopeEvent(
            sid,
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    auto seqno = vb.addSystemEventItem(
            std::move(item), optionalSeqno, {}, wHandle, {});

    EP_LOG_INFO("{} create scope:id:{} name:{}, seq:{}, manifest:{:#x}{}{}{}",
                vb.getId(),
                sid,
                scopeName,
                seqno,
                manifestUid,
                entry.getDataLimit() ? fmt::format(", limit:{}",
                                                   entry.getDataLimit().value())
                                     : "",
                dataLimitModified ? ", mod" : "",
                optionalSeqno.has_value() ? ", replica" : "");
}

void Manifest::modifyScope(const WriteHandle& wHandle,
                           ::VBucket& vb,
                           ManifestUid newManUid,
                           const ScopeModified& modification) {
    // Update under write lock
    modification.entry.setDataLimit(modification.dataLimit);

    EP_LOG_INFO(
            "{} modifying scope:id:{} manifest:{:#x}{}",
            vb.getId(),
            modification.sid,
            manifestUid,
            modification.dataLimit.has_value()
                    ? fmt::format(" limit:{}", modification.dataLimit.value())
                    : "");
}

void Manifest::dropScope(const WriteHandle& wHandle,
                         ::VBucket& vb,
                         ManifestUid newManUid,
                         ScopeID sid,
                         OptionalSeqno optionalSeqno) {
    auto itr = scopes.find(sid);
    // An active manifest must store the scope in-order to drop it, the optional
    // seqno being uninitialised is how we know we're active. A replica though
    // can accept scope drop events against scopes it doesn't store, i.e. a
    // tombstone being transmitted from the active.
    if (!optionalSeqno && itr == scopes.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "no seqno defined and the scope doesn't exist, scope:" +
                        sid.to_string());
    } else if (itr != scopes.end()) {
        // Tell the manager so that it can check for final clean-up
        manager->dereferenceMeta(sid, itr->second.takeMeta());
        // erase the entry (drops our reference to meta)
        scopes.erase(itr);
    }

    // record the uid of the manifest which removed the scope
    updateUid(newManUid, optionalSeqno.has_value());

    flatbuffers::FlatBufferBuilder builder;
    auto scope = CreateDroppedScope(builder, getManifestUid(), uint32_t(sid));
    builder.Finish(scope);

    auto item = SystemEventFactory::makeScopeEvent(
            sid,
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    item->setDeleted();

    auto seqno = vb.addSystemEventItem(
            std::move(item), optionalSeqno, {}, wHandle, {});

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!optionalSeqno.has_value()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    EP_LOG_INFO("{} drop scope:id:{} seq:{}, manifest:{:#x}{}",
                vb.getId(),
                sid,
                seqno,
                manifestUid,
                optionalSeqno.has_value() ? ", replica" : "");
}

Manifest::ManifestChanges Manifest::processManifest(
        const Collections::Manifest& manifest) {
    ManifestChanges rv{manifest.getUid()};

    // First iterate through the collections of this VB::Manifest
    for (const auto& [cid, entry] : map) {
        // Look-up the collection inside the new manifest
        auto itr = manifest.findCollection(cid);

        if (itr == manifest.end()) {
            // Not found, so this collection should be dropped.
            rv.collectionsToDrop.push_back(cid);
        }
    }

    // Second iterate through the scopes of this VB::Manifest
    for (const auto& [sid, entry] : scopes) {
        // Look-up the scope inside the new manifest
        auto itr = manifest.findScope(sid);

        if (itr == manifest.endScopes()) {
            // Not found, so this scope should be dropped.
            rv.scopesToDrop.push_back(sid);
        }
    }

    // Finally scopes and collections in Manifest but not in our map
    for (auto scopeItr = manifest.beginScopes();
         scopeItr != manifest.endScopes();
         scopeItr++) {
        auto myScope = scopes.find(scopeItr->first);
        if (myScope == scopes.end()) {
            // Scope is not mapped
            rv.scopesToCreate.push_back({scopeItr->first,
                                         scopeItr->second.name,
                                         scopeItr->second.dataLimit});
        } else {
            if (myScope->second.getDataLimit() != scopeItr->second.dataLimit) {
                // save the scope being modified and the new dataLimit
                rv.scopesToModify.push_back({myScope->first,
                                             myScope->second,
                                             scopeItr->second.dataLimit});
            }
        }

        for (const auto& m : scopeItr->second.collections) {
            auto mapItr = map.find(m.cid);

            if (mapItr == map.end()) {
                rv.collectionsToCreate.push_back(
                        {std::make_pair(m.sid, m.cid), m.name, m.maxTtl});
            }
        }
    }

    return rv;
}

bool Manifest::doesKeyContainValidCollection(const DocKey& key) const {
    return map.count(key.getCollectionID()) > 0;
}

bool Manifest::isScopeValid(ScopeID scopeID) const {
    return scopes.count(scopeID) != 0;
}

Manifest::container::const_iterator Manifest::getManifestEntry(
        const DocKey& key, AllowSystemKeys) const {
    CollectionID lookup = key.getCollectionID();
    if (lookup == CollectionID::System) {
        lookup = getCollectionIDFromKey(key);
    } // else we lookup with CID which if is System => fail
    return map.find(lookup);
}

Manifest::container::const_iterator Manifest::getManifestEntry(
        const DocKey& key) const {
    return map.find(key.getCollectionID());
}

Manifest::container::const_iterator Manifest::getManifestIterator(
        CollectionID id) const {
    return map.find(id);
}

bool Manifest::isLogicallyDeleted(const DocKey& key, int64_t seqno) const {
    CollectionID lookup = key.getCollectionID();
    if (lookup == CollectionID::System) {
        lookup = getCollectionIDFromKey(key);
    }
    auto itr = map.find(lookup);
    return isLogicallyDeleted(itr, seqno);
}

bool Manifest::isLogicallyDeleted(const container::const_iterator entry,
                                  int64_t seqno) const {
    if (entry == map.end()) {
        // Not in map - definitely deleted (or never existed)
        return true;
    }

    // seqno >= 0 (so temp items etc... are ok) AND the seqno is below the
    // collection start (start is set on creation and moves with flush)
    return seqno >= 0 && (uint64_t(seqno) < entry->second.getStartSeqno());
}

void Manifest::processExpiryTime(const container::const_iterator entry,
                                 Item& itm,
                                 std::chrono::seconds bucketTtl) const {
    itm.setExpTime(processExpiryTime(entry, itm.getExptime(), bucketTtl));
}

time_t Manifest::processExpiryTime(const container::const_iterator entry,
                                   time_t t,
                                   std::chrono::seconds bucketTtl) const {
    std::chrono::seconds enforcedTtl{0};

    if (bucketTtl.count()) {
        enforcedTtl = bucketTtl;
    }

    // If the collection has a TTL, it gets used
    if (entry->second.getMaxTtl()) {
        enforcedTtl = entry->second.getMaxTtl().value();
    }

    // Note: A ttl value of 0 means no maxTTL
    if (enforcedTtl.count()) {
        t = ep_limit_abstime(t, enforcedTtl);
    }
    return t;
}

std::unique_ptr<Item> Manifest::makeCollectionSystemEvent(
        ManifestUid uid,
        CollectionID cid,
        std::string_view collectionName,
        const ManifestEntry& entry,
        bool deleted,
        OptionalSeqno seq) {
    flatbuffers::FlatBufferBuilder builder;
    if (!deleted) {
        auto collection = CreateCollection(
                builder,
                uid,
                uint32_t(entry.getScopeID()),
                uint32_t(cid),
                entry.getMaxTtl().has_value(),
                entry.getMaxTtl().has_value() ? (*entry.getMaxTtl()).count()
                                              : 0,
                builder.CreateString(collectionName.data(),
                                     collectionName.size()));
        builder.Finish(collection);
    } else {
        auto collection = CreateDroppedCollection(
                builder, uid, uint32_t(entry.getScopeID()), uint32_t(cid));
        builder.Finish(collection);
    }

    auto item = SystemEventFactory::makeCollectionEvent(
            cid, {builder.GetBufferPointer(), builder.GetSize()}, seq);

    if (deleted) {
        item->setDeleted();
    }
    return item;
}

uint64_t Manifest::queueCollectionSystemEvent(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        CollectionID cid,
        std::string_view collectionName,
        const ManifestEntry& entry,
        bool deleted,
        OptionalSeqno seq,
        std::function<void(int64_t)> assignedSeqnoCallback) const {
    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!seq.has_value()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    auto item = makeCollectionSystemEvent(
            getManifestUid(), cid, collectionName, entry, deleted, seq);

    // Create and transfer Item ownership to the VBucket
    auto rv = vb.addSystemEventItem(
            std::move(item), seq, cid, wHandle, assignedSeqnoCallback);

    return rv;
}

size_t Manifest::getSystemEventItemCount() const {
    // Every 'live' collection has 1 'live' item, except for the default
    // collection which has no creation event.
    size_t rv = map.size();
    if (rv && map.count(CollectionID::Default)) {
        rv--;
    }

    // Every 'live' scope has 1 'live' item, except for the default scope which
    // has no event.
    rv += scopes.size();
    if (rv && scopes.count(ScopeID::Default)) {
        rv--;
    }
    return rv;
}

void Manifest::saveDroppedCollection(CollectionID cid,
                                     const ManifestEntry& droppedEntry,
                                     uint64_t droppedSeqno) {
    // Copy enough data about the collection that any items currently
    // flushing to this collection can calculate statistic updates.
    // Updates need to be compare against these values and not any 'open'
    // version of the collection.
    droppedCollections.wlock()->insert(
            cid,
            DroppedCollectionInfo{droppedEntry.getStartSeqno(),
                                  droppedSeqno,
                                  droppedEntry.getItemCount(),
                                  droppedEntry.getDiskSize(),
                                  droppedEntry.getHighSeqno()});
}

StatsForFlush Manifest::getStatsForFlush(CollectionID cid,
                                         uint64_t seqno) const {
    auto mapItr = map.find(cid);
    if (mapItr != map.end()) {
        // Collection (id) exists, is it the correct 'generation'. If the given
        // seqno is equal or greater than the start, then it is a match
        if (seqno >= mapItr->second.getStartSeqno()) {
            return StatsForFlush{mapItr->second.getItemCount(),
                                 mapItr->second.getDiskSize(),
                                 mapItr->second.getPersistedHighSeqno()};
        }
    }

    // Not open or open is not the correct generation
    // It has to be in this droppedCollections
    return droppedCollections.rlock()->get(cid, seqno);
}

template <class T>
static void verifyFlatbuffersData(std::string_view buf,
                                  const std::string& caller) {
    flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(buf.data()),
                            buf.size());
    if (v.VerifyBuffer<T>(nullptr)) {
        return;
    }

    std::stringstream ss;
    ss << "Collections::VB::Manifest::verifyFlatbuffersData: " << caller
       << " data invalid, ptr:" << reinterpret_cast<const void*>(buf.data())
       << ", size:" << buf.size();

    throw std::runtime_error(ss.str());
}

CreateEventData Manifest::getCreateEventData(std::string_view flatbufferData) {
    verifyFlatbuffersData<Collection>(flatbufferData, "getCreateEventData");
    auto collection = flatbuffers::GetRoot<Collection>(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()));

    // if maxTtlValid needs considering
    cb::ExpiryLimit maxTtl;
    if (collection->ttlValid()) {
        maxTtl = std::chrono::seconds(collection->maxTtl());
    }
    return {ManifestUid(collection->uid()),
            {collection->scopeId(),
             collection->collectionId(),
             collection->name()->str(),
             maxTtl}};
}

DropEventData Manifest::getDropEventData(std::string_view flatbufferData) {
    verifyFlatbuffersData<DroppedCollection>(flatbufferData,
                                             "getDropEventData");
    auto droppedCollection = flatbuffers::GetRoot<DroppedCollection>(
            (const uint8_t*)flatbufferData.data());

    return {ManifestUid(droppedCollection->uid()),
            droppedCollection->scopeId(),
            droppedCollection->collectionId()};
}

CreateScopeEventData Manifest::getCreateScopeEventData(
        std::string_view flatbufferData) {
    verifyFlatbuffersData<Scope>(flatbufferData, "getCreateScopeEventData");
    auto scope = flatbuffers::GetRoot<Scope>(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()));

    return {ManifestUid(scope->uid()),
            {scope->scopeId(), scope->name()->str()}};
}

DropScopeEventData Manifest::getDropScopeEventData(
        std::string_view flatbufferData) {
    verifyFlatbuffersData<DroppedScope>(flatbufferData,
                                        "getDropScopeEventData");
    auto droppedScope = flatbuffers::GetRoot<DroppedScope>(
            reinterpret_cast<const uint8_t*>(flatbufferData.data()));

    return {ManifestUid(droppedScope->uid()), droppedScope->scopeId()};
}

std::string Manifest::getExceptionString(const std::string& thrower,
                                         const std::string& error) const {
    std::stringstream ss;
    ss << "VB::Manifest:" << thrower << ": " << error << ", this:" << *this;
    return ss.str();
}

std::optional<ScopeID> Manifest::getScopeID(CollectionID identifier) const {
    auto itr = map.find(identifier);
    if (itr == map.end()) {
        return {};
    }
    return itr->second.getScopeID();
}

uint64_t Manifest::getItemCount(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.getItemCount();
}

uint64_t Manifest::getHighSeqno(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.getHighSeqno();
}

void Manifest::setHighSeqno(CollectionID collection, uint64_t value) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string() +
                        " with value:" + std::to_string(value));
    }
    itr->second.setHighSeqno(value);
}

void Manifest::setDiskSize(CollectionID collection, size_t size) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    itr->second.setDiskSize(size);
}

void Manifest::updateDataSize(ScopeID sid, ssize_t delta) const {
    auto itr = scopes.find(sid);
    if (itr == scopes.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__, "failed find of scope:" + sid.to_string());
    }
    itr->second.updateDataSize(delta);
}

size_t Manifest::getDataSize(ScopeID sid) const {
    auto scope = scopes.find(sid);
    if (scope == scopes.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope not found for " + sid.to_string());
    }
    return scope->second.getDataSize();
}

void Manifest::updateScopeDataSize(const container::const_iterator entry,
                                   ssize_t delta) const {
    // Don't continue with a zero update, avoids scope map lookup
    if (delta == 0) {
        return;
    }

    if (entry == map.end()) {
        throwException<std::invalid_argument>(__FUNCTION__,
                                              "iterator is invalid");
    }

    updateDataSize(entry->second.getScopeID(), delta);
}

uint64_t Manifest::getPersistedHighSeqno(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.getPersistedHighSeqno();
}

void Manifest::setPersistedHighSeqno(CollectionID collection,
                                     uint64_t value,
                                     bool noThrow) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        if (noThrow) {
            return;
        }
        throwException<std::logic_error>(
                __FUNCTION__,
                "did not find collection:" + collection.to_string());
    }
    itr->second.setPersistedHighSeqno(value);
}

void Manifest::incrementItemCount(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.incrementItemCount();
}

void Manifest::decrementItemCount(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.decrementItemCount();
}

bool Manifest::addCollectionStats(Vbid vbid,
                                  const StatCollector& collector) const {
    fmt::memory_buffer key;

    format_to(key, "vb_{}:collections", vbid.get());
    collector.addStat(std::string_view(key.data(), key.size()), map.size());

    key.resize(0);

    format_to(key, "vb_{}:manifest:uid", vbid.get());
    collector.addStat(std::string_view(key.data(), key.size()), manifestUid);

    for (const auto& entry : map) {
        if (!entry.second.addStats(entry.first.to_string(), vbid, collector)) {
            return false;
        }
    }

    return droppedCollections.rlock()->addStats(vbid, collector);
}

bool Manifest::addScopeStats(Vbid vbid, const StatCollector& collector) const {
    fmt::memory_buffer key;

    format_to(key, "vb_{}:scopes", vbid.get());
    collector.addStat(std::string_view(key.data(), key.size()), scopes.size());

    key.resize(0);

    format_to(key, "vb_{}:manifest:uid", vbid.get());
    collector.addStat(std::string_view(key.data(), key.size()), manifestUid);

    for (const auto& [sid, value] : scopes) {
        key.resize(0);
        format_to(key, "vb_{}:{}:name:", vbid.get(), sid);
        collector.addStat(std::string_view(key.data(), key.size()),
                          value.getName());
        key.resize(0);
        format_to(key, "vb_{}:{}:data_size", vbid.get(), sid);
        collector.addStat(std::string_view(key.data(), key.size()),
                          value.getDataSize());

        if (value.getDataLimit()) {
            key.resize(0);
            format_to(key, "vb_{}:{}:data_limit", vbid.get(), sid);
            collector.addStat(std::string_view(key.data(), key.size()),
                              value.getDataLimit().value());
        }
    }

    // Dump all collections and how they map to a scope. Stats requires unique
    // keys, so cannot use the cid as the value (as key won't be unique), so
    // return anything from the entry for the value, we choose item count.
    for (const auto& [cid, entry] : map) {
        key.resize(0);

        format_to(
                key, "vb_{}:{}:{}:items", vbid.get(), entry.getScopeID(), cid);
        collector.addStat(std::string_view(key.data(), key.size()),
                          entry.getItemCount());
    }

    return true;
}

void Manifest::updateSummary(Summary& summary) const {
    for (const auto& [cid, entry] : map) {
        auto s = summary.find(cid);
        if (s == summary.end()) {
            summary[cid] = entry.getStatsForSummary();
        } else {
            s->second += entry.getStatsForSummary();
        }
    }
}

void Manifest::accumulateStats(const std::vector<CollectionEntry>& collections,
                               Summary& summary) const {
    for (const auto& entry : collections) {
        auto itr = map.find(entry.cid);
        if (itr != map.end()) {
            summary[entry.cid] += itr->second.getStatsForSummary();
        }
    }
}

std::optional<std::vector<CollectionID>> Manifest::getCollectionsForScope(
        ScopeID identifier) const {
    if (scopes.count(identifier) == 0) {
        return {};
    }

    std::vector<CollectionID> rv;
    for (const auto& collection : map) {
        if (collection.second.getScopeID() == identifier) {
            rv.push_back(collection.first);
        }
    }

    return rv;
}

bool Manifest::operator==(const Manifest& rhs) const {
    std::shared_lock<mutex_type> readLock(rwlock);
    std::shared_lock<mutex_type> otherReadLock(rhs.rwlock);

    if (rhs.map.size() != map.size()) {
        return false;
    }
    // Check all collections match
    for (const auto& [cid, entry] : map) {
        if (entry != rhs.getManifestEntry(cid)) {
            return false;
        }
    }

    if (scopes.size() != rhs.scopes.size()) {
        return false;
    }
    // Check all scopes can be found
    for (const auto& [sid, entry] : scopes) {
        if (entry != rhs.getScopeEntry(sid)) {
            return false;
        }
    }
    if (rhs.manifestUid != manifestUid) {
        return false;
    }

    return true;
}

bool Manifest::operator!=(const Manifest& rhs) const {
    return !(*this == rhs);
}

cb::engine_errc Manifest::getScopeDataLimitStatus(
        const container::const_iterator itr, size_t nBytes) const {
    const auto& entry = getScopeEntry(itr->second.getScopeID());

    if (entry.getDataLimit() &&
        (entry.getDataSize() + nBytes) > entry.getDataLimit().value()) {
        return cb::engine_errc::scope_size_limit_exceeded;
    }
    return cb::engine_errc::success;
}

void Manifest::DroppedCollections::insert(CollectionID cid,
                                          const DroppedCollectionInfo& info) {
    auto [dropped, emplaced] = droppedCollections.try_emplace(
            cid, std::vector<DroppedCollectionInfo>{/*empty vector*/});
    (void)emplaced;
    dropped->second.emplace_back(info);
}

void Manifest::DroppedCollections::remove(CollectionID cid, uint64_t seqno) {
    auto dropItr = droppedCollections.find(cid);
    if (dropItr == droppedCollections.end()) {
        // bad time - every call of get should be from a flush which
        // had items we created for a collection which is open or was open.
        // to not find the collection at all means we have flushed an item
        // to a collection that never existed.
        std::stringstream ss;
        ss << *this;
        throw std::logic_error(std::string(__PRETTY_FUNCTION__) + ":" +
                               std::to_string(__LINE__) +
                               " The collection cannot be found collection:" +
                               cid.to_string() + " seqno:" +
                               std::to_string(seqno) + " " + ss.str());
    }

    auto& [key, vector] = *dropItr;
    (void)key;
    bool erased = false;
    for (auto itr = vector.begin(); itr != vector.end();) {
        // remove every dropped collection in the 'list' if seqno is >=
        if (seqno >= itr->end) {
            itr = vector.erase(itr);
            erased = true;
        } else {
            itr++;
        }
    }

    if (erased) {
        // clear the map?
        if (vector.empty()) {
            droppedCollections.erase(dropItr);
        }
        return;
    }

    std::stringstream ss;
    ss << *this;

    throw std::logic_error(std::string(__PRETTY_FUNCTION__) + ":" +
                           std::to_string(__LINE__) +
                           " The collection@seqno cannot be found collection:" +
                           cid.to_string() + " seqno:" + std::to_string(seqno) +
                           " " + ss.str());
}

StatsForFlush Manifest::DroppedCollections::get(CollectionID cid,
                                                uint64_t seqno) const {
    auto dropItr = droppedCollections.find(cid);

    if (dropItr == droppedCollections.end()) {
        // bad time - every call of get should be from a flush which
        // had items we created for a collection which is open or was open.
        // to not find the collection at all means we have flushed an item
        // to a collection that never existed.
        throw std::logic_error(std::string(__PRETTY_FUNCTION__) +
                               " The collection cannot be found collection:" +
                               cid.to_string() +
                               " seqno:" + std::to_string(seqno));
    }

    // loop - the length of this list in reality would be short it would
    // each 'depth' requires the collection to be dropped once, so length of
    // n means collection was drop/re-created n times.
    for (const auto& droppedInfo : dropItr->second) {
        // If the seqno is of the range of this dropped generation, this is
        // a match.
        if (seqno >= droppedInfo.start && seqno <= droppedInfo.end) {
            return StatsForFlush{droppedInfo.itemCount,
                                 droppedInfo.diskSize,
                                 droppedInfo.highSeqno};
        }
    }

    std::stringstream ss;
    ss << *this;

    // Similar to above, we should find something
    throw std::logic_error(
            std::string(__PRETTY_FUNCTION__) +
            " The collection seqno cannot be found cid:" + cid.to_string() +
            " seqno:" + std::to_string(seqno) + " " + ss.str());
}

size_t Manifest::DroppedCollections::size() const {
    return droppedCollections.size();
}

std::optional<size_t> Manifest::DroppedCollections::size(
        CollectionID cid) const {
    auto dropItr = droppedCollections.find(cid);
    if (dropItr != droppedCollections.end()) {
        return dropItr->second.size();
    }

    return {};
}

bool Manifest::DroppedCollections::addStats(
        Vbid vbid, const StatCollector& collector) const {
    for (const auto& [cid, list] : droppedCollections) {
        for (const auto& entry : list) {
            if (!entry.addStats(vbid, cid, collector)) {
                return false;
            }
        }
    }
    return true;
}

bool Manifest::DroppedCollectionInfo::addStats(
        Vbid vbid, CollectionID cid, const StatCollector& collector) const {
    fmt::memory_buffer prefix;
    format_to(prefix, "vb_{}:{}", vbid.get(), cid);
    const auto addStat = [&prefix, &collector](const auto& statKey,
                                               auto statValue) {
        fmt::memory_buffer key;
        format_to(key,
                  "{}:{}",
                  std::string_view{prefix.data(), prefix.size()},
                  statKey);
        collector.addStat(std::string_view(key.data(), key.size()), statValue);
    };

    addStat("start", start);
    addStat("end", end);
    addStat("items", itemCount);
    addStat("disk", diskSize);
    addStat("high_seqno", highSeqno);
    return true;
}

std::ostream& operator<<(std::ostream& os,
                         const Manifest::DroppedCollectionInfo& info) {
    os << "DroppedCollectionInfo s:" << info.start << ", e:" << info.end
       << ", items:" << info.itemCount << ", disk:" << info.diskSize
       << ", highSeq:" << info.highSeqno;
    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const Manifest::DroppedCollections& dc) {
    for (const auto& [cid, list] : dc.droppedCollections) {
        os << "DroppedCollections: cid:" << cid.to_string()
           << ", entries:" << list.size() << std::endl;
        for (const auto& info : list) {
            os << "  " << info << std::endl;
        }
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "VB::Manifest: "
       << "uid:" << manifest.manifestUid
       << ", scopes.size:" << manifest.scopes.size()
       << ", map.size:" << manifest.map.size() << std::endl;
    for (const auto& [cid, entry] : manifest.map) {
        os << "cid:" << cid.to_string() << ":" << entry << std::endl;
    }

    for (const auto& [sid, entry] : manifest.scopes) {
        os << "scope:" << sid.to_string() << ":" << entry << std::endl;
    }

    os << *manifest.droppedCollections.rlock() << std::endl;

    return os;
}

ReadHandle Manifest::lock() const {
    return {this, rwlock};
}

CachingReadHandle Manifest::lock(DocKey key) const {
    return {this, rwlock, key};
}

CachingReadHandle Manifest::lock(DocKey key,
                                 Manifest::AllowSystemKeys tag) const {
    return {this, rwlock, key, tag};
}

StatsReadHandle Manifest::lock(CollectionID cid) const {
    return {this, rwlock, cid};
}

WriteHandle Manifest::wlock() {
    return {*this, rwlock};
}

std::ostream& operator<<(std::ostream& os, const ReadHandle& readHandle) {
    os << "VB::Manifest::ReadHandle: manifest:" << *readHandle.manifest;
    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const CachingReadHandle& readHandle) {
    os << "VB::Manifest::CachingReadHandle: itr:";
    if (readHandle.valid()) {
        os << (*readHandle.itr).second;
    } else {
        os << "end";
    }
    os << ", cid:" << readHandle.key.getCollectionID().to_string();
    os << ", manifest:" << *readHandle.manifest;
    return os;
}

} // namespace Collections::VB
