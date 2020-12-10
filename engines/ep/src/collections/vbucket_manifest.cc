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

#include "collections/vbucket_manifest.h"
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

Manifest::Manifest() : scopes({{ScopeID::Default}}) {
    addNewCollectionEntry({ScopeID::Default, CollectionID::Default}, {});
}

Manifest::Manifest(const KVStore::Manifest& data)
    : manifestUid(data.manifestUid),
      dropInProgress(data.droppedCollectionsExist) {
    for (const auto& scope : data.scopes) {
        scopes.insert(scope.metaData.sid);
    }

    for (const auto& e : data.collections) {
        const auto& meta = e.metaData;
        addNewCollectionEntry({meta.sid, meta.cid}, meta.maxTtl, e.startSeqno);
    }
}

std::optional<CollectionID> Manifest::applyDeletions(WriteHandle& wHandle,
                                                     ::VBucket& vb,
                                                     ManifestChanges& changes) {
    std::optional<CollectionID> rv;
    if (!changes.collectionsToDrop.empty()) {
        rv = changes.collectionsToDrop.back();
        changes.collectionsToDrop.pop_back();
    }
    for (CollectionID id : changes.collectionsToDrop) {
        dropCollection(wHandle,
                       vb,
                       manifestUid,
                       id,
                       OptionalSeqno{/*no-seqno*/},
                       changes.forced);
    }

    return rv;
}

std::optional<Manifest::CollectionCreation> Manifest::applyCreates(
        const WriteHandle& wHandle, ::VBucket& vb, ManifestChanges& changes) {
    std::optional<CollectionCreation> rv;
    if (!changes.collectionsToCreate.empty()) {
        rv = changes.collectionsToCreate.back();
        changes.collectionsToCreate.pop_back();
    }
    for (const auto& creation : changes.collectionsToCreate) {
        createCollection(wHandle,
                         vb,
                         manifestUid,
                         creation.identifiers,
                         creation.name,
                         creation.maxTtl,
                         OptionalSeqno{/*no-seqno*/},
                         changes.forced);
    }

    return rv;
}

std::optional<ScopeID> Manifest::applyScopeDrops(const WriteHandle& wHandle,
                                                 ::VBucket& vb,
                                                 ManifestChanges& changes) {
    std::optional<ScopeID> rv;
    if (!changes.scopesToDrop.empty()) {
        rv = changes.scopesToDrop.back();
        changes.scopesToDrop.pop_back();
    }
    for (ScopeID id : changes.scopesToDrop) {
        dropScope(wHandle,
                  vb,
                  manifestUid,
                  id,
                  OptionalSeqno{/*no-seqno*/},
                  changes.forced);
    }

    return rv;
}

std::optional<Manifest::ScopeCreation> Manifest::applyScopeCreates(
        const WriteHandle& wHandle, ::VBucket& vb, ManifestChanges& changes) {
    std::optional<ScopeCreation> rv;
    if (!changes.scopesToCreate.empty()) {
        rv = changes.scopesToCreate.back();
        changes.scopesToCreate.pop_back();
    }
    for (const auto& creation : changes.scopesToCreate) {
        createScope(wHandle,
                    vb,
                    manifestUid,
                    creation.sid,
                    creation.name,
                    OptionalSeqno{/*no-seqno*/},
                    changes.forced);
    }

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
ManifestUpdateStatus Manifest::update(::VBucket& vb,
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

    // If manifest was equal. expect no changes (unless forced)
    if (manifest.getUid() == manifestUid && !manifest.isForcedUpdate()) {
        if (changes.empty()) {
            return ManifestUpdateStatus::Success;
        } else {
            // Log verbosely for this case
            EP_LOG_WARN(
                    "Manifest::update {} with equal uid:{:#x} but differences "
                    "scopes+:{}, collections+:{}, scopes-:{}, collections-:{}",
                    vb.getId(),
                    manifestUid,
                    changes.scopesToCreate.size(),
                    changes.collectionsToCreate.size(),
                    changes.scopesToDrop.size(),
                    changes.collectionsToDrop.size());
            return ManifestUpdateStatus::EqualUidWithDifferences;
        }
    }

    completeUpdate(std::move(upgradeLock), vb, changes);
    return ManifestUpdateStatus::Success;
}

void Manifest::completeUpdate(mutex_type::UpgradeHolder&& upgradeLock,
                              ::VBucket& vb,
                              Manifest::ManifestChanges& changes) {
    // Write Handle now needed
    WriteHandle wHandle(*this, std::move(upgradeLock));

    // Capture the UID
    if (changes.empty()) {
        updateUid(changes.uid, changes.forced);
        return;
    }

    auto finalScopeCreate = applyScopeCreates(wHandle, vb, changes);
    if (finalScopeCreate) {
        auto uid = changes.collectionsToCreate.empty() &&
                                   changes.collectionsToDrop.empty() &&
                                   changes.scopesToDrop.empty()
                           ? changes.uid
                           : manifestUid;
        createScope(wHandle,
                    vb,
                    uid,
                    finalScopeCreate.value().sid,
                    finalScopeCreate.value().name,
                    OptionalSeqno{/*no-seqno*/},
                    changes.forced);
    }

    auto finalDeletion = applyDeletions(wHandle, vb, changes);
    if (finalDeletion) {
        auto uid = changes.collectionsToCreate.empty() &&
                                   changes.scopesToDrop.empty()
                           ? changes.uid
                           : manifestUid;
        dropCollection(wHandle,
                       vb,
                       uid,
                       *finalDeletion,
                       OptionalSeqno{/*no-seqno*/},
                       changes.forced);
    }

    auto finalAddition = applyCreates(wHandle, vb, changes);

    if (finalAddition) {
        auto uid = changes.scopesToDrop.empty() ? changes.uid : manifestUid;
        createCollection(wHandle,
                         vb,
                         uid,
                         finalAddition.value().identifiers,
                         finalAddition.value().name,
                         finalAddition.value().maxTtl,
                         OptionalSeqno{/*no-seqno*/},
                         changes.forced);
    }

    // This is done last so the scope deletion follows any collection
    // deletions
    auto finalScopeDrop = applyScopeDrops(wHandle, vb, changes);

    if (finalScopeDrop) {
        dropScope(wHandle,
                  vb,
                  changes.uid,
                  *finalScopeDrop,
                  OptionalSeqno{/*no-seqno*/},
                  changes.forced);
    }
}

ManifestUpdateStatus Manifest::canUpdate(
        const Collections::Manifest& manifest) const {
    // Cannot go backwards (unless forced)
    if (manifest.getUid() < manifestUid && !manifest.isForcedUpdate()) {
        return ManifestUpdateStatus::Behind;
    }
    return ManifestUpdateStatus::Success;
}

void Manifest::createCollection(const WriteHandle& wHandle,
                                ::VBucket& vb,
                                ManifestUid newManUid,
                                ScopeCollectionPair identifiers,
                                std::string_view collectionName,
                                cb::ExpiryLimit maxTtl,
                                OptionalSeqno optionalSeqno,
                                bool isForcedAdd) {
    // 1. Update the manifest, adding or updating an entry in the collections
    // map. Specify a non-zero start and 0 for the TTL
    auto& entry = addNewCollectionEntry(identifiers, maxTtl);

    // 1.1 record the uid of the manifest which is adding the collection
    updateUid(newManUid, optionalSeqno.has_value() || isForcedAdd);

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
            "collections: {} create collection:id:{}, name:{} to "
            "scope:{}, "
            "maxTTL:{} {}, "
            "replica:{}, seqno:{}, manifest:{:#x}, force:{}",
            vb.getId(),
            identifiers.second,
            collectionName,
            identifiers.first,
            maxTtl.has_value(),
            maxTtl.value_or(std::chrono::seconds::zero()).count(),
            optionalSeqno.has_value(),
            seqno,
            newManUid,
            isForcedAdd);

    // 3. Now patch the entry with the seqno of the system event, note the copy
    //    of the manifest taken at step 1 gets the correct seqno when the system
    //    event is flushed.
    entry.setStartSeqno(seqno);
}

ManifestEntry& Manifest::addNewCollectionEntry(ScopeCollectionPair identifiers,
                                               cb::ExpiryLimit maxTtl,
                                               int64_t startSeqno) {
    // This method is only for when the map does not have the collection
    auto itr = map.find(identifiers.second);
    if (itr != map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "The collection already exists: failed adding collection:" +
                        identifiers.second.to_string() +
                        ", scope:" + identifiers.first.to_string() +
                        ", startSeqno:" + std::to_string(startSeqno));
    }

    auto inserted =
            map.emplace(identifiers.second,
                        ManifestEntry(identifiers.first, maxTtl, startSeqno));

    return (*inserted.first).second;
}

void Manifest::dropCollection(WriteHandle& wHandle,
                              ::VBucket& vb,
                              ManifestUid newManUid,
                              CollectionID cid,
                              OptionalSeqno optionalSeqno,
                              bool isForcedDrop) {
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
        addNewCollectionEntry({ScopeID::Default, cid}, {});
    }

    auto itr = map.find(cid);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "did not find collection:" + cid.to_string());
    }

    // record the uid of the manifest which removed the collection
    updateUid(newManUid, optionalSeqno.has_value() || isForcedDrop);

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
            "collections: {} drop of collection:{} from scope:{}"
            ", replica:{}, seqno:{}, manifest:{:#x} tombstone:{}, force:{}",
            vb.getId(),
            cid,
            itr->second.getScopeID(),
            optionalSeqno.has_value(),
            seqno,
            newManUid,
            processingTombstone,
            isForcedDrop);

    map.erase(itr);
}

// Method is cleaning up for insertions made in Manifest::dropCollection
void Manifest::collectionDropPersisted(CollectionID cid, uint64_t seqno) {
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

void Manifest::createScope(const WriteHandle& wHandle,
                           ::VBucket& vb,
                           ManifestUid newManUid,
                           ScopeID sid,
                           std::string_view scopeName,
                           OptionalSeqno optionalSeqno,
                           bool isForcedAdd) {
    if (isScopeValid(sid)) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope already exists, scope:" + sid.to_string());
    }

    scopes.insert(sid);

    // record the uid of the manifest which added the scope
    updateUid(newManUid, optionalSeqno.has_value() || isForcedAdd);

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

    EP_LOG_INFO(
            "collections: {} create scope:id:{} name:{},"
            "replica:{}, seqno:{}, manifest:{:#x}, force:{}",
            vb.getId(),
            sid,
            scopeName,
            optionalSeqno.has_value(),
            seqno,
            manifestUid,
            isForcedAdd);
}

void Manifest::dropScope(const WriteHandle& wHandle,
                         ::VBucket& vb,
                         ManifestUid newManUid,
                         ScopeID sid,
                         OptionalSeqno optionalSeqno,
                         bool isForcedDrop) {
    // An active manifest must store the scope in-order to drop it, the optional
    // seqno being uninitialised is how we know we're active. A replica though
    // can accept scope drop events against scopes it doesn't store, i.e. a
    // tombstone being transmitted from the active.
    if (!optionalSeqno && scopes.count(sid) == 0) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "no seqno defined and the scope doesn't exist, scope:" +
                        sid.to_string());
    }

    // Note: the following erase may actually do nothing if we're processing a
    // drop-scope tombstone
    scopes.erase(sid);

    // record the uid of the manifest which removed the scope
    updateUid(newManUid, optionalSeqno.has_value() || isForcedDrop);

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

    EP_LOG_INFO(
            "collections: {} dropped scope:id:{} "
            "replica:{}, seqno:{}, manifest:{:#x} force:{}",
            vb.getId(),
            sid,
            optionalSeqno.has_value(),
            seqno,
            manifestUid,
            isForcedDrop);
}

Manifest::ManifestChanges Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    ManifestChanges rv{manifest.getUid(), manifest.isForcedUpdate()};

    for (const auto& entry : map) {
        // If the entry is not found in the new manifest it must be
        // deleted.
        if (manifest.findCollection(entry.first) == manifest.end()) {
            rv.collectionsToDrop.push_back(entry.first);
        }
    }

    for (ScopeID sid : scopes) {
        // Remove the scopes that don't exist in the new manifest
        if (manifest.findScope(sid) == manifest.endScopes()) {
            rv.scopesToDrop.push_back(sid);
        }
    }

    // Add scopes and collections in Manifest but not in our map
    for (auto scopeItr = manifest.beginScopes();
         scopeItr != manifest.endScopes();
         scopeItr++) {
        if (std::find(scopes.begin(), scopes.end(), scopeItr->first) ==
            scopes.end()) {
            rv.scopesToCreate.push_back(
                    {scopeItr->first, scopeItr->second.name});
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
    return std::find(scopes.begin(), scopes.end(), scopeID) != scopes.end();
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

bool Manifest::isDropInProgress() const {
    return dropInProgress;
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
                "failed find of collection:" + collection.to_string());
    }
    itr->second.setHighSeqno(value);
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

    // Dump the scopes container, which only stores scope-identifiers, so that
    // we have a key and value we include the iteration index as the value.
    int i = 0;
    for (auto sid : scopes) {
        key.resize(0);

        format_to(key, "vb_{}:{}", vbid.get(), sid);
        collector.addStat(std::string_view(key.data(), key.size()), i++);
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
        summary[entry.cid] += itr->second.getStatsForSummary();
    }
}

std::optional<std::vector<CollectionID>> Manifest::getCollectionsForScope(
        ScopeID identifier) const {
    if (std::find(scopes.begin(), scopes.end(), identifier) == scopes.end()) {
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
    for (const auto& e : map) {
        const auto& entry1 = e.second;
        const auto& entry2 = rhs.getManifestEntry(e.first);

        if (!(entry1 == entry2)) {
            return false;
        }
    }

    if (scopes.size() != rhs.scopes.size()) {
        return false;
    }
    // Check all scopes can be found
    for (ScopeID sid : scopes) {
        if (std::find(rhs.scopes.begin(), rhs.scopes.end(), sid) ==
            rhs.scopes.end()) {
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
    for (auto& m : manifest.map) {
        os << "cid:" << m.first.to_string() << ":" << m.second << std::endl;
    }

    for (auto s : manifest.scopes) {
        os << "scope:" << s.to_string() << std::endl;
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
