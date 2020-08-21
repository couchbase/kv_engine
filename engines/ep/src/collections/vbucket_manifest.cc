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
#include "statistics/collector.h"
#include "systemevent.h"
#include "vbucket.h"

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

std::optional<CollectionID> Manifest::applyDeletions(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<CollectionID>& changes) {
    std::optional<CollectionID> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto id : changes) {
        dropCollection(
                wHandle, vb, manifestUid, id, OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

std::optional<Manifest::CollectionAddition> Manifest::applyCreates(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<CollectionAddition>& changes) {
    std::optional<CollectionAddition> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& addition : changes) {
        addCollection(wHandle,
                      vb,
                      manifestUid,
                      addition.identifiers,
                      addition.name,
                      addition.maxTtl,
                      OptionalSeqno{/*no-seqno*/});
    }

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
    for (const auto id : changes) {
        dropScope(wHandle, vb, manifestUid, id, OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

std::optional<Manifest::ScopeAddition> Manifest::applyScopeCreates(
        const WriteHandle& wHandle,
        ::VBucket& vb,
        std::vector<ScopeAddition>& changes) {
    std::optional<ScopeAddition> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& addition : changes) {
        addScope(wHandle,
                 vb,
                 manifestUid,
                 addition.sid,
                 addition.name,
                 OptionalSeqno{/*no-seqno*/});
    }

    return rv;
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
        return status;
    }

    auto changes = processManifest(manifest);

    // If manifest was equal. expect no changes
    if (manifest.getUid() == manifestUid) {
        if (changes.empty()) {
            return ManifestUpdateStatus::Success;
        } else {
            // Log verbosely for this case
            EP_LOG_WARN(
                    "Manifest::update with equal uid:{} but differences "
                    "scopes+:{}, collections+:{}, scopes-:{}, collections-:{}",
                    manifestUid,
                    changes.scopesToAdd.size(),
                    changes.collectionsToAdd.size(),
                    changes.scopesToRemove.size(),
                    changes.collectionsToRemove.size());
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

    auto finalScopeCreate = applyScopeCreates(wHandle, vb, changes.scopesToAdd);
    if (finalScopeCreate) {
        auto uid = changes.collectionsToAdd.empty() &&
                                   changes.collectionsToRemove.empty() &&
                                   changes.scopesToRemove.empty()
                           ? changes.uid
                           : manifestUid;
        addScope(wHandle,
                 vb,
                 uid,
                 finalScopeCreate.value().sid,
                 finalScopeCreate.value().name,
                 OptionalSeqno{/*no-seqno*/});
    }

    auto finalDeletion =
            applyDeletions(wHandle, vb, changes.collectionsToRemove);
    if (finalDeletion) {
        auto uid = changes.collectionsToAdd.empty() &&
                                   changes.scopesToRemove.empty()
                           ? changes.uid
                           : manifestUid;
        dropCollection(
                wHandle, vb, uid, *finalDeletion, OptionalSeqno{/*no-seqno*/});
    }

    auto finalAddition = applyCreates(wHandle, vb, changes.collectionsToAdd);

    if (finalAddition) {
        auto uid = changes.scopesToRemove.empty() ? changes.uid : manifestUid;
        addCollection(wHandle,
                      vb,
                      uid,
                      finalAddition.value().identifiers,
                      finalAddition.value().name,
                      finalAddition.value().maxTtl,
                      OptionalSeqno{/*no-seqno*/});
    }

    // This is done last so the scope deletion follows any collection
    // deletions
    auto finalScopeDrop = applyScopeDrops(wHandle, vb, changes.scopesToRemove);

    if (finalScopeDrop) {
        dropScope(wHandle,
                  vb,
                  changes.uid,
                  *finalScopeDrop,
                  OptionalSeqno{/*no-seqno*/});
    }
}

ManifestUpdateStatus Manifest::canUpdate(
        const Collections::Manifest& manifest) const {
    // Cannot go backwards
    if (manifest.getUid() < manifestUid) {
        return ManifestUpdateStatus::Behind;
    }
    return ManifestUpdateStatus::Success;
}

void Manifest::addCollection(const WriteHandle& wHandle,
                             ::VBucket& vb,
                             ManifestUid manifestUid,
                             ScopeCollectionPair identifiers,
                             std::string_view collectionName,
                             cb::ExpiryLimit maxTtl,
                             OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the collections
    // map. Specify a non-zero start and 0 for the TTL
    auto& entry = addNewCollectionEntry(identifiers, maxTtl);

    // 1.1 record the uid of the manifest which is adding the collection
    this->manifestUid = manifestUid;

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueCollectionSystemEvent(wHandle,
                                            vb,
                                            identifiers.second,
                                            collectionName,
                                            entry,
                                            false,
                                            optionalSeqno);

    EP_LOG_INFO(
            "collections: {} adding collection:[name:{},id:{:#x}] to "
            "scope:{:#x}, "
            "maxTTL:{} {}, "
            "replica:{}, seqno:{}, manifest:{:#x}",
            vb.getId(),
            collectionName,
            identifiers.second,
            identifiers.first,
            maxTtl.has_value(),
            maxTtl.value_or(std::chrono::seconds::zero()).count(),
            optionalSeqno.has_value(),
            seqno,
            manifestUid);

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

void Manifest::dropCollection(const WriteHandle& wHandle,
                              ::VBucket& vb,
                              ManifestUid manifestUid,
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
        addNewCollectionEntry({ScopeID::Default, cid}, {});
    }

    auto itr = map.find(cid);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "did not find collection:" + cid.to_string());
    }

    // record the uid of the manifest which removed the collection
    this->manifestUid = manifestUid;

    auto seqno = queueCollectionSystemEvent(wHandle,
                                            vb,
                                            cid,
                                            {/*no name*/},
                                            itr->second,
                                            true /*delete*/,
                                            optionalSeqno);

    EP_LOG_INFO(
            "collections: {} drop of collection:{:#x} from scope:{:#x}"
            ", replica:{}, seqno:{}, manifest:{:#x} tombstone:{}",
            vb.getId(),
            cid,
            itr->second.getScopeID(),
            optionalSeqno.has_value(),
            seqno,
            manifestUid,
            processingTombstone);

    map.erase(itr);
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

void Manifest::addScope(const WriteHandle& wHandle,
                        ::VBucket& vb,
                        ManifestUid manifestUid,
                        ScopeID sid,
                        std::string_view scopeName,
                        OptionalSeqno optionalSeqno) {
    if (isScopeValid(sid)) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope already exists, scope:" + sid.to_string());
    }

    scopes.insert(sid);

    // record the uid of the manifest which added the scope
    this->manifestUid = manifestUid;

    flatbuffers::FlatBufferBuilder builder;
    auto scope = CreateScope(
            builder,
            getManifestUid(),
            sid,
            builder.CreateString(scopeName.data(), scopeName.size()));
    builder.Finish(scope);

    auto item = SystemEventFactory::makeScopeEvent(
            sid,
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    auto seqno =
            vb.addSystemEventItem(item.release(), optionalSeqno, {}, wHandle);

    EP_LOG_INFO(
            "collections: {} added scope:name:{},id:{:#x} "
            "replica:{}, seqno:{}, manifest:{:#x}",
            vb.getId(),
            scopeName,
            sid,
            optionalSeqno.has_value(),
            seqno,
            manifestUid);
}

void Manifest::dropScope(const WriteHandle& wHandle,
                         ::VBucket& vb,
                         ManifestUid manifestUid,
                         ScopeID sid,
                         OptionalSeqno optionalSeqno) {
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
    this->manifestUid = manifestUid;

    flatbuffers::FlatBufferBuilder builder;
    auto scope = CreateDroppedScope(builder, getManifestUid(), sid);
    builder.Finish(scope);

    auto item = SystemEventFactory::makeScopeEvent(
            sid,
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    item->setDeleted();

    auto seqno =
            vb.addSystemEventItem(item.release(), optionalSeqno, {}, wHandle);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!optionalSeqno.has_value()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    EP_LOG_INFO(
            "collections: {} dropped scope:id:{:#x} "
            "replica:{}, seqno:{}, manifest:{:#x}",
            vb.getId(),
            sid,
            optionalSeqno.has_value(),
            seqno,
            manifestUid);
}

Manifest::ManifestChanges Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    ManifestChanges rv{manifest.getUid()};

    for (const auto& entry : map) {
        // If the entry is not found in the new manifest it must be
        // deleted.
        if (manifest.findCollection(entry.first) == manifest.end()) {
            rv.collectionsToRemove.push_back(entry.first);
        }
    }

    for (const auto scope : scopes) {
        // Remove the scopes that don't exist in the new manifest
        if (manifest.findScope(scope) == manifest.endScopes()) {
            rv.scopesToRemove.push_back(scope);
        }
    }

    // Add scopes and collections in Manifest but not in our map
    for (auto scopeItr = manifest.beginScopes();
         scopeItr != manifest.endScopes();
         scopeItr++) {
        if (std::find(scopes.begin(), scopes.end(), scopeItr->first) ==
            scopes.end()) {
            rv.scopesToAdd.push_back({scopeItr->first, scopeItr->second.name});
        }

        for (const auto& m : scopeItr->second.collections) {
            auto mapItr = map.find(m.id);

            if (mapItr == map.end()) {
                rv.collectionsToAdd.push_back(
                        {std::make_pair(scopeItr->first, m.id),
                         manifest.findCollection(m.id)->second.name,
                         m.maxTtl});
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
    return seqno >= 0 && seqno < entry->second.getStartSeqno();
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
                entry.getScopeID(),
                cid,
                entry.getMaxTtl().has_value(),
                entry.getMaxTtl().has_value() ? (*entry.getMaxTtl()).count()
                                              : 0,
                builder.CreateString(collectionName.data(),
                                     collectionName.size()));
        builder.Finish(collection);
    } else {
        auto collection =
                CreateDroppedCollection(builder, uid, entry.getScopeID(), cid);
        builder.Finish(collection);
    }

    auto item = SystemEventFactory::makeCollectionEvent(
            cid, {builder.GetBufferPointer(), builder.GetSize()}, seq);

    if (deleted) {
        item->setDeleted();
    }
    return item;
}

int64_t Manifest::queueCollectionSystemEvent(const WriteHandle& wHandle,
                                             ::VBucket& vb,
                                             CollectionID cid,
                                             std::string_view collectionName,
                                             const ManifestEntry& entry,
                                             bool deleted,
                                             OptionalSeqno seq) const {
    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!seq.has_value()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    auto item = makeCollectionSystemEvent(
            getManifestUid(), cid, collectionName, entry, deleted, seq);

    // We can never purge the drop of the default collection because it has an
    // implied creation event. If we did allow the default collection tombstone
    // to be purged a client would wrongly assume it exists.
    if (!seq.has_value() && deleted && cid.isDefaultCollection()) {
        item->setExpTime(~0);
    }

    // Create and transfer Item ownership to the VBucket
    auto rv = vb.addSystemEventItem(item.release(), seq, cid, wHandle);

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
    return rv;
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
    // For now link through to disk count
    // @todo: ephemeral support
    return itr->second.getDiskCount();
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

void Manifest::incrementDiskCount(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.incrementDiskCount();
}

void Manifest::decrementDiskCount(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.decrementDiskCount();
}

bool Manifest::addCollectionStats(Vbid vbid,
                                  const void* cookie,
                                  const AddStatFn& add_stat) const {
    try {
        const int bsize = 512;
        char buffer[bsize];
        checked_snprintf(buffer, bsize, "vb_%d:collections", vbid.get());
        add_casted_stat(buffer, map.size(), add_stat, cookie);
        checked_snprintf(buffer, bsize, "vb_%d:manifest:uid", vbid.get());
        add_casted_stat(buffer, manifestUid, add_stat, cookie);
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "VB::Manifest::addCollectionStats {}, failed to build stats "
                "exception:{}",
                vbid,
                e.what());
        return false;
    }
    for (const auto& entry : map) {
        if (!entry.second.addStats(
                    entry.first.to_string(), vbid, cookie, add_stat)) {
            return false;
        }
    }
    return true;
}

bool Manifest::addScopeStats(Vbid vbid,
                             const void* cookie,
                             const AddStatFn& add_stat) const {
    const int bsize = 512;
    char buffer[bsize];
    try {
        checked_snprintf(buffer, bsize, "vb_%d:scopes", vbid.get());
        add_casted_stat(buffer, scopes.size(), add_stat, cookie);
        checked_snprintf(buffer, bsize, "vb_%d:manifest_uid", vbid.get());
        add_casted_stat(buffer, manifestUid, add_stat, cookie);
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "VB::Manifest::addScopeStats {}, failed to build stats "
                "exception:{}",
                vbid,
                e.what());
        return false;
    }

    // Dump the scopes container, which only stores scope-identifiers, so that
    // we have a key and value we include the iteration index as the value.
    int i = 0;
    for (auto sid : scopes) {
        checked_snprintf(
                buffer, bsize, "vb_%d:%s", vbid.get(), sid.to_string().c_str());
        add_casted_stat(buffer, i++, add_stat, cookie);
    }

    // Dump all collections and how they map to a scope. Stats requires unique
    // keys, so cannot use the cid as the value (as key won't be unique), so
    // return anything from the entry for the value, we choose item count.
    for (const auto& [cid, entry] : map) {
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:%s:%s:items",
                         vbid.get(),
                         entry.getScopeID().to_string().c_str(),
                         cid.to_string().c_str());
        add_casted_stat(buffer, entry.getDiskCount(), add_stat, cookie);
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
    for (const auto s : scopes) {
        if (std::find(rhs.scopes.begin(), rhs.scopes.end(), s) ==
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
