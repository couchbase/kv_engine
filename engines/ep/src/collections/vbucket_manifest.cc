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

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/manifest.h"
#include "collections/vbucket_serialised_manifest_entry_generated.h"
#include "ep_time.h"
#include "item.h"
#include "statwriter.h"
#include "vbucket.h"

#include <mcbp/protocol/unsigned_leb128.h>

#include <memory>

namespace Collections {
namespace VB {

Manifest::Manifest(const PersistedManifest& data)
    : defaultCollectionExists(false),
      greatestEndSeqno(StoredValue::state_collection_open),
      nDeletingCollections(0) {
    if (data.empty()) {
        // Empty manifest, initialise the manifest with the default collection
        addNewCollectionEntry({ScopeID::Default, CollectionID::Default},
                              {/*no max_ttl*/},
                              0,
                              StoredValue::state_collection_open);
        defaultCollectionExists = true;

        // The default scope always exists
        scopes.push_back(ScopeID::Default);
        return;
    }

    {
        flatbuffers::Verifier v(data.data(), data.size());
        if (!VerifySerialisedManifestBuffer(v)) {
            throwException<std::invalid_argument>(
                    __FUNCTION__, "FlatBuffer validation failed.");
        }
    }

    auto manifest = flatbuffers::GetRoot<SerialisedManifest>(
            reinterpret_cast<const uint8_t*>(data.data()));

    manifestUid = manifest->uid();

    // Loading the scopes from PersistedManifest may encounter the Default scope
    // twice, see comments in patchSerialisedDataForScopeEvent.
    // We want to add it once.
    bool addedDefault = false;
    for (const auto& sid : *manifest->scopes()) {
        if (sid == ScopeID::Default) {
            if (addedDefault) {
                continue;
            } else {
                addedDefault = true;
            }
        }
        scopes.push_back(sid);
    }

    for (const auto& entry : *manifest->entries()) {
        cb::ExpiryLimit maxTtl;
        if (entry->ttlValid()) {
            maxTtl = std::chrono::seconds(entry->maxTtl());
        }
        addNewCollectionEntry({entry->scopeId(), entry->collectionId()},
                              maxTtl,
                              entry->startSeqno(),
                              entry->endSeqno());
    }
}

boost::optional<CollectionID> Manifest::applyDeletions(
        ::VBucket& vb, std::vector<CollectionID>& changes) {
    boost::optional<CollectionID> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto id : changes) {
        beginCollectionDelete(vb, manifestUid, id, OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

boost::optional<Manifest::CollectionAddition> Manifest::applyCreates(
        ::VBucket& vb, std::vector<CollectionAddition>& changes) {
    boost::optional<CollectionAddition> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& addition : changes) {
        addCollection(vb,
                      manifestUid,
                      addition.identifiers,
                      addition.name,
                      addition.maxTtl,
                      OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

boost::optional<ScopeID> Manifest::applyScopeDrops(
        ::VBucket& vb, std::vector<ScopeID>& changes) {
    boost::optional<ScopeID> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto id : changes) {
        dropScope(vb, manifestUid, id, OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

boost::optional<Manifest::ScopeAddition> Manifest::applyScopeCreates(
        ::VBucket& vb, std::vector<ScopeAddition>& changes) {
    boost::optional<ScopeAddition> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& addition : changes) {
        addScope(vb,
                 manifestUid,
                 addition.sid,
                 addition.name,
                 OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}

bool Manifest::update(::VBucket& vb, const Collections::Manifest& manifest) {
    auto rv = processManifest(manifest);
    if (!rv.is_initialized()) {
        EP_LOG_WARN("VB::Manifest::update cannot update {}", vb.getId());
        return false;
    } else {
        auto finalScopeCreate = applyScopeCreates(vb, rv->scopesToAdd);
        if (finalScopeCreate) {
            auto uid = rv->collectionsToAdd.empty() &&
                                       rv->collectionsToRemove.empty() &&
                                       rv->scopesToRemove.empty()
                               ? manifest.getUid()
                               : manifestUid;
            addScope(vb,
                     uid,
                     finalScopeCreate.get().sid,
                     finalScopeCreate.get().name,
                     OptionalSeqno{/*no-seqno*/});
        }

        auto finalDeletion = applyDeletions(vb, rv->collectionsToRemove);
        if (finalDeletion) {
            auto uid =
                    rv->collectionsToAdd.empty() && rv->scopesToRemove.empty()
                            ? manifest.getUid()
                            : manifestUid;
            beginCollectionDelete(
                    vb, uid, *finalDeletion, OptionalSeqno{/*no-seqno*/});
        }

        auto finalAddition = applyCreates(vb, rv->collectionsToAdd);

        if (finalAddition) {
            auto uid = rv->scopesToRemove.empty() ? manifest.getUid()
                                                  : manifestUid;
            addCollection(vb,
                          uid,
                          finalAddition.get().identifiers,
                          finalAddition.get().name,
                          finalAddition.get().maxTtl,
                          OptionalSeqno{/*no-seqno*/});
        }

        // This is done last so the scope deletion follows any collection
        // deletions
        auto finalScopeDrop = applyScopeDrops(vb, rv->scopesToRemove);

        if (finalScopeDrop) {
            dropScope(vb,
                      manifest.getUid(),
                      *finalScopeDrop,
                      OptionalSeqno{/*no-seqno*/});
        }
    }
    return true;
}

void Manifest::addCollection(::VBucket& vb,
                             ManifestUid manifestUid,
                             ScopeCollectionPair identifiers,
                             cb::const_char_buffer collectionName,
                             cb::ExpiryLimit maxTtl,
                             OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the collections
    // map. Specify a non-zero start and 0 for the TTL
    auto& entry = addNewCollectionEntry(identifiers, maxTtl);

    // 1.1 record the uid of the manifest which is adding the collection
    this->manifestUid = manifestUid;

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  identifiers,
                                  collectionName,
                                  false /*deleted*/,
                                  optionalSeqno);

    EP_LOG_INFO(
            "collections: {} adding collection:[name:{},id:{:x}] to "
            "scope:{:x}, "
            "max_ttl:{} {}, "
            "replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            cb::to_string(collectionName),
            identifiers.second,
            identifiers.first,
            maxTtl.is_initialized(),
            maxTtl ? maxTtl.get().count() : 0,
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);

    // 3. Now patch the entry with the seqno of the system event, note the copy
    //    of the manifest taken at step 1 gets the correct seqno when the system
    //    event is flushed.
    entry.setStartSeqno(seqno);
}

ManifestEntry& Manifest::addNewCollectionEntry(ScopeCollectionPair identifiers,
                                               cb::ExpiryLimit maxTtl,
                                               int64_t startSeqno,
                                               int64_t endSeqno) {
    // This method is only for when the map does not have the collection
    auto itr = map.find(identifiers.second);
    if (itr != map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "collection already exists + "
                ", collection:" +
                        identifiers.second.to_string() +
                        ", scope:" + identifiers.first.to_string() +
                        ", startSeqno:" + std::to_string(startSeqno) +
                        ", endSeqno:" + std::to_string(endSeqno));
    }

    auto inserted = map.emplace(
            identifiers.second,
            ManifestEntry(identifiers.first, maxTtl, startSeqno, endSeqno));

    if (identifiers.second.isDefaultCollection() && inserted.second) {
        defaultCollectionExists = (*inserted.first).second.isOpen();
    }

    // Did we insert a deleting collection (can happen if restoring from
    // persisted manifest)
    if ((*inserted.first).second.isDeleting()) {
        trackEndSeqno(endSeqno);
    }

    return (*inserted.first).second;
}

void Manifest::beginCollectionDelete(::VBucket& vb,
                                     ManifestUid manifestUid,
                                     CollectionID cid,
                                     OptionalSeqno optionalSeqno) {
    bool processingTombstone = false;
    // A replica that receives a collection tombstone is required to persist
    // that tombstone, so the replica can switch to active consistently
    if (optionalSeqno.is_initialized() && map.count(cid) == 0) {
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

    auto& entry = getManifestEntry(cid);

    // record the uid of the manifest which removed the collection
    this->manifestUid = manifestUid;

    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  {entry.getScopeID(), cid},
                                  {/* no name*/},
                                  true /*deleted*/,
                                  optionalSeqno);

    EP_LOG_INFO(
            "collections: {} begin delete of collection:{:x} from scope:{:x}"
            ", replica:{}, backfill:{}, seqno:{}, manifest:{:x} tombstone:{}",
            vb.getId(),
            cid,
            entry.getScopeID(),
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid,
            processingTombstone);

    if (cid.isDefaultCollection()) {
        defaultCollectionExists = false;
    }

    entry.setEndSeqno(seqno);

    trackEndSeqno(seqno);

    if (processingTombstone) {
        // If this is a tombstone, we can now remove the entry from the manifest
        completeDeletion(vb, cid);
    }
}

ManifestEntry& Manifest::getManifestEntry(CollectionID identifier) {
    auto itr = map.find(identifier);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "did not find collection:" + identifier.to_string());
    }

    return itr->second;
}

void Manifest::completeDeletion(::VBucket& vb, CollectionID collectionID) {
    auto itr = map.find(collectionID);

    EP_LOG_INFO("collections: {} complete delete of collection:{:x}",
                vb.getId(),
                collectionID);
    // Caller should not be calling in if the collection doesn't exist
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "could not find collection:" + collectionID.to_string());
    }

    map.erase(itr);

    nDeletingCollections--;
    if (nDeletingCollections == 0) {
        greatestEndSeqno = StoredValue::state_collection_open;
    }
}

void Manifest::addScope(::VBucket& vb,
                        ManifestUid manifestUid,
                        ScopeID sid,
                        cb::const_char_buffer scopeName,
                        OptionalSeqno optionalSeqno) {
    if (std::find(scopes.begin(), scopes.end(), sid) != scopes.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope already exists, scope:" + sid.to_string());
    }

    scopes.push_back(sid);

    // record the uid of the manifest which added the scope
    this->manifestUid = manifestUid;

    flatbuffers::FlatBufferBuilder builder;
    populateWithSerialisedData(builder, scopeName);

    auto item = SystemEventFactory::make(
            SystemEvent::Scope,
            makeScopeIdIntoString(sid),
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    auto seqno = vb.addSystemEventItem(item.release(), optionalSeqno);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!optionalSeqno.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    EP_LOG_INFO(
            "collections: {} added scope:name:{},id:{:x} "
            "replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            cb::to_string(scopeName),
            sid,
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);
}

void Manifest::dropScope(::VBucket& vb,
                         ManifestUid manifestUid,
                         ScopeID sid,
                         OptionalSeqno optionalSeqno) {
    // A replica receiving a dropScope for a scope is allowed, i.e. if we are
    // creating a new replica, we will see scope tombstones from the active.
    // The replica use-case is assumed by the optionalSeqno being defined
    if (!optionalSeqno &&
        std::find(scopes.begin(), scopes.end(), sid) == scopes.end()) {
        throwException<std::logic_error>(
                __FUNCTION__, "scope doesn't exist, scope:" + sid.to_string());
    }

    // The sid must be the back() element for populateWithSerialisedData
    // 1) In the 'active' usage, the sid already exists in the set, but we
    //    require it to be the last element when iterating
    //    (populateWithSerialisedData), remove/push_back is a move to back
    // 2) In the replica usage, the sid may not exist, but we can be told to
    //    drop a scope because we see a scope tombstone. So we must add the
    //    scope so that we generate a system-event which represents what we are
    //    being told. In this case the remove does nothing and we push the new
    //    sid to the back
    scopes.remove(sid);
    scopes.push_back(sid);

    // record the uid of the manifest which removed the scope
    this->manifestUid = manifestUid;

    flatbuffers::FlatBufferBuilder builder;
    populateWithSerialisedData(builder, {});

    // And remove
    scopes.pop_back();

    auto item = SystemEventFactory::make(
            SystemEvent::Scope,
            makeScopeIdIntoString(sid),
            {builder.GetBufferPointer(), builder.GetSize()},
            optionalSeqno);

    item->setDeleted();

    auto seqno = vb.addSystemEventItem(item.release(), optionalSeqno);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!optionalSeqno.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }

    EP_LOG_INFO(
            "collections: {} dropped scope:id:{:x} "
            "replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            sid,
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);
}

Manifest::ProcessResult Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    ProcessResult rv = ManifestChanges();

    for (const auto& entry : map) {
        // If the entry is open and not found in the new manifest it must be
        // deleted.
        if (entry.second.isOpen() &&
            manifest.findCollection(entry.first) == manifest.end()) {
            rv->collectionsToRemove.push_back(entry.first);
        }
    }

    for (const auto scope : scopes) {
        // Remove the scopes that don't exist in the new manifest
        if (manifest.findScope(scope) == manifest.endScopes()) {
            rv->scopesToRemove.push_back(scope);
        }
    }

    // Add scopes and collections in Manifest but not in our map
    for (auto scopeItr = manifest.beginScopes();
         scopeItr != manifest.endScopes();
         scopeItr++) {
        if (std::find(scopes.begin(), scopes.end(), scopeItr->first) ==
            scopes.end()) {
            rv->scopesToAdd.push_back({scopeItr->first, scopeItr->second.name});
        }

        for (const auto& m : scopeItr->second.collections) {
            auto mapItr = map.find(m.id);

            if (mapItr == map.end()) {
                rv->collectionsToAdd.push_back(
                        {std::make_pair(scopeItr->first, m.id),
                         manifest.findCollection(m.id)->second,
                         m.maxTtl});
            } else if (mapItr->second.isDeleting()) {
                // trying to add a collection which is deleting, not allowed.
                EP_LOG_WARN("Attempt to add a deleting collection:{}:{:x}",
                            manifest.findCollection(m.id)->second,
                            m.id);
                return {};
            }
        }
    }

    return rv;
}

bool Manifest::doesKeyContainValidCollection(const DocKey& key) const {
    if (defaultCollectionExists &&
        key.getCollectionID().isDefaultCollection()) {
        return true;
    } else {
        auto itr = map.find(key.getCollectionID());
        if (itr != map.end()) {
            return itr->second.isOpen();
        }
    }
    return false;
}

bool Manifest::doesKeyBelongToScope(const DocKey& key, ScopeID scopeID) const {
    auto itr = map.find(key.getCollectionID());
    if (itr != map.end()) {
        return itr->second.getScopeID() == scopeID;
    }
    return false;
}

Manifest::container::const_iterator Manifest::getManifestEntry(
        const DocKey& key, bool allowSystem) const {
    CollectionID lookup = key.getCollectionID();
    if (allowSystem && lookup == CollectionID::System) {
        lookup = getCollectionIDFromKey(key);
    } // else we lookup with CID which if is System => fail
    return map.find(lookup);
}

bool Manifest::isLogicallyDeleted(const DocKey& key, int64_t seqno) const {
    // Only do the searching/scanning work for keys in the deleted range.
    if (seqno <= greatestEndSeqno) {
        if (key.getCollectionID().isDefaultCollection()) {
            return !defaultCollectionExists;
        } else {
            CollectionID lookup = key.getCollectionID();
            if (lookup == CollectionID::System) {
                lookup = getCollectionIDFromKey(key);
            }
            auto itr = map.find(lookup);
            if (itr != map.end()) {
                return seqno <= itr->second.getEndSeqno();
            }
        }
    }
    return false;
}

bool Manifest::isLogicallyDeleted(const container::const_iterator entry,
                                  int64_t seqno) const {
    if (entry == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "iterator is invalid, seqno:" + std::to_string(seqno));
    }

    if (seqno <= greatestEndSeqno) {
        return seqno <= entry->second.getEndSeqno();
    }
    return false;
}

boost::optional<CollectionID> Manifest::shouldCompleteDeletion(
        const DocKey& key,
        int64_t bySeqno,
        const container::const_iterator entry) const {
    // If this is a SystemEvent key then...
    if (key.getCollectionID() == CollectionID::System) {
        if (entry->second.isDeleting()) {
            return entry->first; // returning CID
        }
    }
    return {};
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
        enforcedTtl = entry->second.getMaxTtl().get();
    }

    // Note: A ttl value of 0 means no max_ttl
    if (enforcedTtl.count()) {
        t = ep_limit_abstime(t, enforcedTtl);
    }
    return t;
}

std::string Manifest::makeCollectionIdIntoString(CollectionID collection) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(collection);
    return std::string(reinterpret_cast<const char*>(leb128.data()),
                       leb128.size());
}

std::string Manifest::makeScopeIdIntoString(ScopeID sid) {
    cb::mcbp::unsigned_leb128<ScopeIDType> leb128(sid);
    return std::string(reinterpret_cast<const char*>(leb128.data()),
                       leb128.size());
}

CollectionID Manifest::getCollectionIDFromKey(const DocKey& key) {
    if (key.getCollectionID() != CollectionID::System) {
        throw std::invalid_argument("getCollectionIDFromKey: non-system key");
    }
    return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
                   SystemEventFactory::getKeyExtra(key))
            .first;
}

std::unique_ptr<Item> Manifest::createSystemEvent(
        SystemEvent se,
        ScopeCollectionPair identifiers,
        cb::const_char_buffer collectionName,
        bool deleted,
        OptionalSeqno seqno) const {
    // Create an item (to be queued and written to disk) that represents
    // the update of a collection and allows the checkpoint to update
    // the _local document with a persisted version of this object (the entire
    // manifest is persisted to disk as flatbuffer data).
    flatbuffers::FlatBufferBuilder builder;
    populateWithSerialisedData(builder, identifiers, collectionName);
    auto item = SystemEventFactory::make(
            se,
            makeCollectionIdIntoString(identifiers.second),
            {builder.GetBufferPointer(), builder.GetSize()},
            seqno);

    if (deleted) {
        item->setDeleted();
    }

    return item;
}

int64_t Manifest::queueSystemEvent(::VBucket& vb,
                                   SystemEvent se,
                                   ScopeCollectionPair identifiers,
                                   cb::const_char_buffer collectionName,
                                   bool deleted,
                                   OptionalSeqno seq) const {
    // Create and transfer Item ownership to the VBucket
    auto rv = vb.addSystemEventItem(
            createSystemEvent(se, identifiers, collectionName, deleted, seq)
                    .release(),
            seq);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!seq.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }
    return rv;
}

void Manifest::populateWithSerialisedData(
        flatbuffers::FlatBufferBuilder& builder,
        ScopeCollectionPair identifiers,
        cb::const_char_buffer collectionName) const {
    const ManifestEntry* finalEntry = nullptr;

    std::vector<flatbuffers::Offset<SerialisedManifestEntry>> entriesVector;

    for (const auto& collectionEntry : map) {
        // Check if we find the mutated entry in the map (so we know if we're
        // mutating it)
        if (collectionEntry.first == identifiers.second) {
            // If a collection in the map matches the collection being changed
            // save the iterator so we can use it when creating the final entry
            finalEntry = &collectionEntry.second;
        } else {
            uint32_t maxTtl = 0;
            if (collectionEntry.second.getMaxTtl()) {
                maxTtl = collectionEntry.second.getMaxTtl().get().count();
            }
            auto newEntry = CreateSerialisedManifestEntry(
                    builder,
                    collectionEntry.second.getStartSeqno(),
                    collectionEntry.second.getEndSeqno(),
                    collectionEntry.second.getScopeID(),
                    collectionEntry.first,
                    collectionEntry.second.getMaxTtl().is_initialized(),
                    maxTtl);
            entriesVector.push_back(newEntry);
        }
    }

    // Note that patchSerialisedData will change one of these values when the
    // real seqno is known.
    int64_t startSeqno = StoredValue::state_collection_open;
    int64_t endSeqno = StoredValue::state_collection_open;
    uint32_t maxTtl = 0;
    bool maxTtlValid = false;
    if (finalEntry) {
        startSeqno = finalEntry->getStartSeqno();
        endSeqno = finalEntry->getEndSeqno();
        if (finalEntry->getMaxTtl()) {
            maxTtl = finalEntry->getMaxTtl().get().count();
            maxTtlValid = true;
        }
    }

    auto newEntry = CreateSerialisedManifestEntry(builder,
                                                  startSeqno,
                                                  endSeqno,
                                                  identifiers.first,
                                                  identifiers.second,
                                                  maxTtlValid,
                                                  maxTtl);

    entriesVector.push_back(newEntry);
    auto entries = builder.CreateVector(entriesVector);

    std::vector<uint32_t> scopeVector;
    for (auto sid : scopes) {
        scopeVector.push_back(uint32_t(sid));
    }

    auto scopeEntries = builder.CreateVector(scopeVector);

    if (collectionName.data()) {
        auto manifest = CreateSerialisedManifest(
                builder,
                getManifestUid(),
                entries,
                scopeEntries,
                builder.CreateString(collectionName.data(),
                                     collectionName.size()));
        builder.Finish(manifest);
    } else {
        auto manifest = CreateSerialisedManifest(builder,
                                                 getManifestUid(),
                                                 entries,
                                                 scopeEntries,
                                                 builder.CreateString(""));

        builder.Finish(manifest);
    }
}

void Manifest::populateWithSerialisedData(
        flatbuffers::FlatBufferBuilder& builder,
        cb::const_char_buffer mutatedName) const {
    std::vector<flatbuffers::Offset<SerialisedManifestEntry>> entriesVector;

    for (const auto& collectionEntry : map) {
        auto newEntry = CreateSerialisedManifestEntry(
                builder,
                collectionEntry.second.getStartSeqno(),
                collectionEntry.second.getEndSeqno(),
                collectionEntry.second.getScopeID(),
                collectionEntry.first);
        entriesVector.push_back(newEntry);
    }

    auto entries = builder.CreateVector(entriesVector);

    std::vector<uint32_t> scopeVector;
    for (auto sid : scopes) {
        scopeVector.push_back(sid);
    }

    auto scopeEntries = builder.CreateVector(scopeVector);

    if (mutatedName.data()) {
        auto manifest = CreateSerialisedManifest(
                builder,
                getManifestUid(),
                entries,
                scopeEntries,
                builder.CreateString(mutatedName.data(), mutatedName.size()));
        builder.Finish(manifest);
    } else {
        auto manifest = CreateSerialisedManifest(builder,
                                                 getManifestUid(),
                                                 entries,
                                                 scopeEntries,
                                                 builder.CreateString(""));

        builder.Finish(manifest);
    }
}

PersistedManifest Manifest::getPersistedManifest(const Item& item) {
    switch (SystemEvent(item.getFlags())) {
    case SystemEvent::Collection:
        // Collection events need the endSeqno updating
        return patchSerialisedDataForCollectionEvent(item);
    case SystemEvent::Scope: {
        return patchSerialisedDataForScopeEvent(item);
    }
    default:
        throw std::invalid_argument(
                "Manifest::getManifestData: unknown event:" +
                std::to_string(int(item.getFlags())));
    }
}

PersistedManifest Manifest::patchSerialisedDataForCollectionEvent(
        const Item& item) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(item.getData());
    PersistedManifest mutableData(ptr, ptr + item.getNBytes());
    auto manifest =
            flatbuffers::GetMutableRoot<SerialisedManifest>(mutableData.data());

    // Get the last entry from entries, that is the entry to patch
    auto mutatedEntry = manifest->mutable_entries()->GetMutableObject(
            manifest->entries()->size() - 1);

    bool failed = false;
    if (item.isDeleted()) {
        failed = !mutatedEntry->mutate_endSeqno(item.getBySeqno());
    } else {
        failed = !mutatedEntry->mutate_startSeqno(item.getBySeqno());
    }

    if (failed) {
        throw std::logic_error(
                "Manifest::patchSerialisedDataForCollectionEvent failed to "
                "mutate, new seqno: " +
                std::to_string(item.getBySeqno()) +
                " isDeleted:" + std::to_string(item.isDeleted()));
    }

    return mutableData;
}

PersistedManifest Manifest::patchSerialisedDataForScopeEvent(const Item& item) {
    if (!item.isDeleted()) {
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(item.getData());
        return {ptr, ptr + item.getNBytes()};
    }

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(item.getData());
    PersistedManifest mutableData(ptr, ptr + item.getNBytes());
    auto manifest =
            flatbuffers::GetMutableRoot<SerialisedManifest>(mutableData.data());

    // Dropping a scope sets the last scope to ScopeID::Default. Why?
    // The flatbuffers scopes Vector contains as the last element, the ID of the
    // dropped scope.  The PersistedManifest we want to store (and may roll
    // back to) must not contain the dropped scope. However flatbuffers
    // scopes Vector cannot be resized, we cannot just pop_back the last
    // element. To remove the dropped id we are overwriting it with
    // ScopeID::Default. If a VB::Manifest is ever constructed from this
    // flatbuffers data it can cope with the double entry of ScopeID::Default
    // but importantly it can never bring back the dropped scope.
    manifest->mutable_scopes()->Mutate(manifest->scopes()->size() - 1,
                                       ScopeID::Default);

    return mutableData;
}

void Manifest::trackEndSeqno(int64_t seqno) {
    nDeletingCollections++;
    if (seqno > greatestEndSeqno ||
        greatestEndSeqno == StoredValue::state_collection_open) {
        greatestEndSeqno = seqno;
    }
}

CreateEventData Manifest::getCreateEventData(
        cb::const_char_buffer serialisedManifest) {
    auto manifest = flatbuffers::GetRoot<SerialisedManifest>(
            (const uint8_t*)serialisedManifest.data());

    auto mutatedEntry =
            manifest->entries()->Get(manifest->entries()->size() - 1);

    // if maxTtlValid needs considering
    cb::ExpiryLimit maxTtl;
    if (mutatedEntry->ttlValid()) {
        maxTtl = std::chrono::seconds(mutatedEntry->maxTtl());
    }
    return {manifest->uid(),
            mutatedEntry->scopeId(),
            mutatedEntry->collectionId(),
            manifest->mutatedName()->str(),
            maxTtl};
}

DropEventData Manifest::getDropEventData(
        cb::const_char_buffer serialisedManifest) {
    auto manifest = flatbuffers::GetRoot<SerialisedManifest>(
            (const uint8_t*)serialisedManifest.data());

    auto mutatedEntry =
            manifest->entries()->Get(manifest->entries()->size() - 1);

    return {manifest->uid(),
            mutatedEntry->scopeId(),
            mutatedEntry->collectionId()};
}

CreateScopeEventData Manifest::getCreateScopeEventData(
        cb::const_char_buffer serialisedManifest) {
    auto manifest = flatbuffers::GetRoot<SerialisedManifest>(
            (const uint8_t*)serialisedManifest.data());

    // The last entry in scopes is the ID added
    auto scopeId = manifest->scopes()->Get(manifest->scopes()->size() - 1);

    return {manifest->uid(), scopeId, manifest->mutatedName()->str()};
}

DropScopeEventData Manifest::getDropScopeEventData(
        cb::const_char_buffer serialisedManifest) {
    auto manifest = flatbuffers::GetRoot<SerialisedManifest>(
            (const uint8_t*)serialisedManifest.data());

    // The last entry in scopes is the ID dropped
    auto scopeId = manifest->scopes()->Get(manifest->scopes()->size() - 1);

    return {manifest->uid(), scopeId};
}

std::string Manifest::getExceptionString(const std::string& thrower,
                                         const std::string& error) const {
    std::stringstream ss;
    ss << "VB::Manifest:" << thrower << ": " << error << ", this:" << *this;
    return ss.str();
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

uint64_t Manifest::getPersistedHighSeqno(CollectionID collection) const {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "failed find of collection:" + collection.to_string());
    }
    return itr->second.getPersistedHighSeqno();
}

bool Manifest::addCollectionStats(Vbid vbid,
                                  const void* cookie,
                                  ADD_STAT add_stat) const {
    try {
        const int bsize = 512;
        char buffer[bsize];
        checked_snprintf(buffer, bsize, "vb_%d:manifest:entries", vbid.get());
        add_casted_stat(buffer, map.size(), add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:manifest:default_exists", vbid.get());
        add_casted_stat(buffer, defaultCollectionExists, add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:manifest:greatest_end", vbid.get());
        add_casted_stat(buffer, greatestEndSeqno, add_stat, cookie);
        checked_snprintf(
                buffer, bsize, "vb_%d:manifest:n_deleting", vbid.get());
        add_casted_stat(buffer, nDeletingCollections, add_stat, cookie);
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
                             ADD_STAT add_stat) const {
    const int bsize = 512;
    char buffer[bsize];
    try {
        checked_snprintf(buffer, bsize, "vb_%d:manifest:scopes", vbid.get());
        add_casted_stat(buffer, scopes.size(), add_stat, cookie);
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "VB::Manifest::addScopeStats {}, failed to build stats "
                "exception:{}",
                vbid,
                e.what());
        return false;
    }

    // We'll also print the iteration index of each scope and collection.
    // This is particularly useful for scopes as the ordering of the
    // container matters when we deal with scope deletion events. It's less
    // useful for collection stats, but allows us to print the CollectionID
    // as the value and ensures that we still have unique keys which is a
    // requirement of stats.
    int i = 0;
    for (auto it = scopes.begin(); it != scopes.end(); it++, i++) {
        checked_snprintf(
                buffer, bsize, "vb_%d:manifest:scopes:%d", vbid.get(), i);
        add_casted_stat(buffer, it->to_string().c_str(), add_stat, cookie);
    }

    i = 0;
    for (auto it = map.begin(); it != map.end(); it++, i++) {
        checked_snprintf(buffer,
                         bsize,
                         "vb_%d:manifest:scope:%s:collection:%d",
                         vbid.get(),
                         it->second.getScopeID().to_string().c_str(),
                         i);
        add_casted_stat(
                buffer, it->first.to_string().c_str(), add_stat, cookie);
    }

    return true;
}

void Manifest::updateSummary(Summary& summary) const {
    for (const auto& entry : map) {
        auto s = summary.find(entry.first);
        if (s == summary.end()) {
            summary[entry.first] = entry.second.getDiskCount();
        } else {
            s->second += entry.second.getDiskCount();
        }
    }
}

boost::optional<std::vector<CollectionID>> Manifest::getCollectionsForScope(
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

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "VB::Manifest: "
       << "uid:" << manifest.manifestUid
       << ", defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", greatestEndSeqno:" << manifest.greatestEndSeqno
       << ", nDeletingCollections:" << manifest.nDeletingCollections
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

std::ostream& operator<<(std::ostream& os,
                         const Manifest::ReadHandle& readHandle) {
    os << "VB::Manifest::ReadHandle: manifest:" << readHandle.manifest;
    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const Manifest::CachingReadHandle& readHandle) {
    os << "VB::Manifest::CachingReadHandle: itr:";
    if (readHandle.iteratorValid()) {
        os << (*readHandle.itr).second;
    } else {
        os << "end";
    }
    os << ", manifest:" << readHandle.manifest;
    return os;
}

} // end namespace VB
} // end namespace Collections
