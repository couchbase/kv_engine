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
#include "item.h"
#include "statwriter.h"
#include "vbucket.h"

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

    auto entries = manifest->entries();
    // Use our defined entryCount so we skip any fully dropped collection which
    // maybe at the end
    for (uint32_t ii = 0; ii < manifest->entryCount(); ii++) {
        auto entry = entries->Get(ii);
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

boost::optional<Manifest::Addition> Manifest::applyCreates(
        ::VBucket& vb, std::vector<Addition>& changes) {
    boost::optional<Addition> rv;
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

bool Manifest::update(::VBucket& vb, const Collections::Manifest& manifest) {
    auto rv = processManifest(manifest);
    if (!rv.is_initialized()) {
        EP_LOG_WARN("VB::Manifest::update cannot update {}", vb.getId());
        return false;
    } else {
        for (auto scope : rv->scopesToAdd) {
            scopes.emplace(scope);
        }

        for (auto scope : rv->scopesToRemove) {
            scopes.erase(scope);
        }

        auto finalDeletion = applyDeletions(vb, rv->collectionsToRemove);

        if (rv->collectionsToAdd.empty() && finalDeletion) {
            beginCollectionDelete(
                    vb,
                    manifest.getUid(), // Final update with new UID
                    *finalDeletion,
                    OptionalSeqno{/*no-seqno*/});
            return true;
        } else if (finalDeletion) {
            beginCollectionDelete(vb,
                                  manifestUid,
                                  *finalDeletion,
                                  OptionalSeqno{/*no-seqno*/});
        }

        auto finalAddition = applyCreates(vb, rv->collectionsToAdd);

        if (finalAddition) {
            addCollection(vb,
                          manifest.getUid(), // Final update with new UID
                          finalAddition.get().identifiers,
                          finalAddition.get().name,
                          finalAddition.get().maxTtl,
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
            "collections: {} adding collection:name:{},id:{:x} to scope:{:x}, "
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
            ", replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            cid,
            entry.getScopeID(),
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);

    if (cid.isDefaultCollection()) {
        defaultCollectionExists = false;
    }

    entry.setEndSeqno(seqno);

    trackEndSeqno(seqno);
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

    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "could not find collection:" + collectionID.to_string());
    }

    auto se = itr->second.completeDeletion();

    // Grab the scopeID before we erase the object
    auto scopeID = itr->second.getScopeID();

    if (se == SystemEvent::DeleteCollectionHard) {
        map.erase(itr); // wipe out
    }

    nDeletingCollections--;
    if (nDeletingCollections == 0) {
        greatestEndSeqno = StoredValue::state_collection_open;
    }

    queueSystemEvent(vb,
                     se,
                     {scopeID, collectionID},
                     {/*no name*/},
                     false /*delete*/,
                     OptionalSeqno{/*none*/});
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

    for (const auto& scope : scopes) {
        // Remove the scopes that don't exist in the new manifest
        if (manifest.findScope(scope) == manifest.endScopes()) {
            rv->scopesToRemove.push_back(scope);
        }
    }

    // Add scopes and collections in Manifest but not in our map
    for (auto scopeItr = manifest.beginScopes();
         scopeItr != manifest.endScopes();
         scopeItr++) {
        if (scopes.find(scopeItr->first) == scopes.end()) {
            rv->scopesToAdd.push_back(scopeItr->first);
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

std::string Manifest::makeCollectionIdIntoString(CollectionID collection) {
    return std::string(reinterpret_cast<const char*>(&collection),
                       sizeof(CollectionID));
}

CollectionID Manifest::getCollectionIDFromKey(const DocKey& key) {
    if (key.getCollectionID() != CollectionID::System) {
        throw std::invalid_argument("getCollectionIDFromKey: non-system key");
    }
    auto raw = SystemEventFactory::getKeyExtra(key);
    if (raw.size() != sizeof(CollectionID)) {
        throw std::invalid_argument(
                "getCollectionIDFromKey: key yielded bad CollectionID size:" +
                std::to_string(raw.size()));
    }
    return {*reinterpret_cast<const uint32_t*>(raw.data())};
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
    auto rv = vb.queueItem(
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
    flatbuffers::Offset<flatbuffers::String> name;
    if (collectionName.data()) {
        name = builder.CreateString(collectionName.data(),
                                    collectionName.size());
    } else {
        name = builder.CreateString("");
    }
    auto manifest = CreateSerialisedManifest(
            builder, getManifestUid(), entriesVector.size(), entries, name);
    builder.Finish(manifest);
}

PersistedManifest Manifest::patchSerialisedData(
        const Item& collectionsEventItem) {
    const uint8_t* ptr =
            reinterpret_cast<const uint8_t*>(collectionsEventItem.getData());
    PersistedManifest mutableData(ptr, ptr + collectionsEventItem.getNBytes());
    auto manifest =
            flatbuffers::GetMutableRoot<SerialisedManifest>(mutableData.data());

    const auto se = SystemEvent(collectionsEventItem.getFlags());
    if (se == SystemEvent::Collection) {
        auto mutatedEntry = manifest->mutable_entries()->GetMutableObject(
                manifest->entries()->size() - 1);

        bool failed = false;
        if (collectionsEventItem.isDeleted()) {
            failed = !mutatedEntry->mutate_endSeqno(
                    collectionsEventItem.getBySeqno());
        } else {
            failed = !mutatedEntry->mutate_startSeqno(
                    collectionsEventItem.getBySeqno());
        }

        if (failed) {
            throw std::logic_error(
                    "Manifest::patchSerialisedData failed to mutate, "
                    "new seqno: " +
                    std::to_string(collectionsEventItem.getBySeqno()) +
                    " isDeleted:" +
                    std::to_string(collectionsEventItem.isDeleted()));
        }
    } else if (se == SystemEvent::DeleteCollectionHard) {
        // DeleteHard is removing the mutated collection, we achieve this by
        // trimming the counter we maintain, the mutated entry still exists but#
        // will be ignored if used in the VB::Manifest constructor
        if (!manifest->mutate_entryCount(manifest->entryCount() - 1)) {
            throw std::logic_error(
                    "Manifest::patchSerialisedData failed to mutate entryCount "
                    "to newvalue:" +
                    std::to_string(manifest->entryCount() - 1));
        }
    }

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

    auto mutatedEntry = manifest->entries()->GetMutableObject(
            manifest->entries()->size() - 1);

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

    auto mutatedEntry = manifest->entries()->GetMutableObject(
            manifest->entries()->size() - 1);

    return {manifest->uid(), mutatedEntry->collectionId()};
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
        add_casted_stat(buffer,
                        defaultCollectionExists ? "true" : "false",
                        add_stat,
                        cookie);
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
    if (scopes.find(identifier) == scopes.end()) {
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
       << ", map.size:" << manifest.map.size() << std::endl;
    for (auto& m : manifest.map) {
        os << "cid:" << m.first.to_string() << ":" << m.second << std::endl;
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
