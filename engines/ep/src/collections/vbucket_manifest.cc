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
#include "collections/vbucket_serialised_manifest_entry.h"
#include "item.h"
#include "statwriter.h"
#include "vbucket.h"

#include <json_utilities.h>

#include <JSON_checker.h>
#include <nlohmann/json.hpp>
#include <memory>

namespace Collections {
namespace VB {

Manifest::Manifest(const std::string& manifest)
    : defaultCollectionExists(false),
      greatestEndSeqno(StoredValue::state_collection_open),
      nDeletingCollections(0) {
    if (manifest.empty()) {
        // Empty manifest, initialise the manifest with the default collection
        addNewCollectionEntry({ScopeID::Default, CollectionID::Default},
                              0,
                              StoredValue::state_collection_open);
        defaultCollectionExists = true;
        return;
    }

    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(manifest);
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument(
                "VB::Manifest nlohmann "
                "cannot parse json" +
                cb::to_string(manifest) + ", e:" + e.what());
    }

    manifestUid =
            makeUid(getJsonEntry(parsed, UidKey, UidType).get<std::string>());

    // Load the collections array
    auto collections = getJsonEntry(parsed, CollectionsKey, CollectionsType);

    for (const auto& collection : collections) {
        auto cid = makeCollectionID(
                getJsonEntry(collection, UidKey, UidType).get<std::string>());
        auto sid = makeScopeID(
                getJsonEntry(collection, SidKey, SidType).get<std::string>());
        auto startSeqno = std::stoll(
                getJsonEntry(collection, StartSeqnoKey, StartSeqnoType)
                        .get<std::string>());
        auto endSeqno =
                std::stoll(getJsonEntry(collection, EndSeqnoKey, EndSeqnoType)
                                   .get<std::string>());
        addNewCollectionEntry({sid, cid}, startSeqno, endSeqno);
    }
}

boost::optional<ScopeCollectionPair> Manifest::applyChanges(
        std::function<void(ManifestUid, ScopeCollectionPair, OptionalSeqno)>
                update,
        std::vector<ScopeCollectionPair>& changes) {
    boost::optional<ScopeCollectionPair> rv;
    if (!changes.empty()) {
        rv = changes.back();
        changes.pop_back();
    }
    for (const auto& pair : changes) {
        update(manifestUid, pair, OptionalSeqno{/*no-seqno*/});
    }

    return rv;
}
bool Manifest::update(::VBucket& vb, const Collections::Manifest& manifest) {
    auto rv = processManifest(manifest);
    if (!rv.is_initialized()) {
        EP_LOG_WARN("VB::Manifest::update cannot update {}", vb.getId());
        return false;
    } else {
        std::vector<ScopeCollectionPair>& additions = rv->first;
        std::vector<ScopeCollectionPair>& deletions = rv->second;

        auto finalDeletion =
                applyChanges(std::bind(&Manifest::beginCollectionDelete,
                                       this,
                                       std::ref(vb),
                                       std::placeholders::_1,
                                       std::placeholders::_2,
                                       std::placeholders::_3),
                             deletions);
        if (additions.empty() && finalDeletion) {
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

        auto finalAddition = applyChanges(std::bind(&Manifest::addCollection,
                                                    this,
                                                    std::ref(vb),
                                                    std::placeholders::_1,
                                                    std::placeholders::_2,
                                                    std::placeholders::_3),
                                          additions);

        if (finalAddition) {
            addCollection(vb,
                          manifest.getUid(), // Final update with new UID
                          *finalAddition,
                          OptionalSeqno{/*no-seqno*/});
        }
    }
    return true;
}

void Manifest::addCollection(::VBucket& vb,
                             ManifestUid manifestUid,
                             ScopeCollectionPair identifiers,
                             OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the collections
    // map. Specify a non-zero start
    auto& entry = addNewCollectionEntry(identifiers);

    // 1.1 record the uid of the manifest which is adding the collection
    this->manifestUid = manifestUid;

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  identifiers,
                                  false /*deleted*/,
                                  optionalSeqno);

    EP_LOG_INFO(
            "collections: {} adding collection:{:x} to scope:{:x}, "
            "replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            identifiers.second,
            identifiers.first,
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
    auto inserted =
            map.emplace(identifiers.second,
                        ManifestEntry(identifiers.first, startSeqno, endSeqno));

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
                                     ScopeCollectionPair identifiers,
                                     OptionalSeqno optionalSeqno) {
    auto& entry = getManifestEntry(identifiers.second);

    // record the uid of the manifest which removed the collection
    this->manifestUid = manifestUid;

    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  identifiers,
                                  true /*deleted*/,
                                  optionalSeqno);

    EP_LOG_INFO(
            "collections: {} begin delete of collection:{:x} from scope:{:x}"
            ", replica:{}, backfill:{}, seqno:{}, manifest:{:x}",
            vb.getId(),
            identifiers.second,
            identifiers.first,
            optionalSeqno.is_initialized(),
            vb.isBackfillPhase(),
            seqno,
            manifestUid);

    if (identifiers.second.isDefaultCollection()) {
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
                     false /*delete*/,
                     OptionalSeqno{/*none*/});
}

Manifest::processResult Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    std::vector<ScopeCollectionPair> additions, deletions;

    for (const auto& entry : map) {
        // If the entry is open and not found in the new manifest it must be
        // deleted.
        if (entry.second.isOpen() &&
            manifest.findCollection(entry.first) == manifest.end()) {
            deletions.push_back(
                    std::make_pair(entry.second.getScopeID(), entry.first));
        }
    }

    // Add collections in Manifest but not in our map
    for (auto scopeItr = manifest.beginScopes();
         scopeItr != manifest.endScopes();
         scopeItr++) {
        for (const auto& m : scopeItr->second.collections) {
            auto mapItr = map.find(m);

            if (mapItr == map.end()) {
                additions.push_back(std::make_pair(scopeItr->first, m));
            } else if (mapItr->second.isDeleting()) {
                // trying to add a collection which is deleting, not allowed.
                EP_LOG_WARN("Attempt to add a deleting collection:{}:{:x}",
                            manifest.findCollection(m)->second,
                            m);
                return {};
            }
        }
    }
    return std::make_pair(additions, deletions);
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
        bool deleted,
        OptionalSeqno seqno) const {
    // Create an item (to be queued and written to disk) that represents
    // the update of a collection and allows the checkpoint to update
    // the _local document with a persisted version of this object (the entire
    // manifest is persisted to disk as JSON).
    // The key for the item includes the name and revision to ensure a
    // checkpoint consumer (e.g. DCP) can transmit the full collection info.
    auto item = SystemEventFactory::make(
            se,
            makeCollectionIdIntoString(identifiers.second),
            getSerialisedDataSize(identifiers.second),
            seqno);

    // Quite rightly an Item's value is const, but in this case the Item is
    // owned only by the local scope so is safe to mutate (by const_cast force)
    populateWithSerialisedData(
            {const_cast<char*>(item->getData()), item->getNBytes()},
            identifiers);

    if (deleted) {
        item->setDeleted();
    }

    return item;
}

int64_t Manifest::queueSystemEvent(::VBucket& vb,
                                   SystemEvent se,
                                   ScopeCollectionPair identifiers,
                                   bool deleted,
                                   OptionalSeqno seq) const {
    // Create and transfer Item ownership to the VBucket
    auto rv = vb.queueItem(
            createSystemEvent(se, identifiers, deleted, seq).release(), seq);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dup.
    if (!seq.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }
    return rv;
}

size_t Manifest::getSerialisedDataSize(CollectionID identifier) const {
    size_t bytesNeeded = SerialisedManifest::getObjectSize();
    for (const auto& collectionEntry : map) {
        // Skip if a collection in the map matches the collection being changed
        if (collectionEntry.first == identifier) {
            continue;
        }
        bytesNeeded += SerialisedManifestEntry::getObjectSize();
    }

    return bytesNeeded + SerialisedManifestEntry::getObjectSize();
}

void Manifest::populateWithSerialisedData(
        cb::char_buffer out, ScopeCollectionPair identifiers) const {
    auto* sMan = SerialisedManifest::make(out.data(), getManifestUid(), out);
    uint32_t itemCounter = 1; // always a final entry
    auto* serial = sMan->getManifestEntryBuffer();

    const ManifestEntry* finalEntry = nullptr;
    for (const auto& collectionEntry : map) {
        // Check if we find the mutated entry in the map (so we know if we're
        // mutating it)
        if (collectionEntry.first == identifiers.second) {
            // If a collection in the map matches the collection being changed
            // save the iterator so we can use it when creating the final entry
            finalEntry = &collectionEntry.second;
        } else {
            itemCounter++;
            auto* sme = SerialisedManifestEntry::make(
                    serial,
                    {collectionEntry.second.getScopeID(),
                     collectionEntry.first},
                    collectionEntry.second,
                    out);
            serial = sme->nextEntry();
        }
    }

    SerialisedManifestEntry* finalSme = nullptr;
    if (finalEntry) {
        // delete
        finalSme = SerialisedManifestEntry::make(
                serial, identifiers, *finalEntry, out);
    } else {
        // create
        finalSme = SerialisedManifestEntry::make(serial, identifiers, out);
    }

    sMan->setEntryCount(itemCounter);
    sMan->calculateFinalEntryOffest(finalSme);
}

std::string Manifest::serialToJson(const Item& collectionsEventItem) {
    cb::const_char_buffer buffer(collectionsEventItem.getData(),
                                 collectionsEventItem.getNBytes());
    const auto se = SystemEvent(collectionsEventItem.getFlags());

    const auto* sMan =
            reinterpret_cast<const SerialisedManifest*>(buffer.data());
    const auto* serial = sMan->getManifestEntryBuffer();

    std::stringstream json;
    json << R"({"uid":")" << std::hex << sMan->getManifestUid()
         << R"(","collections":[)";

    if (sMan->getEntryCount() > 1) {
        // Iterate and produce an comma separated list
        for (uint32_t ii = 1; ii < sMan->getEntryCount(); ii++) {
            json << serial->toJson();
            serial = serial->nextEntry();

            if (ii < sMan->getEntryCount() - 1) {
                json << ",";
            }
        }

        // DeleteCollectionHard removes this last entry so no comma
        if (se != SystemEvent::DeleteCollectionHard) {
            json << ",";
        }
    }

    // Last entry is the collection which changed. How did it change?
    if (se == SystemEvent::Collection) {
        // Collection start/end (create/delete)
        json << serial->toJsonCreateOrDelete(collectionsEventItem.isDeleted(),
                                             collectionsEventItem.getBySeqno());
    }

    json << "]}";
    return json.str();
}

nlohmann::json Manifest::getJsonEntry(const nlohmann::json& object,
                                      const std::string& key,
                                      nlohmann::json::value_t expectedType) {
    return cb::getJsonObject(object, key, expectedType, "VB::Manifest");
}

void Manifest::trackEndSeqno(int64_t seqno) {
    nDeletingCollections++;
    if (seqno > greatestEndSeqno ||
        greatestEndSeqno == StoredValue::state_collection_open) {
        greatestEndSeqno = seqno;
    }
}

SystemEventData Manifest::getSystemEventData(
        cb::const_char_buffer serialisedManifest) {
    const auto* sm = SerialisedManifest::make(serialisedManifest);
    const auto* sme = sm->getFinalManifestEntry();
    return {sm->getManifestUid(), sme->getScopeID(), sme->getCollectionID()};
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

bool Manifest::addStats(Vbid vbid,
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
                "VB::Manifest::addStats {}, failed to build stats "
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

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "VB::Manifest"
       << ": defaultCollectionExists:" << manifest.defaultCollectionExists
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
