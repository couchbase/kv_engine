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
#include "checkpoint_manager.h"
#include "collections/manifest.h"
#include "collections/vbucket_serialised_manifest_entry.h"
#include "item.h"
#include "vbucket.h"

#include <JSON_checker.h>
#include <cJSON.h>
#include <cJSON_utils.h>
#include <memory>

namespace Collections {
namespace VB {

Manifest::Manifest(const std::string& manifest)
    : defaultCollectionExists(false),
      greatestEndSeqno(StoredValue::state_collection_open),
      nDeletingCollections(0) {
    if (manifest.empty()) {
        // Empty manifest, initialise the manifest with the default collection
        addNewCollectionEntry(CollectionID::DefaultCollection,
                              0,
                              StoredValue::state_collection_open);
        defaultCollectionExists = true;
        return;
    }

    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(manifest.data()),
                       manifest.size())) {
        throwException<std::invalid_argument>(__FUNCTION__,
                                              "input not valid json");
    }

    unique_cJSON_ptr cjson(cJSON_Parse(manifest.c_str()));
    if (!cjson) {
        throwException<std::invalid_argument>(__FUNCTION__,
                                              "cJSON cannot parse json");
    }

    // Load the uid
    manifestUid = makeUid(getJsonEntry(cjson.get(), "uid"));

    // Load the collections array
    auto jsonCollections = cJSON_GetObjectItem(cjson.get(), "collections");
    if (!jsonCollections || jsonCollections->type != cJSON_Array) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "cannot find valid "
                "collections: " +
                        (!jsonCollections
                                 ? "nullptr"
                                 : std::to_string(jsonCollections->type)));
    }

    // Iterate the collections and load-em up.
    for (int ii = 0; ii < cJSON_GetArraySize(jsonCollections); ii++) {
        auto collection = cJSON_GetArrayItem(jsonCollections, ii);
        CollectionID cid = makeCollectionID(getJsonEntry(collection, "uid"));
        int64_t startSeqno = std::stoll(getJsonEntry(collection, "startSeqno"));
        int64_t endSeqno = std::stoll(getJsonEntry(collection, "endSeqno"));
        auto& entry = addNewCollectionEntry(cid, startSeqno, endSeqno);

        if (cid.isDefaultCollection()) {
            defaultCollectionExists = entry.isOpen();
        }
    }
}

void Manifest::update(::VBucket& vb, const Collections::Manifest& manifest) {
    std::vector<CollectionID> additions, deletions;
    std::tie(additions, deletions) = processManifest(manifest);

    // Process deletions to the manifest
    for (const auto& collection : deletions) {
        beginCollectionDelete(
                vb, manifest.getUid(), collection, OptionalSeqno{/*no-seqno*/});
    }

    // Process additions to the manifest
    for (const auto& collection : additions) {
        addCollection(
                vb, manifest.getUid(), collection, OptionalSeqno{/*no-seqno*/});
    }
}

void Manifest::addCollection(::VBucket& vb,
                             uid_t manifestUid,
                             CollectionID identifier,
                             OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the map. Specify a
    //    non-zero start
    auto& entry = addCollectionEntry(identifier);

    // 1.1 record the uid of the manifest which is adding the collection
    this->manifestUid = manifestUid;

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  identifier,
                                  false /*deleted*/,
                                  optionalSeqno);

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16 " adding collection:%" PRIx32
        ", replica:%s,"
        " backfill:%s, seqno:%" PRId64 ", manifest:%" PRIx64,
        vb.getId(),
        uint32_t(identifier),
        optionalSeqno.is_initialized() ? "true" : "false",
        vb.isBackfillPhase() ? "true" : "false",
        seqno,
        manifestUid);

    // 3. Now patch the entry with the seqno of the system event, note the copy
    //    of the manifest taken at step 1 gets the correct seqno when the system
    //    event is flushed.
    entry.setStartSeqno(seqno);
}

ManifestEntry& Manifest::addCollectionEntry(CollectionID identifier) {
    auto itr = map.find(identifier);
    if (itr == map.end()) {
        if (identifier.isDefaultCollection()) {
            defaultCollectionExists = true;
        }
        // Add new collection with 0,-6 start,end. The caller will correct the
        // seqno based on what the checkpoint manager returns.
        return addNewCollectionEntry(
                identifier, 0, StoredValue::state_collection_open);
    }
    throwException<std::logic_error>(
            __FUNCTION__, "cannot add collection:" + identifier.to_string());
}

ManifestEntry& Manifest::addNewCollectionEntry(CollectionID identifier,
                                               int64_t startSeqno,
                                               int64_t endSeqno) {
    // This method is only for when the map does not have the collection
    if (map.count(identifier) > 0) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "collection already exists, collection:" +
                        identifier.to_string() +
                        ", startSeqno:" + std::to_string(startSeqno) +
                        ", endSeqno:" + std::to_string(endSeqno));
    }

    auto inserted =
            map.emplace(identifier, ManifestEntry(startSeqno, endSeqno));

    // Did we insert a deleting collection (can happen if restoring from
    // persisted manifest)
    if ((*inserted.first).second.isDeleting()) {
        trackEndSeqno(endSeqno);
    }

    return (*inserted.first).second;
}

void Manifest::beginCollectionDelete(::VBucket& vb,
                                     uid_t manifestUid,
                                     CollectionID identifier,
                                     OptionalSeqno optionalSeqno) {
    auto& entry = beginDeleteCollectionEntry(identifier);

    // record the uid of the manifest which removed the collection
    this->manifestUid = manifestUid;

    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  identifier,
                                  true /*deleted*/,
                                  optionalSeqno);

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16 " begin delete of collection:%" PRIx32
        ", replica:%s, backfill:%s, seqno:%" PRId64 ", manifest:%" PRIx64,
        vb.getId(),
        uint32_t(identifier),
        optionalSeqno.is_initialized() ? "true" : "false",
        vb.isBackfillPhase() ? "true" : "false",
        seqno,
        manifestUid);

    if (identifier.isDefaultCollection()) {
        defaultCollectionExists = false;
    }

    entry.setEndSeqno(seqno);

    trackEndSeqno(seqno);
}

ManifestEntry& Manifest::beginDeleteCollectionEntry(CollectionID identifier) {
    auto itr = map.find(identifier);
    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "did not find collection:" + identifier.to_string());
    }

    return itr->second;
}

void Manifest::completeDeletion(::VBucket& vb, CollectionID identifier) {
    auto itr = map.find(identifier);

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16 " complete delete of collection:%" PRIx32,
        vb.getId(),
        uint32_t(identifier));

    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "could not find collection:" + identifier.to_string());
    }

    auto se = itr->second.completeDeletion();

    if (se == SystemEvent::DeleteCollectionHard) {
        map.erase(itr); // wipe out
    }

    nDeletingCollections--;
    if (nDeletingCollections == 0) {
        greatestEndSeqno = StoredValue::state_collection_open;
    }

    queueSystemEvent(
            vb, se, identifier, false /*delete*/, OptionalSeqno{/*none*/});
}

Manifest::processResult Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    std::vector<CollectionID> additions, deletions;

    for (const auto& entry : map) {
        // If the entry is open and not found in the new manifest it must be
        // deleted.
        if (entry.second.isOpen() &&
            manifest.find(entry.first) == manifest.end()) {
            deletions.push_back(entry.first);
        }
    }

    // iterate Manifest and add every collection in Manifest that is not in this
    for (const auto& m : manifest) {
        // if we don't find the collection, then it must be an addition.
        auto itr = map.find(m.first);

        if (itr == map.end()) {
            additions.push_back(m.first);
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
        const DocKey& key) const {
    // If this is a SystemEvent key then...
    if (key.getCollectionID() == CollectionID::System) {
        // A system generated DocKey will have the affected collection ID in the
        // key bytes
        auto lookup = getCollectionIDFromKey(key);
        auto itr = map.find(lookup);
        if (itr == map.end()) {
            throwException<std::logic_error>(
                    __FUNCTION__,
                    "SystemEvent found which didn't match a collection " +
                            std::to_string(lookup));
        }

        if (itr->second.isDeleting()) {
            return lookup;
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

std::unique_ptr<Item> Manifest::createSystemEvent(SystemEvent se,
                                                  CollectionID identifier,
                                                  bool deleted,
                                                  OptionalSeqno seqno) const {
    // Create an item (to be queued and written to disk) that represents
    // the update of a collection and allows the checkpoint to update
    // the _local document with a persisted version of this object (the entire
    // manifest is persisted to disk as JSON).
    // The key for the item includes the name and revision to ensure a
    // checkpoint consumer (e.g. DCP) can transmit the full collection info.

    auto item = SystemEventFactory::make(se,
                                         makeCollectionIdIntoString(identifier),
                                         getSerialisedDataSize(identifier),
                                         seqno);

    // Quite rightly an Item's value is const, but in this case the Item is
    // owned only by the local scope so is safe to mutate (by const_cast force)
    populateWithSerialisedData(
            {const_cast<char*>(item->getData()), item->getNBytes()},
            identifier);

    if (deleted) {
        item->setDeleted();
    }

    return item;
}

int64_t Manifest::queueSystemEvent(::VBucket& vb,
                                   SystemEvent se,
                                   CollectionID identifier,
                                   bool deleted,
                                   OptionalSeqno seq) const {
    // Create and transfer Item ownership to the VBucket
    auto rv = vb.queueItem(
            createSystemEvent(se, identifier, deleted, seq).release(), seq);

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

void Manifest::populateWithSerialisedData(cb::char_buffer out,
                                          CollectionID identifier) const {
    auto* sMan = SerialisedManifest::make(out.data(), getManifestUid(), out);
    uint32_t itemCounter = 1; // always a final entry
    auto* serial = sMan->getManifestEntryBuffer();

    const ManifestEntry* finalEntry = nullptr;
    for (const auto& collectionEntry : map) {
        // Check if we find the mutated entry in the map (so we know if we're
        // mutating it)
        if (collectionEntry.first == identifier) {
            // If a collection in the map matches the collection being changed
            // save the iterator so we can use it when creating the final entry
            finalEntry = &collectionEntry.second;
        } else {
            itemCounter++;
            auto* sme = SerialisedManifestEntry::make(
                    serial, collectionEntry.first, collectionEntry.second, out);
            serial = sme->nextEntry();
        }
    }

    SerialisedManifestEntry* finalSme = nullptr;
    if (finalEntry) {
        // delete
        finalSme = SerialisedManifestEntry::make(
                serial, identifier, *finalEntry, out);
    } else {
        // create
        finalSme = SerialisedManifestEntry::make(serial, identifier, out);
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
    } else if (se == SystemEvent::DeleteCollectionSoft) {
        // Collection delete completed, but collection has been recreated
        json << serial->toJsonResetEnd();
    }

    json << "]}";
    return json.str();
}

std::string Manifest::serialToJson(cb::const_char_buffer buffer) {
    const auto* sMan =
            reinterpret_cast<const SerialisedManifest*>(buffer.data());
    const auto* serial = sMan->getManifestEntryBuffer();

    std::stringstream json;
    json << R"({"uid":")" << std::hex << sMan->getManifestUid()
         << R"(","collections":[)";

    for (uint32_t ii = 0; ii < sMan->getEntryCount(); ii++) {
        json << serial->toJson();
        serial = serial->nextEntry();

        if (ii < sMan->getEntryCount() - 1) {
            json << ",";
        }
    }

    json << "]}";
    return json.str();
}

const char* Manifest::getJsonEntry(cJSON* cJson, const char* key) {
    auto jsonEntry = cJSON_GetObjectItem(cJson, key);
    if (!jsonEntry || jsonEntry->type != cJSON_String) {
        throwException<std::invalid_argument>(
                __FUNCTION__,
                "null or not string, key:" + std::string(key) + " " +
                        (!jsonEntry ? "nullptr"
                                    : std::to_string(jsonEntry->type)));
    }
    return jsonEntry->valuestring;
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
    const auto* sm = reinterpret_cast<const SerialisedManifest*>(
            serialisedManifest.data());
    const auto* sme = sm->getFinalManifestEntry();
    return {sm->getManifestUid(), sme->getCollectionID()};
}

std::string Manifest::getExceptionString(const std::string& thrower,
                                         const std::string& error) const {
    std::stringstream ss;
    ss << "VB::Manifest:" << thrower << ": " << error << ", this:" << *this;
    return ss.str();
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
