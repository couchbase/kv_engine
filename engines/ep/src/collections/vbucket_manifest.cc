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
#include "collections/manifest.h"
#include "collections/vbucket_serialised_manifest_entry.h"
#include "vbucket.h"

#include <JSON_checker.h>
#include <cJSON.h>
#include <cJSON_utils.h>
#include <platform/make_unique.h>

namespace Collections {
namespace VB {

Manifest::Manifest(const std::string& manifest)
    : defaultCollectionExists(false), separator(DefaultSeparator) {
    if (manifest.empty()) {
        // Empty manifest, initialise the manifest with the default collection
        addNewCollectionEntry(DefaultCollectionIdentifier,
                              0,
                              0,
                              StoredValue::state_collection_open);
        defaultCollectionExists = true;
        return;
    }

    if (!checkUTF8JSON(reinterpret_cast<const unsigned char*>(manifest.data()),
                       manifest.size())) {
        throw std::invalid_argument(
                "VB::Manifest::Manifest input not valid json");
    }

    unique_cJSON_ptr cjson(cJSON_Parse(manifest.c_str()));
    if (!cjson) {
        throw std::invalid_argument(
                "VB::Manifest::Manifest cJSON cannot parse json");
    }

    // Load the separator
    separator = getJsonEntry(cjson.get(), "separator");

    // Load the collections array
    auto jsonCollections = cJSON_GetObjectItem(cjson.get(), "collections");
    if (!jsonCollections || jsonCollections->type != cJSON_Array) {
        throw std::invalid_argument(
                "VB::Manifest::Manifest cannot find valid "
                "collections: " +
                (!jsonCollections ? "nullptr"
                                  : std::to_string(jsonCollections->type)));
    }

    // Iterate the collections and load-em up.
    for (int ii = 0; ii < cJSON_GetArraySize(jsonCollections); ii++) {
        auto collection = cJSON_GetArrayItem(jsonCollections, ii);
        int revision = std::stoi(getJsonEntry(collection, "revision"));
        int64_t startSeqno = std::stoll(getJsonEntry(collection, "startSeqno"));
        int64_t endSeqno = std::stoll(getJsonEntry(collection, "endSeqno"));
        std::string collectionName(getJsonEntry(collection, "name"));
        auto& entry = addNewCollectionEntry(
                collectionName, revision, startSeqno, endSeqno);

        if (DefaultCollectionIdentifier == collectionName.c_str()) {
            defaultCollectionExists = entry.isOpen();
        }
    }
}

void Manifest::update(::VBucket& vb, const Collections::Manifest& manifest) {
    std::vector<std::string> additions, deletions;
    std::tie(additions, deletions) = processManifest(manifest);

    if (separator != manifest.getSeparator()) {
        changeSeparator(vb,
                        manifest.getSeparator(),
                        manifest.getRevision(),
                        OptionalSeqno{/*no-seqno*/});
    }

    // Process additions to the manifest
    for (const auto& collection : additions) {
        addCollection(vb,
                      collection,
                      manifest.getRevision(),
                      OptionalSeqno{/*no-seqno*/});
    }

    // Process deletions to the manifest
    for (const auto& collection : deletions) {
        beginCollectionDelete(vb,
                              collection,
                              manifest.getRevision(),
                              OptionalSeqno{/*no-seqno*/});
    }
}

void Manifest::addCollection(::VBucket& vb,
                             cb::const_char_buffer collection,
                             uint32_t revision,
                             OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the map. Specify a
    //    non-zero start
    auto& entry = addCollectionEntry(collection, revision);

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::CreateCollection,
                                  collection,
                                  revision,
                                  optionalSeqno);

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16 " adding collection:%.*s, revision:%" PRIu32
        ", seqno:%" PRIu64,
        vb.getId(),
        int(collection.size()),
        collection.data(),
        revision,
        seqno);

    // 3. Now patch the entry with the seqno of the system event, note the copy
    //    of the manifest taken at step 1 gets the correct seqno when the system
    //    event is flushed.
    entry.setStartSeqno(seqno);
    entry.setRevision(revision);
}

ManifestEntry& Manifest::addCollectionEntry(cb::const_char_buffer collection,
                                            uint32_t revision) {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        if (collection == DefaultCollectionIdentifier) {
            defaultCollectionExists = true;
        }
        // Add new collection with 0,-6 start,end. The caller will correct the
        // seqno based on what the checkpoint manager returns.
        return addNewCollectionEntry(
                collection, revision, 0, StoredValue::state_collection_open);
    } else if (!itr->second->isOpen()) {
        if (collection == DefaultCollectionIdentifier) {
            defaultCollectionExists = true;
        }
        return *itr->second;
    }

    std::stringstream ss;
    ss << *itr->second;
    throw std::logic_error(
            "VB::Manifest::addCollectionEntry: cannot add collection:" +
            cb::to_string(collection) + ", revision:" +
            std::to_string(revision) + ", entry:" + ss.str());
}

ManifestEntry& Manifest::addNewCollectionEntry(cb::const_char_buffer collection,
                                               uint32_t revision,
                                               int64_t startSeqno,
                                               int64_t endSeqno) {
    // This method is only for when the map does not have the collection
    if (map.count(collection) > 0) {
        throw std::logic_error(
                "Manifest::addNewCollectionEntry: already exists collection:" +
                cb::to_string(collection) + ", revision:" +
                std::to_string(revision) + ", startSeqno:" +
                std::to_string(startSeqno) + ", endSeqno:" +
                std::to_string(endSeqno));
    }
    auto m = std::make_unique<ManifestEntry>(
            collection, revision, startSeqno, endSeqno);
    auto* newEntry = m.get();
    map.emplace(m->getCharBuffer(), std::move(m));
    return *newEntry;
}

void Manifest::beginCollectionDelete(::VBucket& vb,
                                     cb::const_char_buffer collection,
                                     uint32_t revision,
                                     OptionalSeqno optionalSeqno) {
    auto& entry = beginDeleteCollectionEntry(collection, revision);
    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::BeginDeleteCollection,
                                  collection,
                                  revision,
                                  optionalSeqno);

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16
        " begin delete of collection:%.*s, revision:%" PRIu32
        ", seqno:%" PRIu64,
        vb.getId(),
        int(collection.size()),
        collection.data(),
        revision,
        seqno);

    entry.setEndSeqno(seqno);
    entry.setRevision(revision);
}

ManifestEntry& Manifest::beginDeleteCollectionEntry(
        cb::const_char_buffer collection, uint32_t revision) {
    auto itr = map.find(collection);
    if (itr == map.end()) {
        throw std::logic_error(
                "VB::Manifest::beginDeleteCollectionEntry: did not find "
                "collection:" +
                cb::to_string(collection) + ", revision:" +
                std::to_string(revision));
    }

    if (collection == DefaultCollectionIdentifier) {
        defaultCollectionExists = false;
    }

    return *itr->second;
}

void Manifest::completeDeletion(::VBucket& vb,
                                cb::const_char_buffer collection,
                                uint32_t revision) {
    auto itr = map.find(collection);

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16
        " complete delete of collection:%.*s, revision:%" PRIu32,
        vb.getId(),
        int(collection.size()),
        collection.data(),
        revision);

    if (itr == map.end()) {
        throw std::logic_error(
                "VB::Manifest::completeDeletion: could not find collection:" +
                cb::to_string(collection));
    }

    if (itr->second->isExclusiveDeleting()) {
        map.erase(itr); // wipe out

        // When we find that the collection is not open, we can hard delete it.
        // This means we are purging it completely from the manifest and will
        // generate a JSON manifest without an entry for the collection.
        queueSystemEvent(vb,
                         SystemEvent::DeleteCollectionHard,
                         collection,
                         revision,
                         OptionalSeqno{/*none*/});
    } else if (itr->second->isOpenAndDeleting()) {
        itr->second->resetEndSeqno(); // just reset the end to our special seqno

        // When we find that the collection open and deleting we can soft delete
        // it. This means we are just adjusting the endseqno so that the entry
        // returns true for isExclusiveOpen()
        queueSystemEvent(vb,
                         SystemEvent::DeleteCollectionSoft,
                         collection,
                         itr->second->getRevision(),
                         OptionalSeqno{/*none*/});
    } else {
        // This is an invalid request
        std::stringstream ss;
        ss << *itr->second;
        throw std::logic_error(
                "VB::Manifest::completeDeletion: cannot delete:" + ss.str());
    }
}

void Manifest::changeSeparator(::VBucket& vb,
                               cb::const_char_buffer newSeparator,
                               uint32_t revision,
                               OptionalSeqno optionalSeqno) {
    // Can we change the separator? Only allowed to change if there are no
    // collections or the only collection is the default collection
    if (cannotChangeSeparator()) {
        std::stringstream ss;
        ss << *this;
        throw std::logic_error(
                "VB::Manifest::changeSeparator cannot change "
                "separator to " +
                cb::to_string(newSeparator) + " " + ss.str());
    } else {
        LOG(EXTENSION_LOG_NOTICE,
            "collections: vb:%" PRIu16
            " changing collection separator from:%s, to:%.*s",
            vb.getId(),
            separator.c_str(),
            int(newSeparator.size()),
            newSeparator.data());

        // Change the separator then queue the event so the new separator
        // is recorded in the serialised manifest
        separator = std::string(newSeparator.data(), newSeparator.size());

        // Queue an event so that the manifest is flushed and DCP can
        // replicate the change.
        (void)queueSeparatorChanged(vb, revision, optionalSeqno);
    }
}

Manifest::processResult Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    std::vector<std::string> additions, deletions;

    for (const auto& manifestEntry : map) {
        // If the manifestEntry is open and not found in the new manifest it
        // must be deleted.
        if (manifestEntry.second->isOpen() &&
            manifest.find(manifestEntry.second->getCollectionName()) ==
                    manifest.end()) {
            deletions.push_back(std::string(
                    reinterpret_cast<const char*>(manifestEntry.first.data()),
                    manifestEntry.first.size()));
        }
    }

    // iterate manifest and add all non-existent collection
    for (const auto& m : manifest) {
        // if we don't find the collection, then it must be an addition.
        // if we do find a name match, then check if the collection is in the
        //  process of being deleted.
        auto itr = map.find(m);

        if (itr == map.end() || !itr->second->isOpen()) {
            additions.push_back(m);
        }
    }
    return std::make_pair(additions, deletions);
}

bool Manifest::doesKeyContainValidCollection(const ::DocKey& key) const {
    if (defaultCollectionExists &&
        key.getDocNamespace() == DocNamespace::DefaultCollection) {
        return true;
    } else if (key.getDocNamespace() == DocNamespace::Collections) {
        const auto cKey = Collections::DocKey::make(key, separator);
        auto itr = map.find({reinterpret_cast<const char*>(cKey.data()),
                             cKey.getCollectionLen()});
        if (itr != map.end()) {
            return itr->second->isOpen();
        }
    }
    return false;
}

bool Manifest::doesKeyContainDeletingCollection(const ::DocKey& key,
                                                int64_t seqno) const {
    if (key.getDocNamespace() == DocNamespace::DefaultCollection &&
        !defaultCollectionExists) {
        return true;
    } else if (key.getDocNamespace() == DocNamespace::Collections) {
        const auto cKey = Collections::DocKey::make(key, separator);
        auto itr = map.find({reinterpret_cast<const char*>(cKey.data()),
                             cKey.getCollectionLen()});
        if (itr != map.end()) {
            return seqno <= itr->second->getEndSeqno();
        }
    }
    return false;
}

std::unique_ptr<Item> Manifest::createSystemEvent(
        SystemEvent se,
        cb::const_char_buffer collection,
        uint32_t revision,
        OptionalSeqno seqno) const {
    // Create an item (to be queued and written to disk) that represents
    // the update of a collection and allows the checkpoint to update
    // the _local document with a persisted version of this object (the entire
    // manifest is persisted to disk as JSON).
    // The key for the item includes the name and revision to ensure a
    // checkpoint consumer (e.g. DCP) can transmit the full collection info.

    auto item = SystemEventFactory::make(
            se,
            separator,
            cb::to_string(collection) + separator + std::to_string(revision),
            getSerialisedDataSize(collection),
            seqno);

    // Quite rightly an Item's value is const, but in this case the Item is
    // owned only by the local scope so is safe to mutate (by const_cast force)
    populateWithSerialisedData(
            {const_cast<char*>(item->getData()), item->getNBytes()},
            collection,
            revision);

    return item;
}

std::unique_ptr<Item> Manifest::createSeparatorChangedEvent(
        uint32_t revision, OptionalSeqno seqno) const {
    // Create an item (to be queued and written to disk) that represents
    // the change of the separator. The item always has the same key.
    // We serialise the state of this object  into the Item's value.
    auto item =
            SystemEventFactory::make(SystemEvent::CollectionsSeparatorChanged,
                                     separator,
                                     {/*empty string*/},
                                     getSerialisedDataSize(),
                                     seqno);

    // Quite rightly an Item's value is const, but in this case the Item is
    // owned only by the local scope so is safe to mutate (by const_cast force)
    populateWithSerialisedData(
            {const_cast<char*>(item->getData()), item->getNBytes()}, revision);

    return item;
}

int64_t Manifest::queueSystemEvent(::VBucket& vb,
                                   SystemEvent se,
                                   cb::const_char_buffer collection,
                                   uint32_t revision,
                                   OptionalSeqno seq) const {
    // Create and transfer Item ownership to the VBucket
    return vb.queueItem(
            createSystemEvent(se, collection, revision, seq).release(), seq);
}

int64_t Manifest::queueSeparatorChanged(::VBucket& vb,
                                        uint32_t revision,
                                        OptionalSeqno seqno) const {
    // Create and transfer Item ownership to the VBucket
    return vb.queueItem(createSeparatorChangedEvent(revision, seqno).release(),
                        seqno);
}

size_t Manifest::getSerialisedDataSize(cb::const_char_buffer collection) const {
    size_t bytesNeeded = SerialisedManifest::getObjectSize(separator.size());
    for (const auto& collectionEntry : map) {
        // Skip if a collection in the map matches the collection being changed
        if (collectionEntry.second->getCharBuffer() == collection) {
            continue;
        }
        bytesNeeded += SerialisedManifestEntry::getObjectSize(
                collectionEntry.second->getCollectionName().size());
    }

    return bytesNeeded +
           SerialisedManifestEntry::getObjectSize(collection.size());
}

size_t Manifest::getSerialisedDataSize() const {
    size_t bytesNeeded = SerialisedManifest::getObjectSize(separator.size());
    for (const auto& collectionEntry : map) {
        // Skip if a collection in the map matches the collection being changed
        bytesNeeded += SerialisedManifestEntry::getObjectSize(
                collectionEntry.second->getCollectionName().size());
    }

    return bytesNeeded;
}

void Manifest::populateWithSerialisedData(cb::char_buffer out,
                                          cb::const_char_buffer collection,
                                          uint32_t revision) const {
    auto* sMan = SerialisedManifest::make(out.data(), separator, revision, out);
    uint32_t itemCounter = 1; // always a final entry
    char* serial = sMan->getManifestEntryBuffer();

    const std::unique_ptr<ManifestEntry>* finalEntry = nullptr;
    for (const auto& collectionEntry : map) {
        // Check if we find the mutated entry in the map (so we know if we're
        // deleting it)
        if (collectionEntry.second->getCharBuffer() == collection) {
            // If a collection in the map matches the collection being changed
            // save the iterator so we can use it when creating the final entry
            finalEntry = &collectionEntry.second;
        } else {
            itemCounter++;
            auto* sme = SerialisedManifestEntry::make(
                    serial, *collectionEntry.second, out);
            serial = sme->nextEntry();
        }
    }

    SerialisedManifestEntry* finalSme = nullptr;
    if (finalEntry) {
        // delete
        finalSme =
                SerialisedManifestEntry::make(serial, *finalEntry->get(), out);
        finalSme->setRevision(revision);
    } else {
        // create
        finalSme = SerialisedManifestEntry::make(
                serial, revision, collection, out);
    }

    sMan->setEntryCount(itemCounter);
    sMan->calculateFinalEntryOffest(finalSme);
}

void Manifest::populateWithSerialisedData(cb::char_buffer out,
                                          uint32_t revision) const {
    auto* sMan = SerialisedManifest::make(out.data(), separator, revision, out);
    char* serial = sMan->getManifestEntryBuffer();

    for (const auto& collectionEntry : map) {
        auto* sme = SerialisedManifestEntry::make(
                serial, *collectionEntry.second, out);
        serial = sme->nextEntry();
    }

    sMan->setEntryCount(map.size());
}

std::string Manifest::serialToJson(SystemEvent se,
                                   cb::const_char_buffer buffer,
                                   int64_t finalEntrySeqno) {
    if (se == SystemEvent::CollectionsSeparatorChanged) {
        return serialToJson(buffer);
    }

    const auto* sMan =
            reinterpret_cast<const SerialisedManifest*>(buffer.data());
    const char* serial = sMan->getManifestEntryBuffer();

    std::string json = R"({"separator":")" + sMan->getSeparator() + R"(","collections":[)";
    if (sMan->getEntryCount() > 1) {
        for (uint32_t ii = 1; ii < sMan->getEntryCount(); ii++) {
            const auto* sme =
                    reinterpret_cast<const SerialisedManifestEntry*>(serial);
            json += sme->toJson();
            serial = sme->nextEntry();

            if (ii < sMan->getEntryCount() - 1) {
                json += ",";
            }
        }
        const auto* sme =
                reinterpret_cast<const SerialisedManifestEntry*>(serial);
        if (se != SystemEvent::DeleteCollectionHard) {
            json += "," + sme->toJson(se, finalEntrySeqno);
        }
    } else {
        const auto* sme =
                reinterpret_cast<const SerialisedManifestEntry*>(serial);
        json += sme->toJson(se, finalEntrySeqno);
    }

    json += "]}";
    return json;
}

std::string Manifest::serialToJson(cb::const_char_buffer buffer) {
    const auto* sMan =
            reinterpret_cast<const SerialisedManifest*>(buffer.data());
    const char* serial = sMan->getManifestEntryBuffer();

    std::string json =
            R"({"separator":")" + sMan->getSeparator() + R"(","collections":[)";

    for (uint32_t ii = 0; ii < sMan->getEntryCount(); ii++) {
        const auto* sme =
                reinterpret_cast<const SerialisedManifestEntry*>(serial);
        json += sme->toJson();
        serial = sme->nextEntry();

        if (ii < sMan->getEntryCount() - 1) {
            json += ",";
        }
    }

    json += "]}";
    return json;
}

const char* Manifest::getJsonEntry(cJSON* cJson, const char* key) {
    auto jsonEntry = cJSON_GetObjectItem(cJson, key);
    if (!jsonEntry || jsonEntry->type != cJSON_String) {
        throw std::invalid_argument(
                "VB::Manifest::getJsonEntry(" + std::string(key) + ") : " +
                (!jsonEntry ? "nullptr" : std::to_string(jsonEntry->type)));
    }
    return jsonEntry->valuestring;
}

bool Manifest::cannotChangeSeparator() const {
    // If any non-default collection exists that isOpen, cannot change separator
    for (const auto& manifestEntry : map) {
        // If the manifestEntry is open and not found in the new manifest it
        // must be deleted.
        if (manifestEntry.second->isOpen() &&
            manifestEntry.second->getCharBuffer() !=
                    DefaultCollectionIdentifier) {
            // Collection is open and is not $default - cannot change
            return true;
        }
    }

    return false;
}

std::pair<cb::const_char_buffer, cb::const_byte_buffer>
Manifest::getSystemEventData(cb::const_char_buffer serialisedManifest) {
    const auto* sm = reinterpret_cast<const SerialisedManifest*>(
            serialisedManifest.data());
    const auto* sme = sm->getFinalManifestEntry();
    return std::make_pair(sme->getCollectionName(), sm->getRevisionBuffer());
}

std::pair<cb::const_char_buffer, cb::const_byte_buffer>
Manifest::getSystemEventSeparatorData(
        cb::const_char_buffer serialisedManifest) {
    const auto* sm = reinterpret_cast<const SerialisedManifest*>(
            serialisedManifest.data());
    return std::make_pair(sm->getSeparatorBuffer(), sm->getRevisionBuffer());
}

std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
    os << "VB::Manifest"
       << ": defaultCollectionExists:" << manifest.defaultCollectionExists
       << ", separator:" << manifest.separator
       << ", map.size:" << manifest.map.size() << std::endl;
    for (auto& m : manifest.map) {
        os << *m.second << std::endl;
    }

    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const Manifest::ReadHandle& readHandle) {
    os << readHandle.manifest;
    return os;
}

} // end namespace VB
} // end namespace Collections