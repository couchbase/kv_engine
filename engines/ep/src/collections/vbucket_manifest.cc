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
#include "checkpoint.h"
#include "collections/manifest.h"
#include "collections/vbucket_serialised_manifest_entry.h"
#include "item.h"
#include "vbucket.h"

#include <JSON_checker.h>
#include <cJSON.h>
#include <cJSON_utils.h>
#include <platform/make_unique.h>

namespace Collections {
namespace VB {

Manifest::Manifest(const std::string& manifest)
    : defaultCollectionExists(false),
      separator(DefaultSeparator),
      greatestEndSeqno(StoredValue::state_collection_open),
      nDeletingCollections(0) {
    if (manifest.empty()) {
        // Empty manifest, initialise the manifest with the default collection
        addNewCollectionEntry({DefaultCollectionIdentifier, 0},
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

    // Load the separator
    separator = getJsonEntry(cjson.get(), "separator");

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
        uid_t uid = makeUid(getJsonEntry(collection, "uid"));
        int64_t startSeqno = std::stoll(getJsonEntry(collection, "startSeqno"));
        int64_t endSeqno = std::stoll(getJsonEntry(collection, "endSeqno"));
        std::string collectionName(getJsonEntry(collection, "name"));
        auto& entry = addNewCollectionEntry(
                {collectionName, uid}, startSeqno, endSeqno);

        if (DefaultCollectionIdentifier == collectionName.c_str()) {
            defaultCollectionExists = entry.isOpen();
        }
    }
}

void Manifest::update(::VBucket& vb, const Collections::Manifest& manifest) {
    std::vector<Collections::Manifest::Identifier> additions, deletions;
    std::tie(additions, deletions) = processManifest(manifest);

    if (separator != manifest.getSeparator()) {
        changeSeparator(vb,
                        manifest.getUid(),
                        manifest.getSeparator(),
                        OptionalSeqno{/*no-seqno*/});
    }

    // Process deletions to the manifest
    for (const auto& collection : deletions) {
        beginCollectionDelete(vb,
                              manifest.getUid(),
                              {collection.getName(), collection.getUid()},
                              OptionalSeqno{/*no-seqno*/});
    }

    // Process additions to the manifest
    for (const auto& collection : additions) {
        addCollection(vb,
                      manifest.getUid(),
                      {collection.getName(), collection.getUid()},
                      OptionalSeqno{/*no-seqno*/});
    }
}

void Manifest::addCollection(::VBucket& vb,
                             uid_t manifestUid,
                             Identifier identifier,
                             OptionalSeqno optionalSeqno) {
    // 1. Update the manifest, adding or updating an entry in the map. Specify a
    //    non-zero start
    auto& entry = addCollectionEntry(identifier);

    // 2. Queue a system event, this will take a copy of the manifest ready
    //    for persistence into the vb state file.
    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  identifier,
                                  false /*deleted*/,
                                  optionalSeqno);

    // record the uid of the manifest which is adding the collection
    this->manifestUid = manifestUid;

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16 " adding collection:%.*s, uid:%" PRIx64
        ", replica:%s, backfill:%s, seqno:%" PRId64 ", manifest:%" PRIx64,
        vb.getId(),
        int(identifier.getName().size()),
        identifier.getName().data(),
        identifier.getUid(),
        optionalSeqno.is_initialized() ? "true" : "false",
        vb.isBackfillPhase() ? "true" : "false",
        seqno,
        manifestUid);

    // 3. Now patch the entry with the seqno of the system event, note the copy
    //    of the manifest taken at step 1 gets the correct seqno when the system
    //    event is flushed.
    entry.setStartSeqno(seqno);
}

ManifestEntry& Manifest::addCollectionEntry(Identifier identifier) {
    auto itr = map.find(identifier.getName());
    if (itr == map.end()) {
        if (identifier.isDefaultCollection()) {
            defaultCollectionExists = true;
        }
        // Add new collection with 0,-6 start,end. The caller will correct the
        // seqno based on what the checkpoint manager returns.
        return addNewCollectionEntry(
                identifier, 0, StoredValue::state_collection_open);
    } else if (!itr->second->isOpen()) {
        if (identifier.isDefaultCollection()) {
            defaultCollectionExists = true;
        }

        itr->second->setUid(identifier.getUid());
        return *itr->second;
    }
    throwException<std::logic_error>(
            __FUNCTION__, "cannot add collection:" + to_string(identifier));
}

ManifestEntry& Manifest::addNewCollectionEntry(Identifier identifier,
                                               int64_t startSeqno,
                                               int64_t endSeqno) {
    // This method is only for when the map does not have the collection
    if (map.count(identifier.getName()) > 0) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "collection already exists, collection:" +
                        to_string(identifier) + ", startSeqno:" +
                        std::to_string(startSeqno) + ", endSeqno:" +
                        std::to_string(endSeqno));
    }
    auto m = std::make_unique<ManifestEntry>(identifier, startSeqno, endSeqno);
    auto* newEntry = m.get();
    map.emplace(m->getCharBuffer(), std::move(m));

    if (newEntry->isDeleting()) {
        trackEndSeqno(endSeqno);
    }

    return *newEntry;
}

void Manifest::beginCollectionDelete(::VBucket& vb,
                                     uid_t manifestUid,
                                     Identifier identifier,
                                     OptionalSeqno optionalSeqno) {
    auto& entry = beginDeleteCollectionEntry(identifier);
    auto seqno = queueSystemEvent(vb,
                                  SystemEvent::Collection,
                                  identifier,
                                  true /*deleted*/,
                                  optionalSeqno);

    // record the uid of the manifest which removed the collection
    this->manifestUid = manifestUid;

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16
        " begin delete of collection:%.*s, uid:%" PRIx64
        ", replica:%s, backfill:%s, seqno:%" PRId64 ", manifest:%" PRIx64,
        vb.getId(),
        int(identifier.getName().size()),
        identifier.getName().data(),
        identifier.getUid(),
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

ManifestEntry& Manifest::beginDeleteCollectionEntry(Identifier identifier) {
    auto itr = map.find(identifier.getName());
    if (itr == map.end()) {
            throwException<std::logic_error>(
                    __FUNCTION__,
                    "did not find collection:" + to_string(identifier));
    }

    return *itr->second;
}

void Manifest::completeDeletion(::VBucket& vb,
                                cb::const_char_buffer collection) {
    auto itr = map.find(collection);

    LOG(EXTENSION_LOG_NOTICE,
        "collections: vb:%" PRIu16 " complete delete of collection:%.*s",
        vb.getId(),
        int(collection.size()),
        collection.data());

    if (itr == map.end()) {
        throwException<std::logic_error>(
                __FUNCTION__,
                "could not find collection:" + cb::to_string(collection));
    }

    auto se = itr->second->completeDeletion();
    auto uid = itr->second->getUid();

    if (se == SystemEvent::DeleteCollectionHard) {
        map.erase(itr); // wipe out
    }

    nDeletingCollections--;
    if (nDeletingCollections == 0) {
        greatestEndSeqno = StoredValue::state_collection_open;
    }

    queueSystemEvent(vb,
                     se,
                     {collection, uid},
                     false /*delete*/,
                     OptionalSeqno{/*none*/});
}

void Manifest::changeSeparator(::VBucket& vb,
                               uid_t manifestUid,
                               cb::const_char_buffer newSeparator,
                               OptionalSeqno optionalSeqno) {
    // Can we change the separator? Only allowed to change if there are no
    // collections or the only collection is the default collection
    if (cannotChangeSeparator()) {
        throwException<std::logic_error>(__FUNCTION__,
                                         "cannot change "
                                         "separator to " +
                                                 cb::to_string(newSeparator));
    } else {
        LOG(EXTENSION_LOG_NOTICE,
            "collections: vb:%" PRIu16
            " changing collection separator from:%s, to:%.*s, replica:%s, "
            "backfill:%s, manifest:%" PRIx64,
            vb.getId(),
            separator.c_str(),
            int(newSeparator.size()),
            newSeparator.data(),
            optionalSeqno.is_initialized() ? "true" : "false",
            vb.isBackfillPhase() ? "true" : "false",
            manifestUid);

        // record the uid of the manifest which changed the separator
        this->manifestUid = manifestUid;

        std::string oldSeparator = separator;
        // Change the separator then queue the event so the new separator
        // is recorded in the serialised manifest
        separator = std::string(newSeparator.data(), newSeparator.size());

        // Queue an event so that the manifest is flushed and DCP can
        // replicate the change.
        (void)queueSeparatorChanged(vb, oldSeparator, optionalSeqno);
    }
}

Manifest::processResult Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    std::vector<Collections::Manifest::Identifier> additions, deletions;

    for (const auto& entry : map) {
        // If the entry is open and not found in the new manifest it must be
        // deleted.
        if (entry.second->isOpen() &&
            manifest.find(entry.second->getIdentifier()) == manifest.end()) {
            deletions.push_back(entry.second->getIdentifier());
        }
    }

    // iterate manifest and add all non-existent collection
    for (const auto& m : manifest) {
        // if we don't find the collection, then it must be an addition.
        // if we do find a name match, then check if the collection is in the
        //  process of being deleted or has a new UID
        auto itr = map.find(m.getName());

        if (itr == map.end() || !itr->second->isOpen() ||
            itr->second->getUid() != m.getUid()) {
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

Manifest::container::const_iterator Manifest::getManifestEntry(
        const ::DocKey& key) const {
    return getManifestEntry(key, separator);
}

Manifest::container::const_iterator Manifest::getManifestEntry(
        const ::DocKey& key, const std::string& separator) const {
    cb::const_char_buffer identifier;
    if (defaultCollectionExists &&
        key.getDocNamespace() == DocNamespace::DefaultCollection) {
        identifier = DefaultCollectionIdentifier;
    } else if (key.getDocNamespace() == DocNamespace::Collections) {
        const auto cKey = Collections::DocKey::make(key, separator);
        identifier = cKey.getCollection();
    } else if (key.getDocNamespace() == DocNamespace::System) {
        const auto cKey = Collections::DocKey::make(key);

        if (cKey.getCollection() == SystemEventPrefix) {
            identifier = cKey.getKey();
        } else {
            std::string sysKey(reinterpret_cast<const char*>(key.data()),
                               key.size());
            throwException<std::invalid_argument>(
                    __FUNCTION__, "Use of system key invalid, key:" + sysKey);
        }
    }

    return map.find(identifier);
}

bool Manifest::isLogicallyDeleted(const ::DocKey& key, int64_t seqno) const {
    return isLogicallyDeleted(key, seqno, separator);
}

bool Manifest::isLogicallyDeleted(const ::DocKey& key,
                                  int64_t seqno,
                                  const std::string& separator) const {
    // Only do the searching/scanning work for keys in the deleted range.
    if (seqno <= greatestEndSeqno) {
        switch (key.getDocNamespace()) {
        case DocNamespace::DefaultCollection:
            return !defaultCollectionExists;
        case DocNamespace::Collections: {
            const auto cKey = Collections::DocKey::make(key, separator);
            auto itr = map.find(cKey.getCollection());
            if (itr != map.end()) {
                return seqno <= itr->second->getEndSeqno();
            }
            break;
        }
        case DocNamespace::System: {
            const auto cKey = Collections::DocKey::make(key);
            if (cKey.getCollection() == SystemEventPrefix) {
                auto itr = map.find(cKey.getKey());
                if (itr != map.end()) {
                    return seqno <= itr->second->getEndSeqno();
                }
            }
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
        return seqno <= entry->second->getEndSeqno();
    }
    return false;
}

boost::optional<cb::const_char_buffer> Manifest::shouldCompleteDeletion(
        const ::DocKey& key) const {
    // If this is a SystemEvent key then...
    if (key.getDocNamespace() == DocNamespace::System) {
        const auto cKey = Collections::DocKey::make(key);
        // 1. Check it's a collection's event
        if (cKey.getCollection() == SystemEventPrefix) {
            // 2. Lookup the collection entry
            auto itr = map.find(cKey.getKey());
            if (itr == map.end()) {
                throwException<std::logic_error>(
                        __FUNCTION__,
                        "SystemEvent found which didn't match a collection " +
                                cb::to_string(cKey.getKey()));
            }

            // 3. If this collection is deleting, return the collection name.
            if (itr->second->isDeleting()) {
                return {cKey.getKey()};
            }
        }
    }
    return {};
}

std::unique_ptr<Item> Manifest::createSystemEvent(SystemEvent se,
                                                  Identifier identifier,
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
            cb::to_string(identifier.getName()),
            getSerialisedDataSize(identifier.getName()),
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

std::unique_ptr<Item> Manifest::createSeparatorChangedEvent(
        int64_t highSeqno,
        OptionalSeqno seqno) const {
    // Create an item (to be queued and written to disk) that represents
    // the change of the separator. The item always has the same key.
    // We serialise the state of this object  into the Item's value.
    auto item = SystemEventFactory::make(
            SystemEvent::CollectionsSeparatorChanged,
            std::to_string(highSeqno) + SystemSeparator + separator,
            getSerialisedDataSize(),
            seqno);

    // Quite rightly an Item's value is const, but in this case the Item is
    // owned only by the local scope so is safe to mutate (by const_cast force)
    populateWithSerialisedData(
            {const_cast<char*>(item->getData()), item->getNBytes()});

    return item;
}

int64_t Manifest::queueSystemEvent(::VBucket& vb,
                                   SystemEvent se,
                                   Identifier identifier,
                                   bool deleted,
                                   OptionalSeqno seq) const {
    // Create and transfer Item ownership to the VBucket
    auto rv = vb.queueItem(
            createSystemEvent(se, identifier, deleted, seq).release(), seq);

    // If seq is not set, then this is an active vbucket queueing the event.
    // Collection events will end the CP so they don't de-dep.
    if (!seq.is_initialized()) {
        vb.checkpointManager->createNewCheckpoint();
    }
    return rv;
}

int64_t Manifest::queueSeparatorChanged(::VBucket& vb,
                                        const std::string& oldSeparator,
                                        OptionalSeqno seqno) const {
    // Create and transfer Item ownership to the VBucket
    return vb.queueItem(
            createSeparatorChangedEvent(vb.getHighSeqno(), seqno).release(),
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
                                          Identifier identifier) const {
    auto* sMan = SerialisedManifest::make(
            out.data(), separator, getManifestUid(), out);
    uint32_t itemCounter = 1; // always a final entry
    char* serial = sMan->getManifestEntryBuffer();

    const std::unique_ptr<ManifestEntry>* finalEntry = nullptr;
    for (const auto& collectionEntry : map) {
        // Check if we find the mutated entry in the map (so we know if we're
        // mutating it)
        if (collectionEntry.second->getCharBuffer() == identifier.getName()) {
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
    } else {
        // create
        finalSme = SerialisedManifestEntry::make(serial, identifier, out);
    }

    sMan->setEntryCount(itemCounter);
    sMan->calculateFinalEntryOffest(finalSme);
}

void Manifest::populateWithSerialisedData(cb::char_buffer out) const {
    auto* sMan = SerialisedManifest::make(
            out.data(), separator, getManifestUid(), out);
    char* serial = sMan->getManifestEntryBuffer();

    for (const auto& collectionEntry : map) {
        auto* sme = SerialisedManifestEntry::make(
                serial, *collectionEntry.second, out);
        serial = sme->nextEntry();
    }

    sMan->setEntryCount(map.size());
}

std::string Manifest::serialToJson(const Item& collectionsEventItem) {
    cb::const_char_buffer buffer(collectionsEventItem.getData(),
                                 collectionsEventItem.getNBytes());
    const auto se = SystemEvent(collectionsEventItem.getFlags());

    if (se == SystemEvent::CollectionsSeparatorChanged) {
        return serialToJson(buffer);
    }

    const auto* sMan =
            reinterpret_cast<const SerialisedManifest*>(buffer.data());
    const char* serial = sMan->getManifestEntryBuffer();

    std::stringstream json;
    json << R"({"separator":")" << sMan->getSeparator() << R"(","uid":")"
         << std::hex << sMan->getManifestUid() << R"(","collections":[)";

    if (sMan->getEntryCount() > 1) {
        // Iterate and produce an comma separated list
        for (uint32_t ii = 1; ii < sMan->getEntryCount(); ii++) {
            const auto* sme =
                    reinterpret_cast<const SerialisedManifestEntry*>(serial);
            json << sme->toJson();
            serial = sme->nextEntry();

            if (ii < sMan->getEntryCount() - 1) {
                json << ",";
            }
        }

        // DeleteCollectionHard removes this last entry so no comma
        if (se != SystemEvent::DeleteCollectionHard) {
            json << ",";
        }
    }

    const auto* sme = reinterpret_cast<const SerialisedManifestEntry*>(serial);
    // Last entry is the collection which changed. How did it change?
    if (se == SystemEvent::Collection) {
        // Collection start/end (create/delete)
        json << sme->toJsonCreateOrDelete(collectionsEventItem.isDeleted(),
                                          collectionsEventItem.getBySeqno());
    } else if (se == SystemEvent::DeleteCollectionSoft) {
        // Collection delete completed, but collection has been recreated
        json << sme->toJsonResetEnd();
    }

    json << "]}";
    return json.str();
}

std::string Manifest::serialToJson(cb::const_char_buffer buffer) {
    const auto* sMan =
            reinterpret_cast<const SerialisedManifest*>(buffer.data());
    const char* serial = sMan->getManifestEntryBuffer();

    std::stringstream json;
    json << R"({"separator":")" << sMan->getSeparator() << R"(","uid":")"
         << std::hex << sMan->getManifestUid() << R"(","collections":[)";

    for (uint32_t ii = 0; ii < sMan->getEntryCount(); ii++) {
        const auto* sme =
                reinterpret_cast<const SerialisedManifestEntry*>(serial);
        json << sme->toJson();
        serial = sme->nextEntry();

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

SystemEventData Manifest::getSystemEventData(
        cb::const_char_buffer serialisedManifest) {
    const auto* sm = reinterpret_cast<const SerialisedManifest*>(
            serialisedManifest.data());
    const auto* sme = sm->getFinalManifestEntry();
    return {sm->getManifestUid(), {sme->getCollectionName(), sme->getUid()}};
}

SystemEventSeparatorData Manifest::getSystemEventSeparatorData(
        cb::const_char_buffer serialisedManifest) {
    const auto* sm = reinterpret_cast<const SerialisedManifest*>(
            serialisedManifest.data());
    return {sm->getManifestUid(), sm->getSeparatorBuffer()};
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
       << ", separator:" << manifest.separator
       << ", greatestEndSeqno:" << manifest.greatestEndSeqno
       << ", nDeletingCollections:" << manifest.nDeletingCollections
       << ", map.size:" << manifest.map.size() << std::endl;
    for (auto& m : manifest.map) {
        os << *m.second << std::endl;
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
        os << *readHandle.itr->second;
    } else {
        os << "end";
    }
    os << ", manifest:" << readHandle.manifest;
    return os;
}

} // end namespace VB
} // end namespace Collections
