/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/kvstore.h"
#include "collections/kvstore_generated.h"
#include "collections/vbucket_manifest.h"
#include "item.h"
#include <flatbuffers/flatbuffers.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <ostream>
#include <string>
#include <unordered_set>

namespace Collections::KVStore {

template <class T>
void verifyFlatbuffersData(cb::const_byte_buffer buf,
                           const std::string& caller) {
    flatbuffers::Verifier v(buf.data(), buf.size());
    if (v.VerifyBuffer<T>(nullptr)) {
        return;
    }
    throw std::runtime_error(fmt::format(
            "verifyFlatbuffersData:{} data invalid, ptr:{}, size:{}",
            caller,
            fmt::ptr(buf.data()),
            buf.size()));
}

ManifestUid decodeManifestUid(cb::const_byte_buffer manifest) {
    verifyFlatbuffersData<Collections::KVStore::CommittedManifest>(
            manifest, "decodeManifestUid(manifest)");
    auto fbData = flatbuffers::GetRoot<Collections::KVStore::CommittedManifest>(
            manifest.data());
    return ManifestUid{fbData->uid()};
}

Collections::KVStore::Manifest decodeManifest(cb::const_byte_buffer manifest,
                                              cb::const_byte_buffer collections,
                                              cb::const_byte_buffer scopes,
                                              cb::const_byte_buffer dropped) {
    Collections::KVStore::Manifest rv{Collections::KVStore::Manifest::Empty{}};

    // As we build the vector of scopes we want to validate that they are unique
    // use a set of ids for checking
    std::unordered_set<ScopeID> openScopes;

    if (!manifest.empty()) {
        rv.manifestUid = decodeManifestUid(manifest);
    }

    rv.collections = decodeOpenCollections(collections);

    if (!scopes.empty()) {
        verifyFlatbuffersData<Collections::KVStore::Scopes>(
                scopes, "decodeManifest(scopes)");
        auto fbData = flatbuffers::GetRoot<Collections::KVStore::Scopes>(
                scopes.data());
        for (const auto* entry : *fbData->entries()) {
            auto emplaced = openScopes.emplace(entry->scopeId());
            // same scope exists many times
            if (!emplaced.second) {
                throw std::invalid_argument(
                        "decodeManifest: duplicate scope:" +
                        ScopeID(entry->scopeId()).to_string() +
                        " in stored data");
            }
            rv.scopes.push_back(
                    {entry->startSeqno(),
                     Collections::ScopeMetaData{entry->scopeId(),
                                                entry->name()->str()}});
        }
    } else {
        // Nothing on disk - the default scope is assumed
        rv.scopes.push_back(OpenScope{0, Collections::ScopeMetaData{}});
    }

    // Do dropped collections exist?
    auto dc = decodeDroppedCollections(dropped);
    rv.droppedCollectionsExist = !dc.empty();
    return rv;
}

std::vector<Collections::KVStore::OpenCollection> decodeOpenCollections(
        cb::const_byte_buffer data) {
    using namespace Collections::KVStore;
    if (data.empty()) {
        // Nothing on disk - the default collection is assumed.
        // start-seqno:0
        // CollectionMetaData{} default ctor is the default collection
        return {OpenCollection{0, Collections::CollectionMetaData{}}};
    }
    // Disk has data, decode it and populate the vector
    verifyFlatbuffersData<OpenCollections>(data, "decodeOpenCollections");

    // As we build the vector of collections to validate that they are unique,
    // use a set of ids for checking
    std::unordered_set<CollectionID> openCollections;
    std::vector<OpenCollection> rv;

    auto fbData = flatbuffers::GetRoot<OpenCollections>(data.data());
    for (const auto* entry : *fbData->entries()) {
        cb::ExpiryLimit maxTtl;
        if (entry->ttlValid()) {
            maxTtl = std::chrono::seconds(entry->maxTtl());
        }

        auto emplaced = openCollections.emplace(entry->collectionId());
        // same collection exists already
        if (!emplaced.second) {
            throw std::invalid_argument(
                    "decodeOpenCollections: duplicate collection:" +
                    CollectionID(entry->collectionId()).to_string() +
                    " in stored data");
        }
        rv.emplace_back(entry->startSeqno(),
                        Collections::CollectionMetaData{
                                entry->scopeId(),
                                entry->collectionId(),
                                entry->name()->str(),
                                maxTtl,
                                getCanDeduplicateFromHistory(entry->history()),
                                Collections::getMetered(entry->metered()),
                                Collections::ManifestUid{entry->flushUid()}});
    }
    return rv;
}

std::vector<Collections::KVStore::DroppedCollection> decodeDroppedCollections(
        cb::const_byte_buffer dc) {
    if (dc.empty()) {
        return {};
    }
    std::vector<Collections::KVStore::DroppedCollection> rv;
    verifyFlatbuffersData<Collections::KVStore::DroppedCollections>(
            dc, "decodeDroppedCollections()");
    auto fbData =
            flatbuffers::GetRoot<Collections::KVStore::DroppedCollections>(
                    dc.data());
    for (const auto* entry : *fbData->entries()) {
        rv.push_back({static_cast<uint64_t>(entry->startSeqno()),
                      static_cast<uint64_t>(entry->endSeqno()),
                      entry->collectionId()});
    }
    return rv;
}

bool OpenCollection::operator==(const OpenCollection& other) const {
    return startSeqno == other.startSeqno && metaData == other.metaData;
}

bool OpenScope::operator==(const OpenScope& other) const {
    return startSeqno == other.startSeqno && metaData == other.metaData;
}

bool Manifest::compareCollections(const Manifest& other) const {
    if (collections.size() != other.collections.size()) {
        return false;
    }

    for (const auto& collection : collections) {
        if (std::count(other.collections.begin(),
                       other.collections.end(),
                       collection) == 0) {
            return false;
        }
    }

    return true;
}

bool Manifest::compareScopes(const Manifest& other) const {
    if (scopes.size() != other.scopes.size()) {
        return false;
    }

    for (const auto& scope : scopes) {
        if (std::count(other.scopes.begin(), other.scopes.end(), scope) == 0) {
            return false;
        }
    }

    return true;
}

bool Manifest::operator==(const Manifest& other) const {
    return manifestUid == other.manifestUid && compareCollections(other) &&
           compareScopes(other) &&
           droppedCollectionsExist == other.droppedCollectionsExist;
}

bool DroppedCollection::operator==(const DroppedCollection& other) const {
    return startSeqno == other.startSeqno && endSeqno == other.endSeqno &&
           collectionId == other.collectionId;
}

std::ostream& operator<<(
        std::ostream& os,
        const Collections::KVStore::OpenCollection& collection) {
    return os << collection.metaData
              << ", startSeqno:" << collection.startSeqno;
}

std::ostream& operator<<(std::ostream& os,
                         const Collections::KVStore::OpenScope& scope) {
    return os << scope.metaData << ", startSeqno:" << scope.startSeqno;
}

std::ostream& operator<<(std::ostream& os,
                         const Collections::KVStore::Manifest& manifest) {
    std::string scopesBuff;
    for (const auto& scope : manifest.scopes) {
        fmt::format_to(std::back_inserter(scopesBuff), "[{}],", scope);
    }
    if (!scopesBuff.empty()) {
        scopesBuff.pop_back();
    }

    std::string collectionBuff;
    for (const auto& collection : manifest.collections) {
        fmt::format_to(std::back_inserter(collectionBuff), "[{}],", collection);
    }
    if (!collectionBuff.empty()) {
        collectionBuff.pop_back();
    }

    return os << fmt::format(
                   "manifestUid:{:#x} droppedCollectionsExist:{} scopes:{{{}}} "
                   "collections:{{{}}}",
                   manifest.manifestUid,
                   manifest.droppedCollectionsExist,
                   scopesBuff,
                   collectionBuff);
}

} // namespace Collections::KVStore
