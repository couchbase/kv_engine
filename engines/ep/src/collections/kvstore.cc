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
#include <iostream>
#include <sstream>
#include <unordered_set>

namespace Collections::KVStore {

template <class T>
void verifyFlatbuffersData(cb::const_byte_buffer buf,
                           const std::string& caller) {
    flatbuffers::Verifier v(buf.data(), buf.size());
    if (v.VerifyBuffer<T>(nullptr)) {
        return;
    }

    std::stringstream ss;
    ss << "verifyFlatbuffersData: " << caller
       << " data invalid, ptr:" << reinterpret_cast<const void*>(buf.data())
       << ", size:" << buf.size();

    throw std::runtime_error(ss.str());
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

    // As we build the lists of collections and scopes we want to validate that
    // they are unique, use a set of ids for checking
    std::unordered_set<CollectionID> openCollections;
    std::unordered_set<ScopeID> openScopes;

    if (!manifest.empty()) {
        rv.manifestUid = decodeManifestUid(manifest);
    }
    if (!collections.empty()) {
        verifyFlatbuffersData<Collections::KVStore::OpenCollections>(
                collections, "decodeManifest(open)");

        auto fbData =
                flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                        collections.data());
        for (const auto* entry : *fbData->entries()) {
            cb::ExpiryLimit maxTtl;
            if (entry->ttlValid()) {
                maxTtl = std::chrono::seconds(entry->maxTtl());
            }

            auto emplaced = openCollections.emplace(entry->collectionId());
            // same collection exists already
            if (!emplaced.second) {
                throw std::invalid_argument(
                        "decodeManifest: duplicate collection:" +
                        CollectionID(entry->collectionId()).to_string() +
                        " in stored data");
            }
            rv.collections.push_back(
                    {entry->startSeqno(),
                     Collections::CollectionMetaData{entry->scopeId(),
                                                     entry->collectionId(),
                                                     entry->name()->str(),
                                                     maxTtl}});
        }
    } else {
        // Nothing on disk - the default collection is assumed
        rv.collections.push_back(
                OpenCollection{0, Collections::CollectionMetaData{}});
    }

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

bool Manifest::operator==(const Manifest& other) const {
    if ((manifestUid == other.manifestUid) &&
        (droppedCollectionsExist == other.droppedCollectionsExist) &&
        (collections.size() == other.collections.size()) &&
        (scopes.size() == other.scopes.size())) {
        for (const auto& collection : collections) {
            if (std::count(other.collections.begin(),
                           other.collections.end(),
                           collection) == 0) {
                return false;
            }
        }

        for (const auto& scope : scopes) {
            if (std::count(other.scopes.begin(), other.scopes.end(), scope) ==
                0) {
                return false;
            }
        }
    } else {
        return false;
    }

    return true;
}

bool DroppedCollection::operator==(const DroppedCollection& other) const {
    return startSeqno == other.startSeqno && endSeqno == other.endSeqno &&
           collectionId == other.collectionId;
}

} // namespace Collections::KVStore
