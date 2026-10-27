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
#include <fmt/ranges.h>
#include <gsl/gsl-lite.hpp>
#include <algorithm>
#include <functional>
#include <limits>
#include <ostream>
#include <queue>
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
                                              cb::const_byte_buffer dropped,
                                              cb::const_byte_buffer erase) {
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
            auto emplaced = openScopes.emplace(entry->scope_id());
            // same scope exists many times
            if (!emplaced.second) {
                throw std::invalid_argument(
                        "decodeManifest: duplicate scope:" +
                        ScopeID(entry->scope_id()).to_string() +
                        " in stored data");
            }
            rv.scopes.push_back(
                    {entry->start_seqno(),
                     Collections::ScopeMetaData{entry->scope_id(),
                                                entry->name()->str()}});
        }
    } else {
        // Nothing on disk - the default scope is assumed
        rv.scopes.push_back(OpenScope{0, Collections::ScopeMetaData{}});
    }

    // Dropped data exists if erase seqnos are stored, or a dropped list (as
    // written by an older version) is non-empty. Both are always decoded so
    // that corrupt data is detected here (e.g. at warmup).
    const bool eraseSeqnosExist = !decodeEraseSeqnos(erase).empty();
    const bool droppedListExists = !decodeDroppedCollections(dropped).empty();
    rv.droppedCollectionsExist = eraseSeqnosExist || droppedListExists;
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
        if (entry->ttl_valid()) {
            maxTtl = std::chrono::seconds(entry->max_ttl());
        }

        auto emplaced = openCollections.emplace(entry->collection_id());
        // same collection exists already
        if (!emplaced.second) {
            throw std::invalid_argument(
                    "decodeOpenCollections: duplicate collection:" +
                    CollectionID(entry->collection_id()).to_string() +
                    " in stored data");
        }
        rv.emplace_back(entry->start_seqno(),
                        Collections::CollectionMetaData{
                                entry->scope_id(),
                                entry->collection_id(),
                                entry->name()->str(),
                                maxTtl,
                                getCanDeduplicateFromHistory(entry->history()),
                                Collections::getMetered(entry->metered()),
                                Collections::ManifestUid{entry->flush_uid()}});
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
        rv.push_back({entry->start_seqno(),
                      entry->end_seqno(),
                      entry->collection_id()});
    }
    return rv;
}

EraseSeqnos::EraseSeqnos(std::vector<uint64_t> seqnos)
    : seqnos(std::move(seqnos)) {
    Expects(std::ranges::adjacent_find(this->seqnos, std::greater_equal<>()) ==
            this->seqnos.end());
}

void EraseSeqnos::add(const std::vector<uint64_t>& newSeqnos, size_t maxSize) {
    for (const auto seqno : newSeqnos) {
        if (seqno == 0) {
            continue;
        }
        auto itr = std::ranges::lower_bound(seqnos, seqno);
        if (itr == seqnos.end() || *itr != seqno) {
            seqnos.insert(itr, seqno);
        }
    }
    merge(maxSize);
}

void EraseSeqnos::merge(size_t maxSize) {
    // At least the erase seqno is kept
    maxSize = std::max(maxSize, size_t{1});
    if (seqnos.size() <= maxSize) {
        return;
    }

    // Repeatedly remove the lesser seqno of the closest adjacent pair, the
    // greater seqno stands for both. The pairs are kept in a min-heap of
    // (gap, lesser index), the index breaking a tie in favour of the lesser
    // pair. Removing seqnos[i] joins its neighbours into a new pair, a heap
    // entry is stale once either of its seqnos has been removed.
    const auto size = seqnos.size();
    constexpr auto none = std::numeric_limits<size_t>::max();
    std::vector<size_t> previous(size);
    std::vector<size_t> next(size);
    std::vector<bool> removed(size, false);
    using Pair = std::pair<uint64_t, size_t>;
    std::priority_queue<Pair, std::vector<Pair>, std::greater<>> pairs;
    for (size_t index = 0; index < size; ++index) {
        previous[index] = index == 0 ? none : index - 1;
        next[index] = index + 1;
        if (index + 1 < size) {
            pairs.emplace(seqnos[index + 1] - seqnos[index], index);
        }
    }

    for (auto remaining = size; remaining > maxSize;) {
        const auto [gap, lesser] = pairs.top();
        pairs.pop();
        if (removed[lesser] || removed[next[lesser]] ||
            seqnos[next[lesser]] - seqnos[lesser] != gap) {
            continue; // stale
        }
        removed[lesser] = true;
        --remaining;
        const auto greater = next[lesser];
        const auto before = previous[lesser];
        previous[greater] = before;
        if (before != none) {
            next[before] = greater;
            pairs.emplace(seqnos[greater] - seqnos[before], before);
        }
    }

    std::vector<uint64_t> kept;
    kept.reserve(maxSize);
    for (size_t index = 0; index < size; ++index) {
        if (!removed[index]) {
            kept.push_back(seqnos[index]);
        }
    }
    seqnos = std::move(kept);
}

bool EraseSeqnos::removeUpTo(uint64_t seqno) {
    return std::erase_if(seqnos, [seqno](uint64_t erased) {
               return erased <= seqno;
           }) != 0;
}

std::ostream& operator<<(std::ostream& os, const EraseSeqnos& eraseSeqnos) {
    return os << fmt::format("[{}]", fmt::join(eraseSeqnos.getSeqnos(), ","));
}

EraseSeqnos decodeEraseSeqnos(cb::const_byte_buffer data) {
    if (data.empty()) {
        return {};
    }
    verifyFlatbuffersData<EraseSeqno>(data, "decodeEraseSeqnos()");
    const auto* root = flatbuffers::GetRoot<EraseSeqno>(data.data());
    std::vector<uint64_t> seqnos;
    if (root->seqnos()) {
        seqnos.assign(root->seqnos()->begin(), root->seqnos()->end());
    }
    // Correct (rather than reject) an invalid set: drop 0, sort and remove
    // duplicates
    std::erase(seqnos, 0);
    std::ranges::sort(seqnos);
    const auto duplicates = std::ranges::unique(seqnos);
    seqnos.erase(duplicates.begin(), duplicates.end());
    return EraseSeqnos{std::move(seqnos)};
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
        if (std::ranges::count(other.collections, collection) == 0) {
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
        if (std::ranges::count(other.scopes, scope) == 0) {
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
                   "manifestUid:{:#x} droppedCollectionsExist:{} "
                   "scopes:{{{}}} collections:{{{}}}",
                   manifest.manifestUid,
                   manifest.droppedCollectionsExist,
                   scopesBuff,
                   collectionBuff);
}

} // namespace Collections::KVStore
