/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "collections/kvstore.h"
#include "collections/kvstore_generated.h"
#include <sstream>

namespace Collections::KVStore {

template <class T>
static void verifyFlatbuffersData(cb::const_byte_buffer buf,
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

Collections::KVStore::Manifest decodeManifest(cb::const_byte_buffer manifest,
                                              cb::const_byte_buffer collections,
                                              cb::const_byte_buffer scopes,
                                              cb::const_byte_buffer dropped) {
    Collections::KVStore::Manifest rv{Collections::KVStore::Manifest::Empty{}};

    if (!manifest.empty()) {
        verifyFlatbuffersData<Collections::KVStore::CommittedManifest>(
                manifest, "decodeManifest(manifest)");
        auto fbData =
                flatbuffers::GetRoot<Collections::KVStore::CommittedManifest>(
                        manifest.data());
        rv.manifestUid = fbData->uid();
    }
    if (!collections.empty()) {
        verifyFlatbuffersData<Collections::KVStore::OpenCollections>(
                collections, "decodeManifest(open)");

        auto fbData =
                flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                        collections.data());
        for (const auto& entry : *fbData->entries()) {
            cb::ExpiryLimit maxTtl;
            if (entry->ttlValid()) {
                maxTtl = std::chrono::seconds(entry->maxTtl());
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
                {0,
                 {ScopeID::Default,
                  CollectionID::Default,
                  Collections::DefaultCollectionIdentifier.data(),
                  {}}});
    }

    if (!scopes.empty()) {
        verifyFlatbuffersData<Collections::KVStore::Scopes>(
                scopes, "decodeManifest(scopes)");
        auto fbData = flatbuffers::GetRoot<Collections::KVStore::Scopes>(
                scopes.data());
        for (const auto& entry : *fbData->entries()) {
            rv.scopes.push_back(entry);
        }
    } else {
        // Nothing on disk - the default scope is assumed
        rv.scopes.emplace_back(ScopeID::Default);
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
    for (const auto& entry : *fbData->entries()) {
        rv.push_back({entry->startSeqno(),
                      entry->endSeqno(),
                      entry->collectionId()});
    }
    return rv;
}

flatbuffers::DetachedBuffer encodeManifestUid(
        Collections::KVStore::CommitMetaData& meta) {
    flatbuffers::FlatBufferBuilder builder;
    auto toWrite = Collections::KVStore::CreateCommittedManifest(
            builder, meta.manifestUid);
    builder.Finish(toWrite);
    std::string_view buffer{
            reinterpret_cast<const char*>(builder.GetBufferPointer()),
            builder.GetSize()};
    return builder.Release();
}

flatbuffers::DetachedBuffer encodeOpenCollections(
        std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        Collections::KVStore::CommitMetaData& collectionsMeta,
        cb::const_byte_buffer collections) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Collection>>
            openCollections;

    for (const auto& event : collectionsMeta.collections) {
        const auto& meta = event.metaData;
        auto newEntry = Collections::KVStore::CreateCollection(
                builder,
                event.startSeqno,
                meta.sid,
                meta.cid,
                meta.maxTtl.has_value(),
                meta.maxTtl.value_or(std::chrono::seconds::zero()).count(),
                builder.CreateString(meta.name.data(), meta.name.size()));
        openCollections.push_back(newEntry);

        // Validate we are not adding a dropped collection
        auto itr = std::find_if(
                droppedCollections.begin(),
                droppedCollections.end(),
                [&meta](const Collections::KVStore::DroppedCollection&
                                dropped) {
                    return dropped.collectionId == meta.cid;
                });
        if (itr != droppedCollections.end()) {
            // we have found the created collection in the drop list, not good
            throw std::logic_error(
                    "Collections::KVStore::encodeOpenCollections found a new "
                    "collection in dropped list, cid:" +
                    meta.cid.to_string());
        }
    }

    // And 'merge' with the data we read
    if (!collections.empty()) {
        verifyFlatbuffersData<Collections::KVStore::OpenCollections>(
                collections, "encodeOpenCollections()");
        auto fbData =
                flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                        collections.data());
        for (const auto& entry : *fbData->entries()) {
            auto p = [entry](const Collections::KVStore::DroppedCollection& c) {
                return c.collectionId == entry->collectionId();
            };
            auto result =
                    std::find_if(collectionsMeta.droppedCollections.begin(),
                                 collectionsMeta.droppedCollections.end(),
                                 p);

            // If not found in dropped collections add to output
            if (result == collectionsMeta.droppedCollections.end()) {
                auto newEntry = Collections::KVStore::CreateCollection(
                        builder,
                        entry->startSeqno(),
                        entry->scopeId(),
                        entry->collectionId(),
                        entry->ttlValid(),
                        entry->maxTtl(),
                        builder.CreateString(entry->name()));
                openCollections.push_back(newEntry);
            } else {
                // Here we maintain the startSeqno of the dropped collection
                result->startSeqno = entry->startSeqno();
            }
        }
    } else {
        // Nothing on disk - assume the default collection lives
        auto newEntry = Collections::KVStore::CreateCollection(
                builder,
                0,
                ScopeID::Default,
                CollectionID::Default,
                false /* ttl invalid*/,
                0,
                builder.CreateString(
                        Collections::DefaultCollectionIdentifier.data()));
        openCollections.push_back(newEntry);
    }

    auto collectionsVector = builder.CreateVector(openCollections);
    auto toWrite = Collections::KVStore::CreateOpenCollections(
            builder, collectionsVector);

    builder.Finish(toWrite);
    return builder.Release();
}

flatbuffers::DetachedBuffer encodeDroppedCollections(
        Collections::KVStore::CommitMetaData& collectionsMeta,
        const std::vector<Collections::KVStore::DroppedCollection>& dropped) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Dropped>>
            droppedCollections;
    for (const auto& dropped : collectionsMeta.droppedCollections) {
        auto newEntry =
                Collections::KVStore::CreateDropped(builder,
                                                    dropped.startSeqno,
                                                    dropped.endSeqno,
                                                    dropped.collectionId);
        droppedCollections.push_back(newEntry);
    }

    for (const auto& entry : dropped) {
        auto newEntry = Collections::KVStore::CreateDropped(
                builder, entry.startSeqno, entry.endSeqno, entry.collectionId);
        droppedCollections.push_back(newEntry);
    }

    auto vector = builder.CreateVector(droppedCollections);
    auto final =
            Collections::KVStore::CreateDroppedCollections(builder, vector);
    builder.Finish(final);

    // write back
    return builder.Release();
}

flatbuffers::DetachedBuffer encodeScopes(
        Collections::KVStore::CommitMetaData& collectionsMeta,
        cb::const_byte_buffer scopes) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<ScopeIDType> openScopes;
    for (const auto& sid : collectionsMeta.scopes) {
        openScopes.push_back(sid);
    }

    // And 'merge' with the data we read (remove any dropped)
    if (!scopes.empty()) {
        verifyFlatbuffersData<Collections::KVStore::Scopes>(scopes,
                                                            "encodeScopes()");
        auto fbData = flatbuffers::GetRoot<Collections::KVStore::Scopes>(
                scopes.data());

        for (const auto& sid : *fbData->entries()) {
            auto result = std::find(collectionsMeta.droppedScopes.begin(),
                                    collectionsMeta.droppedScopes.end(),
                                    sid);

            // If not found in dropped scopes add to output
            if (result == collectionsMeta.droppedScopes.end()) {
                openScopes.push_back(sid);
            }
        }
    } else {
        // Nothing on disk, the default scope is assumed to exist
        openScopes.push_back(ScopeID::Default);
    }

    auto vector = builder.CreateVector(openScopes);
    auto final = Collections::KVStore::CreateScopes(builder, vector);
    builder.Finish(final);

    // write back
    return builder.Release();
}

} // namespace Collections::KVStore
