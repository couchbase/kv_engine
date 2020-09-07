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
#include "collections/vbucket_manifest.h"
#include "item.h"
#include <iostream>
#include <sstream>
#include <unordered_set>

namespace Collections::KVStore {

void CommitMetaData::clear() {
    needsCommit = false;
    collections.clear();
    scopes.clear();
    droppedCollections.clear();
    droppedScopes.clear();
    manifestUid.reset(0);
}

void CommitMetaData::setManifestUid(ManifestUid in) {
    manifestUid = std::max<ManifestUid>(manifestUid, in);
}

void CommitMetaData::recordCreateCollection(const Item& item) {
    auto createEvent = Collections::VB::Manifest::getCreateEventData(
            {item.getData(), item.getNBytes()});
    OpenCollection collection{item.getBySeqno(), createEvent.metaData};
    auto [itr, emplaced] = collections.try_emplace(
            collection.metaData.cid, CollectionSpan{collection, collection});
    if (!emplaced) {
        // Collection already in the map, we must set this new create as the
        // high or low (or ignore)
        if (item.getBySeqno() > itr->second.high.startSeqno) {
            itr->second.high = collection;
        } else if (item.getBySeqno() < itr->second.low.startSeqno) {
            itr->second.low = collection;
        }
    }
    setManifestUid(createEvent.manifestUid);
}

void CommitMetaData::recordDropCollection(const Item& item) {
    auto dropEvent = Collections::VB::Manifest::getDropEventData(
            {item.getData(), item.getNBytes()});
    // The startSeqno is unknown, so here we set to zero.
    // The Collections::KVStore can discover the real startSeqno when
    // processing the open collection list against the dropped collection
    // list. A kvstore which can atomically drop a collection has no need
    // for this, but one which will background purge dropped collection
    // should maintain the start.
    auto [itr, emplaced] = droppedCollections.try_emplace(
            dropEvent.cid,
            DroppedCollection{0, item.getBySeqno(), dropEvent.cid});

    if (!emplaced) {
        // Collection already in the map, we must set this new drop if the
        // highest or ignore
        if (item.getBySeqno() > itr->second.endSeqno) {
            itr->second =
                    DroppedCollection{0, item.getBySeqno(), dropEvent.cid};
        }
    }
    setManifestUid(dropEvent.manifestUid);
}

void CommitMetaData::recordCreateScope(const Item& item) {
    auto scopeEvent = Collections::VB::Manifest::getCreateScopeEventData(
            {item.getData(), item.getNBytes()});
    auto [itr, emplaced] = scopes.try_emplace(
            scopeEvent.metaData.sid,
            OpenScope{item.getBySeqno(), scopeEvent.metaData});

    // Did we succeed?
    if (!emplaced) {
        // Nope, scope already in the list, the greatest seqno shall
        // remain
        if (item.getBySeqno() > itr->second.startSeqno) {
            itr->second = OpenScope{item.getBySeqno(), scopeEvent.metaData};
        }
    }

    setManifestUid(scopeEvent.manifestUid);
}

void CommitMetaData::recordDropScope(const Item& item) {
    auto dropEvent = Collections::VB::Manifest::getDropScopeEventData(
            {item.getData(), item.getNBytes()});
    auto [itr, emplaced] =
            droppedScopes.try_emplace(dropEvent.sid, item.getBySeqno());

    // Did we succeed?
    if (!emplaced) {
        // Nope, scope already in the list, the greatest seqno shall
        // remain
        if (item.getBySeqno() > itr->second) {
            itr->second = item.getBySeqno();
        }
    }

    setManifestUid(dropEvent.manifestUid);
}

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

    // As we build the lists of collections and scopes we want to validate that
    // they are unique, use a set of ids for checking
    std::unordered_set<CollectionID> openCollections;
    std::unordered_set<ScopeID> openScopes;

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
        for (const auto& entry : *fbData->entries()) {
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
    for (const auto& entry : *fbData->entries()) {
        rv.push_back({entry->startSeqno(),
                      entry->endSeqno(),
                      entry->collectionId()});
    }
    return rv;
}

flatbuffers::DetachedBuffer CommitMetaData::encodeManifestUid() {
    flatbuffers::FlatBufferBuilder builder;
    auto toWrite =
            Collections::KVStore::CreateCommittedManifest(builder, manifestUid);
    builder.Finish(toWrite);
    std::string_view buffer{
            reinterpret_cast<const char*>(builder.GetBufferPointer()),
            builder.GetSize()};
    return builder.Release();
}

// Process the collection commit meta-data to generate the set of open
// collections as flatbuffer data. The inputs to this function are he current
// flatbuffer openCollection data
flatbuffers::DetachedBuffer CommitMetaData::encodeOpenCollections(
        cb::const_byte_buffer currentCollections) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Collection>>
            finalisedOpenCollection;

    // For each created collection ensure that we use the most recent (by-seqno)
    // meta-data for the output but only if there is no drop event following.
    for (auto& [cid, span] : collections) {
        using Collections::KVStore::OpenCollection;

        if (auto itr = droppedCollections.find(cid);
            itr != droppedCollections.end()) {
            // Important - patch the startSeqno of the drop event so that it
            // is set to the entire span of the collection from the flush batch.
            // This may get 'patched' again if the collection is already open,
            // a second check occurs in the merge loop below.
            itr->second.startSeqno = span.low.startSeqno;

            if (itr->second.endSeqno > span.high.startSeqno) {
                // The collection has been dropped
                continue;
            }
        }

        // generate
        const auto& meta = span.high.metaData;
        auto newEntry = Collections::KVStore::CreateCollection(
                builder,
                span.high.startSeqno,
                meta.sid,
                meta.cid,
                meta.maxTtl.has_value(),
                meta.maxTtl.value_or(std::chrono::seconds::zero()).count(),
                builder.CreateString(meta.name.data(), meta.name.size()));
        finalisedOpenCollection.push_back(newEntry);
    }

    // And 'merge' with the data we read
    if (!currentCollections.empty()) {
        verifyFlatbuffersData<Collections::KVStore::OpenCollections>(
                currentCollections, "encodeOpenCollections()");
        auto open = flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                currentCollections.data());
        for (const auto& entry : *open->entries()) {
            // For each currently open collection, is it in the dropped map?
            auto result = droppedCollections.find(entry->collectionId());

            // If not found in dropped collections add to output
            if (result == droppedCollections.end()) {
                auto newEntry = Collections::KVStore::CreateCollection(
                        builder,
                        entry->startSeqno(),
                        entry->scopeId(),
                        entry->collectionId(),
                        entry->ttlValid(),
                        entry->maxTtl(),
                        builder.CreateString(entry->name()));
                finalisedOpenCollection.push_back(newEntry);
            } else {
                // Here we maintain the startSeqno of the dropped collection
                result->second.startSeqno = entry->startSeqno();
            }
        }
    } else if (droppedCollections.count(CollectionID::Default) == 0) {
        // Nothing on disk - and not dropped assume the default collection lives
        auto newEntry = Collections::KVStore::CreateCollection(
                builder,
                0,
                ScopeID::Default,
                CollectionID::Default,
                false /* ttl invalid*/,
                0,
                builder.CreateString(
                        Collections::DefaultCollectionIdentifier.data()));
        finalisedOpenCollection.push_back(newEntry);
    }

    auto collectionsVector = builder.CreateVector(finalisedOpenCollection);
    auto toWrite = Collections::KVStore::CreateOpenCollections(
            builder, collectionsVector);

    builder.Finish(toWrite);
    return builder.Release();
}

flatbuffers::DetachedBuffer CommitMetaData::encodeDroppedCollections(
        std::vector<Collections::KVStore::DroppedCollection>& existingDropped) {
    // Iterate through the existing dropped collections and look each up in the
    // commit metadata. If the collection is in both lists, we will just update
    // the existing data (adjusting the endSeqno) and then erase the collection
    // from the commit meta's dropped collections.
    for (auto& collection : existingDropped) {
        if (auto itr = droppedCollections.find(collection.collectionId);
            itr != droppedCollections.end()) {
            collection.endSeqno = itr->second.endSeqno;

            // Now kick the collection out of collectionsMeta, its contribution
            // to the final output is complete
            droppedCollections.erase(itr);
        }
    }

    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Dropped>> output;

    // Now merge, first the newly dropped collections
    // Iterate  through the list collections dropped in the commit batch and
    // and create flatbuffer versions of each one
    for (const auto& [cid, dropped] : droppedCollections) {
        (void)cid;
        auto newEntry =
                Collections::KVStore::CreateDropped(builder,
                                                    dropped.startSeqno,
                                                    dropped.endSeqno,
                                                    dropped.collectionId);
        output.push_back(newEntry);
    }

    // and now copy across the existing dropped collections
    for (const auto& entry : existingDropped) {
        auto newEntry = Collections::KVStore::CreateDropped(
                builder, entry.startSeqno, entry.endSeqno, entry.collectionId);
        output.push_back(newEntry);
    }

    auto vector = builder.CreateVector(output);
    auto final =
            Collections::KVStore::CreateDroppedCollections(builder, vector);
    builder.Finish(final);

    return builder.Release();
}

flatbuffers::DetachedBuffer CommitMetaData::encodeOpenScopes(
        cb::const_byte_buffer existingScopes) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Scope>> openScopes;

    // For each scope from the kvstore list, copy them into the flatbuffer
    // output
    for (const auto& [sid, event] : scopes) {
        // The scope could have been dropped in the batch
        if (auto itr = droppedScopes.find(sid); itr != droppedScopes.end()) {
            if (itr->second > event.startSeqno) {
                // The scope has been dropped
                continue;
            }
        }

        const auto& meta = event.metaData;
        auto newEntry = Collections::KVStore::CreateScope(
                builder,
                event.startSeqno,
                meta.sid,
                builder.CreateString(meta.name.data(), meta.name.size()));
        openScopes.push_back(newEntry);
    }

    // And 'merge' with the scope flatbuffer data that was read.
    if (!existingScopes.empty()) {
        verifyFlatbuffersData<Collections::KVStore::Scopes>(
                existingScopes, "encodeOpenScopes()");
        auto fbData = flatbuffers::GetRoot<Collections::KVStore::Scopes>(
                existingScopes.data());

        for (const auto& entry : *fbData->entries()) {
            auto result = droppedScopes.find(entry->scopeId());

            // If not found in dropped scopes add to output
            if (result == droppedScopes.end()) {
                auto newEntry = Collections::KVStore::CreateScope(
                        builder,
                        entry->startSeqno(),
                        entry->scopeId(),
                        builder.CreateString(entry->name()));
                openScopes.push_back(newEntry);
            }
        }
    } else {
        // Nothing on disk - assume the default scope lives (it always does)
        auto newEntry = Collections::KVStore::CreateScope(
                builder,
                0, // start-seqno
                ScopeID::Default,
                builder.CreateString(
                        Collections::DefaultScopeIdentifier.data()));
        openScopes.push_back(newEntry);
    }

    auto vector = builder.CreateVector(openScopes);
    auto final = Collections::KVStore::CreateScopes(builder, vector);
    builder.Finish(final);

    // write back
    return builder.Release();
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
