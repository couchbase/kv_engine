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

#include "collections/flush.h"
#include "../kvstore.h"
#include "collections/collection_persisted_stats.h"
#include "collections/kvstore_generated.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "item.h"

namespace Collections::VB {

// Helper class for doing collection stat updates
class StatsUpdate {
public:
    explicit StatsUpdate(CachingReadHandle&& handle)
        : handle(std::move(handle)) {
    }

    /**
     * An item is being inserted into the collection
     * @param isCommitted the prepare/commit state of the item inserted
     * @param isDelete alive/delete stats of the item inserted
     * @param diskSizeDelta the +/- bytes the insert changes disk-used by
     */
    void insert(bool isCommitted, bool isDelete, ssize_t diskSizeDelta);

    /**
     * An item is being updated in the collection
     * @param isCommitted the prepare/commit state of the item updated
     * @param diskSizeDelta the +/- bytes the update changes disk-used by
     */
    void update(bool isCommitted, ssize_t diskSizeDelta);

    /**
     * An item is being removed (deleted) from the collection
     * @param isCommitted the prepare/commit state of the item removed
     * @param diskSizeDelta the +/- bytes the delete changes disk-used by
     */
    void remove(bool isCommitted, ssize_t diskSizeDelta);

    /**
     * @return true if the seqno represents a logically deleted item for the
     *         locked collection.
     */
    bool isLogicallyDeleted(uint64_t seqno) const;

    /**
     * Increment the item count for the collection associated with the key
     */
    void incrementItemCount();

    /**
     * Decrement the item count for the collection associated with the key
     */
    void decrementItemCount();

    /**
     * Update the on disk size (bytes) for the collection associated with
     * the key
     */
    void updateDiskSize(ssize_t delta);

private:
    /// handle on the collection
    CachingReadHandle handle;
};

bool StatsUpdate::isLogicallyDeleted(uint64_t seqno) const {
    return handle.isLogicallyDeleted(seqno);
}

void StatsUpdate::incrementItemCount() {
    if (!handle.getKey().isInSystemCollection()) {
        handle.incrementItemCount();
    }
}

void StatsUpdate::decrementItemCount() {
    if (!handle.getKey().isInSystemCollection()) {
        handle.decrementItemCount();
    }
}

void StatsUpdate::updateDiskSize(ssize_t delta) {
    handle.updateDiskSize(delta);
}

static std::optional<StatsUpdate> tryTolockAndSetPersistedSeqno(
        Flush& flush, const DocKey& key, uint64_t seqno, bool isCommitted) {
    if (key.isInSystemCollection()) {
        // Is it a collection system event?
        auto [event, id] = SystemEventFactory::getTypeAndID(key);
        switch (event) {
        case SystemEvent::Collection: {
            auto handle =
                    flush.getManifest().lock(key, Manifest::AllowSystemKeys{});
            if (handle.setPersistedHighSeqno(seqno)) {
                // Update the 'mutated' set, stats are changing
                flush.setMutated(CollectionID(id));
            } else {
                // Cannot set the seqno (flushing dropped items) no more updates
                return {};
            }
            return StatsUpdate{std::move(handle)};
        }
        case SystemEvent::Scope:
            break;
        }
        return {};
    }

    auto handle = flush.getManifest().lock(key);

    if (isCommitted) {
        if (handle.setPersistedHighSeqno(seqno)) {
            // Update the 'mutated' set, stats are changing
            flush.setMutated(key.getCollectionID());
            return StatsUpdate{std::move(handle)};
        } else {
            // Cannot set the seqno (flushing dropped items) no more updates
            return {};
        }
    }

    return StatsUpdate{std::move(handle)};
}

void StatsUpdate::insert(bool isCommitted,
                         bool isDelete,
                         ssize_t diskSizeDelta) {
    if (!isDelete && isCommitted) {
        incrementItemCount();
    } // else inserting a tombstone or it's a prepare

    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void StatsUpdate::update(bool isCommitted, ssize_t diskSizeDelta) {
    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void StatsUpdate::remove(bool isCommitted, ssize_t diskSizeDelta) {
    if (isCommitted) {
        decrementItemCount();
    } // else inserting a tombstone or it's a prepare

    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void Flush::saveCollectionStats(
        std::function<void(CollectionID, PersistedStats)> cb) const {
    for (CollectionID c : mutated) {
        PersistedStats stats;
        {
            auto lock = manifest.lock(c);
            if (!lock.valid()) {
                // Can be flushing for a dropped collection (no longer in the
                // manifest)
                continue;
            }
            stats = lock.getPersistedStats();
        }
        cb(c, stats);
    }
}

void Flush::checkAndTriggerPurge(Vbid vbid, KVBucket& bucket) const {
    if (needsPurge) {
        triggerPurge(vbid, bucket);
    }
}

void Flush::triggerPurge(Vbid vbid, KVBucket& bucket) {
    CompactionConfig config;
    config.vbid = vbid;
    bucket.scheduleCompaction(config, nullptr);
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size) {
    auto update = tryTolockAndSetPersistedSeqno(*this, key, seqno, isCommitted);
    if (update) {
        update->insert(isCommitted, isDelete, size);
    }
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size,
                        uint64_t oldSeqno,
                        bool oldIsDelete,
                        size_t oldSize) {
    auto update = tryTolockAndSetPersistedSeqno(*this, key, seqno, isCommitted);
    if (update) {
        if (update->isLogicallyDeleted(oldSeqno) || oldIsDelete) {
            update->insert(isCommitted, isDelete, size);
        } else if (!oldIsDelete && isDelete) {
            update->remove(isCommitted, size - oldSize);
        } else {
            update->update(isCommitted, size - oldSize);
        }
    }
}

void Flush::setMutated(CollectionID cid) {
    mutated.insert(cid);
}

void Flush::recordSystemEvent(const Item& item) {
    switch (SystemEvent(item.getFlags())) {
    case SystemEvent::Collection: {
        if (item.isDeleted()) {
            recordDropCollection(item);
        } else {
            recordCreateCollection(item);
        }
        break;
    }
    case SystemEvent::Scope: {
        if (item.isDeleted()) {
            recordDropScope(item);
        } else {
            recordCreateScope(item);
        }
        break;
    }
    default:
        throw std::invalid_argument("Flush::recordSystemEvent unknown event:" +
                                    std::to_string(item.getFlags()));
    }
    setReadyForCommit();
}

void Flush::setManifestUid(ManifestUid in) {
    manifestUid = std::max<ManifestUid>(manifestUid, in);
}

void Flush::recordCreateCollection(const Item& item) {
    auto createEvent = Collections::VB::Manifest::getCreateEventData(
            {item.getData(), item.getNBytes()});
    KVStore::OpenCollection collection{uint64_t(item.getBySeqno()),
                                       createEvent.metaData};
    auto [itr, emplaced] = collections.try_emplace(
            collection.metaData.cid, CollectionSpan{collection, collection});
    if (!emplaced) {
        // Collection already in the map, we must set this new create as the
        // high or low (or ignore)
        if (uint64_t(item.getBySeqno()) > itr->second.high.startSeqno) {
            itr->second.high = collection;
        } else if (uint64_t(item.getBySeqno()) < itr->second.low.startSeqno) {
            itr->second.low = collection;
        }
    }
    setManifestUid(createEvent.manifestUid);
}

void Flush::recordDropCollection(const Item& item) {
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
            KVStore::DroppedCollection{
                    0, uint64_t(item.getBySeqno()), dropEvent.cid});

    if (!emplaced) {
        // Collection already in the map, we must set this new drop if the
        // highest or ignore
        if (uint64_t(item.getBySeqno()) > itr->second.endSeqno) {
            itr->second = KVStore::DroppedCollection{
                    0, uint64_t(item.getBySeqno()), dropEvent.cid};
        }
    }
    setManifestUid(dropEvent.manifestUid);
}

void Flush::recordCreateScope(const Item& item) {
    auto scopeEvent = Collections::VB::Manifest::getCreateScopeEventData(
            {item.getData(), item.getNBytes()});
    auto [itr, emplaced] =
            scopes.try_emplace(scopeEvent.metaData.sid,
                               KVStore::OpenScope{uint64_t(item.getBySeqno()),
                                                  scopeEvent.metaData});

    // Did we succeed?
    if (!emplaced) {
        // Nope, scope already in the list, the greatest seqno shall
        // remain
        if (uint64_t(item.getBySeqno()) > itr->second.startSeqno) {
            itr->second = KVStore::OpenScope{uint64_t(item.getBySeqno()),
                                             scopeEvent.metaData};
        }
    }

    setManifestUid(scopeEvent.manifestUid);
}

void Flush::recordDropScope(const Item& item) {
    auto dropEvent = Collections::VB::Manifest::getDropScopeEventData(
            {item.getData(), item.getNBytes()});
    auto [itr, emplaced] =
            droppedScopes.try_emplace(dropEvent.sid, item.getBySeqno());

    // Did we succeed?
    if (!emplaced) {
        // Nope, scope already in the list, the greatest seqno shall
        // remain
        if (uint64_t(item.getBySeqno()) > itr->second) {
            itr->second = item.getBySeqno();
        }
    }

    setManifestUid(dropEvent.manifestUid);
}

flatbuffers::DetachedBuffer Flush::encodeManifestUid() {
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
flatbuffers::DetachedBuffer Flush::encodeOpenCollections(
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
                uint32_t(meta.cid),
                meta.maxTtl.has_value(),
                meta.maxTtl.value_or(std::chrono::seconds::zero()).count(),
                builder.CreateString(meta.name.data(), meta.name.size()));
        finalisedOpenCollection.push_back(newEntry);
    }

    // And 'merge' with the data we read
    if (!currentCollections.empty()) {
        KVStore::verifyFlatbuffersData<Collections::KVStore::OpenCollections>(
                currentCollections, "encodeOpenCollections()");
        auto open = flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                currentCollections.data());
        for (const auto* entry : *open->entries()) {
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

flatbuffers::DetachedBuffer Flush::encodeDroppedCollections(
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
        auto newEntry = Collections::KVStore::CreateDropped(
                builder,
                dropped.startSeqno,
                dropped.endSeqno,
                uint32_t(dropped.collectionId));
        output.push_back(newEntry);
    }

    // and now copy across the existing dropped collections
    for (const auto& entry : existingDropped) {
        auto newEntry = Collections::KVStore::CreateDropped(
                builder,
                entry.startSeqno,
                entry.endSeqno,
                uint32_t(entry.collectionId));
        output.push_back(newEntry);
    }

    auto vector = builder.CreateVector(output);
    auto final =
            Collections::KVStore::CreateDroppedCollections(builder, vector);
    builder.Finish(final);

    return builder.Release();
}

flatbuffers::DetachedBuffer Flush::encodeOpenScopes(
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
        KVStore::verifyFlatbuffersData<Collections::KVStore::Scopes>(
                existingScopes, "encodeOpenScopes()");
        auto fbData = flatbuffers::GetRoot<Collections::KVStore::Scopes>(
                existingScopes.data());

        for (const auto* entry : *fbData->entries()) {
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

} // namespace Collections::VB
