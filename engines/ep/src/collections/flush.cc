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
#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "collections/kvstore_generated.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "item.h"

namespace Collections::VB {

Flush::StatisticsUpdate& Flush::getStatsAndMaybeSetPersistedHighSeqno(
        CollectionID cid, uint64_t seqno) {
    auto [itr, inserted] = stats.try_emplace(cid, StatisticsUpdate{seqno});
    auto& [key, value] = *itr;
    (void)key;
    if (!inserted) {
        value.maybeSetPersistedHighSeqno(seqno);
    }

    return value;
}

void Flush::StatisticsUpdate::maybeSetPersistedHighSeqno(uint64_t seqno) {
    if (seqno > persistedHighSeqno) {
        persistedHighSeqno = seqno;
    }
}

void Flush::StatisticsUpdate::incrementItemCount() {
    itemCount++;
}

void Flush::StatisticsUpdate::decrementItemCount() {
    itemCount--;
}

void Flush::StatisticsUpdate::updateDiskSize(ssize_t delta) {
    diskSize += delta;
}

void Flush::StatisticsUpdate::insert(bool isSystem,
                                     bool isDelete,
                                     ssize_t diskSizeDelta) {
    if (!isSystem && !isDelete) {
        incrementItemCount();
    } // else inserting a tombstone - no item increment

    updateDiskSize(diskSizeDelta);
}

void Flush::StatisticsUpdate::update(ssize_t diskSizeDelta) {
    updateDiskSize(diskSizeDelta);
}

void Flush::StatisticsUpdate::remove(bool isSystem,
                                     ssize_t diskSizeDelta) {
    if (!isSystem) {
        decrementItemCount();
    }
    updateDiskSize(diskSizeDelta);
}

// Called from KVStore during the flush process and before we consider the
// data of the flush to be committed. This method iterates through the
// statistics gathered by the Flush and uses the std::function callback to
// have the KVStore implementation write them to storage, e.g. a local document.
void Flush::saveCollectionStats(
        std::function<void(CollectionID, const PersistedStats&)> cb) const {
    // For each collection modified in the flush run ask the VBM for the
    // current stats (using the high-seqno so we find the correct generation
    // of stats)
    for (const auto& [cid, flushStats] : stats) {
        // Don't generate an update of the statistics for a dropped collection.
        // 1) it's wasted effort
        // 2) the kvstore may not be able handle an update of a 'local' doc
        //    and a delete of the same in one flush (couchstore doesn't)
        auto itr = droppedCollections.find(cid);
        if (itr != droppedCollections.end() &&
            flushStats.getPersistedHighSeqno() < itr->second.endSeqno) {
            continue;
        }
        // Get the current stats of the collection (for the seqno)
        auto stats = manifest.lock().getStatsForFlush(
                cid, flushStats.getPersistedHighSeqno());

        // Generate new stats, add the deltas from this flush batch for count
        // and size and set the high-seqno
        PersistedStats ps(stats.itemCount + flushStats.getItemCount(),
                          flushStats.getPersistedHighSeqno(),
                          stats.diskSize + flushStats.getDiskSize());
        cb(cid, ps);
    }
}

void Flush::forEachDroppedCollection(
        std::function<void(CollectionID)> cb) const {
    // To invoke the callback only for dropped collections iterate the dropped
    // map and then check in the 'stats' map (and if found do an ordering check)
    for (const auto& [cid, dropped] : droppedCollections) {
        auto itr = stats.find(cid);
        if (itr == stats.end() ||
            dropped.endSeqno > itr->second.getPersistedHighSeqno()) {
            cb(cid);
        }
    }
}

// Called from KVStore after a successful commit.
// This method will iterate through all of the collection stats that the Flush
// gathered and attempt to update the VB::Manifest (which is where cmd_stat
// reads from). This method has to consider that the VB::Manifest can
// be modified by changes to the collections manifest during the flush, for
// example by the time we've gathered statistics about a collection, the
// VB::Manifest may have 1) dropped the collection 2) dropped and recreated
// the collection - in either of these cases the gathered statistics are no
// longer applicable and are not pushed to the VB::Manifest.
void Flush::postCommitMakeStatsVisible() {
    for (const auto& [cid, flushStats] : stats) {
        auto lock = manifest.lock(cid);
        if (!lock.valid() ||
            lock.isLogicallyDeleted(flushStats.getPersistedHighSeqno())) {
            // Can be flushing for a dropped collection (no longer in the
            // manifest, or was flushed/recreated at a new seqno)
            continue;
        }
        // update the stats with the changes collected by the flusher
        lock.updateItemCount(flushStats.getItemCount());
        lock.setPersistedHighSeqno(flushStats.getPersistedHighSeqno());
        lock.updateDiskSize(flushStats.getDiskSize());
    }
}

void Flush::flushSuccess(Vbid vbid, KVBucket& bucket) {
    try {
        notifyManifestOfAnyDroppedCollections();
    } catch (const std::exception& e) {
        EP_LOG_CRITICAL(
                "Flush notifyManifestOfAnyDroppedCollections caught exception "
                "for {}",
                vbid);
        EP_LOG_CRITICAL("{}", e.what());
        throw;
    }
    checkAndTriggerPurge(vbid, bucket);
}

void Flush::notifyManifestOfAnyDroppedCollections() {
    for (const auto& [cid, droppedData] : droppedCollections) {
        manifest.collectionDropPersisted(cid, droppedData.endSeqno);
    }
}

void Flush::checkAndTriggerPurge(Vbid vbid, KVBucket& bucket) const {
    if (!droppedCollections.empty()) {
        triggerPurge(vbid, bucket);
    }
}

void Flush::triggerPurge(Vbid vbid, KVBucket& bucket) {
    CompactionConfig config;
    config.vbid = vbid;
    bucket.scheduleCompaction(config, nullptr);
}

static std::pair<bool, std::optional<CollectionID>> getCollectionID(
        const DocKey& key) {
    bool isSystemEvent = key.isInSystemCollection();
    CollectionID cid;
    if (isSystemEvent) {
        auto [event, id] = SystemEventFactory::getTypeAndID(key);
        switch (event) {
        case SystemEvent::Collection: {
            cid = CollectionID(id);
            break;
        }
        case SystemEvent::Scope:
            return {true, {}};
        }
    } else {
        cid = key.getCollectionID();
    }
    return {isSystemEvent, cid};
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size) {
    // Prepares don't change the stats
    if (!isCommitted) {
        return;
    }
    auto [isSystemEvent, cid] = getCollectionID(key);

    if (!cid || isLogicallyDeleted(cid.value(), seqno)) {
        // 1) The key is not for a collection (could be a scope event) or
        // 2) The key belongs to a collection now dropped, the drop is in this
        //    flush batch.
        // The flusher still persists documents that are in this state but we
        // do not gather statistics about them - this is because the current
        // statistics will be wiped out by the flush (side effect of the drop
        // going through the KVStore).
        return;
    }

    getStatsAndMaybeSetPersistedHighSeqno(cid.value(), seqno)
            .insert(isSystemEvent, isDelete, size);
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size,
                        uint64_t oldSeqno,
                        bool oldIsDelete,
                        size_t oldSize) {
    // Prepares don't change the stats
    if (!isCommitted) {
        return;
    }
    // Same logic and comment as updateStats above.
    auto [isSystemEvent, cid] = getCollectionID(key);
    if (!cid || isLogicallyDeleted(cid.value(), seqno)) {
        return;
    }

    // Next update the delete state for the old item.
    // 1) Already deleted or
    // 2) the key's collection is dropped in this flush batch
    // 3) the key's collection is dropped in the snapshot we are writing to
    // For 2 and 3 we are switching live documents of dropped collections into
    // being deleted, thus any update flips to an insert
    oldIsDelete =
            oldIsDelete || (isLogicallyDeleted(cid.value(), oldSeqno) ||
                            isLogicallyDeletedInStore(cid.value(), oldSeqno));

    auto& stats = getStatsAndMaybeSetPersistedHighSeqno(cid.value(), seqno);

    if (oldIsDelete) {
        stats.insert(isSystemEvent, isDelete, size);
    } else if (!oldIsDelete && isDelete) {
        stats.remove(isSystemEvent, size - oldSize);
    } else {
        stats.update(size - oldSize);
    }
}

void Flush::setDroppedCollectionsForStore(
        const std::vector<Collections::KVStore::DroppedCollection>& v) {
    for (const auto& c : v) {
        droppedInStore.emplace(c.collectionId, c);
    }
}

bool Flush::isLogicallyDeleted(CollectionID cid, uint64_t seqno) const {
    auto itr = droppedCollections.find(cid);
    if (itr != droppedCollections.end()) {
        return seqno <= itr->second.endSeqno;
    }
    return false;
}

bool Flush::isLogicallyDeletedInStore(CollectionID cid, uint64_t seqno) const {
    auto itr = droppedInStore.find(cid);
    if (itr != droppedInStore.end()) {
        return seqno <= itr->second.endSeqno;
    }
    return false;
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
                uint32_t{meta.sid},
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
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Dropped>> output;

    // Add Collection's to this set only if they are in both old and new
    // sets of dropped collections - this ensures we generate it once into
    // the new output.
    std::unordered_set<CollectionID> skip;

    // Iterate through the existing dropped collections and look each up in the
    // commit metadata. If the collection is in both lists, we will just update
    // the existing data (adjusting the endSeqno) and then erase the collection
    // from the commit meta's dropped collections.
    for (auto& collection : existingDropped) {
        if (auto itr = droppedCollections.find(collection.collectionId);
            itr != droppedCollections.end()) {
            // Collection is in both old and new 'sets' of dropped collections
            // we only want it in the output once - update the end-seqno here
            // and add to skip set
            collection.endSeqno = itr->second.endSeqno;
            skip.emplace(collection.collectionId);
        }
        auto newEntry = Collections::KVStore::CreateDropped(
                builder,
                collection.startSeqno,
                collection.endSeqno,
                uint32_t(collection.collectionId));
        output.push_back(newEntry);
    }

    // Now add the newly dropped collections
    // Iterate through the set of collections dropped in the commit batch and
    // and create flatbuffer versions of each one
    for (const auto& [cid, dropped] : droppedCollections) {
        (void)cid;
        if (skip.count(cid) > 0) {
            // This collection is already in output
            continue;
        }
        auto newEntry = Collections::KVStore::CreateDropped(
                builder,
                dropped.startSeqno,
                dropped.endSeqno,
                uint32_t(dropped.collectionId));
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
                uint32_t{meta.sid},
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
