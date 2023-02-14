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

#include "collections/flush.h"
#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "collections/kvstore_generated.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "item.h"
#include "kvstore/kvstore.h"

namespace Collections::VB {

// Called from KVStore during the flush process and before we consider the
// data of the flush to be committed. This method iterates through the
// statistics gathered by the Flush and uses the std::function callback to
// have the KVStore implementation write them to storage, e.g. a local document.
void Flush::saveCollectionStats(
        std::function<void(CollectionID, const PersistedStats&)> cb) {
    // For each collection modified in the flush run ask the VBM for the
    // current stats (using the high-seqno so we find the correct generation
    // of stats)
    for (const auto& [cid, flushStats] : flushAccounting.getStats()) {
        // Don't generate an update of the statistics for a dropped collection.
        // 1) it's wasted effort
        // 2) the kvstore may not be able handle an update of a 'local' doc
        //    and a delete of the same in one flush (couchstore doesn't)
        auto itr = flushAccounting.getDroppedCollections().find(cid);
        if (itr != flushAccounting.getDroppedCollections().end() &&
            flushStats.getPersistedHighSeqno() < itr->second.endSeqno) {
            continue;
        }

        // Get the current stats of the collection (for the seqno).
        auto collsFlushStats = manifest.get().lock().getStatsForFlush(
                cid, flushStats.getPersistedHighSeqno());

        // Generate new stats, add the deltas from this flush batch for count
        // and size and set the high-seqno (which includes prepares)
        PersistedStats ps(collsFlushStats.itemCount + flushStats.getItemCount(),
                          flushStats.getPersistedHighSeqno(),
                          collsFlushStats.diskSize + flushStats.getDiskSize());
        cb(cid, ps);
    }
}

uint32_t Flush::countNonEmptyDroppedCollections() const {
    uint32_t nonEmpty = 0;
    // For a flush batch that was dropping collections detect and count
    // non-empty collections so we can schedule a purge only if needed (avoids
    // a compaction if the collection is empty)
    for (const auto& [cid, dropped] : flushAccounting.getDroppedCollections()) {
        // An empty collection never had items added to it. From the meta
        // data we have regarding the collection this is evident by having the
        // start-seqno equal to the collection's high-seqno. However for the
        // flusher one corner case exists and that is when the collection was
        // created and dropped in the same flush-batch, then the high-seqno is
        // 0.
        //
        // In the flusher we count how many dropped collections had items and
        // will need a purge.
        //
        // Two scenarios must be considered here:
        // 1) If the collection previously existed on disk and was dropped in
        //    this flush batch
        // 2a) If the collection was created and dropped in this flush batch and
        //     we only persisted the dropped (couchstore)
        // 2b) If the collection was created and dropped in this flush batch and
        //     we persisted both the create and the drop (magma)
        //
        // Two checks are in-place corresponding to the above scenarios:
        // 1) If the 'stats' map doesn't store the dropped 'cid', then this
        // flush batch had no items for the collection, we must inspect the
        // manifest "StatsForFlush" object which stores the state of the dropped
        // collection. From there inspect the high-seqno and start-seqno.
        //
        // 2) If the 'stats' map does store the dropped 'cid', then the
        // collection is empty only if we didn't persist an item belonging to it
        // in this flush batch.
        auto sItr = flushAccounting.getStats().find(cid);

        if (sItr == flushAccounting.getStats().end() ||
            !sItr->second.itemInBatch()) {
            const auto highSeqno =
                    manifest.get()
                            .lock()
                            .getStatsForFlush(cid, dropped.endSeqno)
                            .highSeqno;
            if (highSeqno != 0 && highSeqno != dropped.startSeqno) {
                nonEmpty++; // 1)
            }
        } else {
            nonEmpty++; // 2)
        }
    }
    return nonEmpty;
}

void Flush::forEachDroppedCollection(
        std::function<void(CollectionID)> cb) const {
    flushAccounting.forEachDroppedCollection(cb);
}

// Called from KVStore after a successful commit.
// This method will iterate through all of the collection stats that the Flush
// gathered and attempt to update the VB::Manifest (which is where cmd_stat
// reads from).
void Flush::postCommitMakeStatsVisible() {
    for (const auto& [cid, flushStats] : flushAccounting.getStats()) {
        manifest.get().applyFlusherStats(cid, flushStats);
    }
}

void Flush::flushSuccess(Vbid vbid, EPBucket& bucket) {
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
    for (const auto& [cid, droppedData] :
         flushAccounting.getDroppedCollections()) {
        manifest.get().collectionDropPersisted(cid, droppedData.endSeqno);
    }
}

void Flush::checkAndTriggerPurge(Vbid vbid, EPBucket& bucket) const {
    if (nonEmptyDroppedCollections != 0) {
        triggerPurge(vbid, bucket);
    }
}

void Flush::triggerPurge(Vbid vbid, EPBucket& bucket) {
    // There's no requirement for compaction to run 'now', schedule with a delay
    // which allows for any other drop events in the queue to all end up
    // 'coalesced' into one run of compaction.
    bucket.scheduleCompaction(
            vbid,
            std::chrono::milliseconds(
                    bucket.getEPEngine()
                            .getConfiguration()
                            .getCollectionsDropCompactionDelay()));
}

void Flush::setDroppedCollectionsForStore(
        const std::vector<Collections::KVStore::DroppedCollection>& v) {
    flushAccounting.setDroppedCollectionsForStore(v);
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
    case SystemEvent::ModifyCollection: {
        recordModifyCollection(item);
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
    auto createEvent = Collections::VB::Manifest::getCreateEventData(item);
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

void Flush::recordModifyCollection(const Item& item) {
    // Modify and Create carry the same data - all of the collection meta, hence
    // call to getCreateEventData
    auto createEvent = Collections::VB::Manifest::getCreateEventData(item);
    KVStore::OpenCollection collection{uint64_t(item.getBySeqno()),
                                       createEvent.metaData};
    auto [itr, emplaced] =
            collectionMods.try_emplace(collection.metaData.cid, collection);
    if (!emplaced) {
        // Collection already in the map, only keep this event if >
        if (uint64_t(item.getBySeqno()) > itr->second.startSeqno) {
            itr->second = collection;
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
    auto [itr, emplaced] = flushAccounting.getDroppedCollections().try_emplace(
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
    return builder.Release();
}

// Process the collection commit meta-data to generate the set of open
// collections as flatbuffer data. The inputs to this function are he current
// flatbuffer openCollection data
flatbuffers::DetachedBuffer Flush::encodeOpenCollections(
        cb::const_byte_buffer currentCollections) {
    // The 'array' of collections must be exclusive. Generating a collection
    // vector with duplicates means the data will 'crash' decode. This set and
    // function will prevent bad data being placed on disk.
    std::unordered_set<CollectionID> outputIds;

    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Collection>>
            finalisedOpenCollection;

    auto exclusiveInsertCollection =
            [&outputIds, &finalisedOpenCollection](
                    CollectionID cid,
                    flatbuffers::Offset<Collections::KVStore::Collection>
                            newEntry) {
                auto result = outputIds.emplace(cid);
                if (!result.second) {
                    throw std::logic_error(
                            "encodeOpenCollections: duplicate collection "
                            "detected cid:" +
                            cid.to_string());
                }
                finalisedOpenCollection.push_back(newEntry);
            };

    // For each created collection ensure that we use the most recent (by-seqno)
    // meta-data for the output but only if there is no drop event following.
    for (auto& [cid, span] : collections) {
        using Collections::KVStore::OpenCollection;

        if (auto itr = flushAccounting.getDroppedCollections().find(cid);
            itr != flushAccounting.getDroppedCollections().end()) {
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

        const auto& meta = span.high.metaData;

        // This will set the value based on any modification which may have
        // been flushed
        auto history = getHistorySetting(
                meta.cid, span.high.startSeqno, meta.canDeduplicate);

        // generate
        exclusiveInsertCollection(
                meta.cid,
                Collections::KVStore::CreateCollection(
                        builder,
                        span.high.startSeqno,
                        uint32_t{meta.sid},
                        uint32_t(meta.cid),
                        meta.maxTtl.has_value(),
                        meta.maxTtl.value_or(std::chrono::seconds::zero())
                                .count(),
                        builder.CreateString(meta.name.data(),
                                             meta.name.size()),
                        history));
    }

    // And 'merge' with the data we read
    if (!currentCollections.empty()) {
        KVStore::verifyFlatbuffersData<Collections::KVStore::OpenCollections>(
                currentCollections, "encodeOpenCollections()");
        auto open = flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                currentCollections.data());
        for (const auto* entry : *open->entries()) {
            // For each currently open collection, is it in the dropped map?
            auto result = flushAccounting.getDroppedCollections().find(
                    entry->collectionId());

            // If not found in dropped collections add to output
            if (result == flushAccounting.getDroppedCollections().end()) {
                exclusiveInsertCollection(
                        entry->collectionId(),
                        Collections::KVStore::CreateCollection(
                                builder,
                                entry->startSeqno(),
                                entry->scopeId(),
                                entry->collectionId(),
                                entry->ttlValid(),
                                entry->maxTtl(),
                                builder.CreateString(entry->name()),
                                // getHistorySetting checks for modifications
                                getHistorySetting(entry->collectionId(),
                                                  entry->startSeqno(),
                                                  getCanDeduplicateFromHistory(
                                                          entry->history()))));
            } else {
                // Here we maintain the startSeqno of the dropped collection
                result->second.startSeqno = entry->startSeqno();
            }
        }
    } else if (flushAccounting.getDroppedCollections().count(
                       CollectionID::Default) == 0) {
        // Nothing on disk - and not dropped assume the default collection lives
        exclusiveInsertCollection(
                CollectionID::Default,
                Collections::KVStore::CreateCollection(
                        builder,
                        0,
                        ScopeID::Default,
                        CollectionID::Default,
                        false /* ttl invalid*/,
                        0,
                        builder.CreateString(
                                Collections::DefaultCollectionIdentifier
                                        .data()),
                        getHistorySetting(
                                CollectionID::Default,
                                0,
                                getCanDeduplicateFromHistory(false))));
    }

    auto collectionsVector = builder.CreateVector(finalisedOpenCollection);
    auto toWrite = Collections::KVStore::CreateOpenCollections(
            builder, collectionsVector);

    builder.Finish(toWrite);
    return builder.Release();
}

flatbuffers::DetachedBuffer Flush::encodeDroppedCollections(
        std::vector<Collections::KVStore::DroppedCollection>& existingDropped) {
    nonEmptyDroppedCollections = countNonEmptyDroppedCollections();

    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Dropped>> output;

    // Add Collection's to this set only if they are in both old and new
    // sets of dropped collections - this ensures we generate it once into
    // the new output.
    std::unordered_set<CollectionID> skip;

    // Iterate through the existing dropped collections and look each up in the
    // commit metadata. If the collection is in both lists, we will just update
    // the existing data (adjusting the endSeqno) and then mark as skipped
    for (auto& collection : existingDropped) {
        if (auto itr = flushAccounting.getDroppedCollections().find(
                    collection.collectionId);
            itr != flushAccounting.getDroppedCollections().end()) {
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
    for (const auto& [cid, dropped] : flushAccounting.getDroppedCollections()) {
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

flatbuffers::DetachedBuffer Flush::encodeRelativeComplementOfDroppedCollections(
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        const std::unordered_set<CollectionID>& idsToRemove,
        uint64_t endSeqno) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Dropped>> output;

    // Iterate over the set of dropped collections loaded from the latest
    // snapshot. This includes any collections newly dropped during in replay.
    for (const auto& dc : droppedCollections) {
        // For each collection that is marked as dropped we must do two checks.
        // 1) If the collection is unknown to the eraser's set (which was
        //   populated from the pre-compacted snapshot) then it must remain.
        // 2) If endSeqno of the collection is greater than eraser's endSeqno
        //   this collection is not been fully purged. This occurs in the case
        //   a collection was resurrected and dropped during replay. In this
        //   case the collection must remain in the final output.
        if (idsToRemove.count(dc.collectionId) == 0 || dc.endSeqno > endSeqno) {
            // Include this collection in the final set
            auto newEntry = Collections::KVStore::CreateDropped(
                    builder,
                    dc.startSeqno,
                    dc.endSeqno,
                    uint32_t(dc.collectionId));
            output.push_back(newEntry);
        }
    }

    // If the output vector is empty, return an empty buffer so the caller
    // knows there is actually nothing to store (prefer no data stored vs
    // storing a flatbuffer encoded empty vector)
    if (output.empty()) {
        return {};
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

    // The 'array' of scops must be exclusive. Generating a scope
    // vector with duplicates means the data will 'crash' decode. This set and
    // function will prevent bad data being placed on disk.
    std::unordered_set<ScopeID> outputIds;
    auto exclusiveInsertScope =
            [&outputIds, &openScopes](
                    ScopeID sid,
                    flatbuffers::Offset<Collections::KVStore::Scope> newEntry) {
                // Must be exclusive
                auto result = outputIds.emplace(sid);
                if (!result.second) {
                    throw std::logic_error(
                            "encodeOpenScopes: duplicate scope detected sid:" +
                            sid.to_string());
                }
                openScopes.push_back(newEntry);
            };

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
        exclusiveInsertScope(meta.sid,
                             Collections::KVStore::CreateScope(
                                     builder,
                                     event.startSeqno,
                                     uint32_t{meta.sid},
                                     builder.CreateString(meta.name.data(),
                                                          meta.name.size())));
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
                exclusiveInsertScope(
                        entry->scopeId(),
                        Collections::KVStore::CreateScope(
                                builder,
                                entry->startSeqno(),
                                entry->scopeId(),
                                builder.CreateString(entry->name())));
            }
        }
    } else {
        // Nothing on disk - assume the default scope lives (it always does)
        exclusiveInsertScope(
                ScopeID::Default,
                Collections::KVStore::CreateScope(
                        builder,
                        0, // start-seqno
                        ScopeID::Default,
                        builder.CreateString(
                                Collections::DefaultScopeIdentifier.data())));
    }

    auto vector = builder.CreateVector(openScopes);
    auto final = Collections::KVStore::CreateScopes(builder, vector);
    builder.Finish(final);

    // write back
    return builder.Release();
}

bool Flush::droppedCollectionsExists() const {
    return manifest.get().isDropInProgress();
}

FlushAccounting::StatisticsUpdate Flush::getDroppedStats(CollectionID cid) {
    auto itr = flushAccounting.getDroppedStats().find(cid);
    if (itr == flushAccounting.getDroppedStats().end()) {
        return FlushAccounting::StatisticsUpdate(0);
    }

    return FlushAccounting::StatisticsUpdate(itr->second);
}

const FlushAccounting::StatsMap& Flush::getDroppedStats() const {
    return flushAccounting.getDroppedStats();
}

bool Flush::isOpen(CollectionID cid) const {
    auto collectionsItr = collections.find(cid);
    if (collectionsItr != collections.end()) {
        // Probably open, check dropped

        auto droppedItr = flushAccounting.getDroppedCollections().find(cid);
        if (droppedItr != flushAccounting.getDroppedCollections().end()) {
            return collectionsItr->second.high.startSeqno >
                   droppedItr->second.endSeqno;
        }

        // Opened in this flush
        return true;
    }

    // Not opened in this flush
    return false;
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        IsCommitted isCommitted,
                        IsDeleted isDelete,
                        size_t size,
                        CompactionCallbacks compactionCallbacks) {
    flushAccounting.updateStats(key,
                                seqno,
                                isCommitted,
                                isDelete,
                                size,
                                IsCompaction::No,
                                compactionCallbacks);
}

bool Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        IsCommitted isCommitted,
                        IsDeleted isDelete,
                        size_t size,
                        uint64_t oldSeqno,
                        IsDeleted oldIsDelete,
                        size_t oldSize,
                        CompactionCallbacks compactionCallbacks) {
    return flushAccounting.updateStats(key,
                                       seqno,
                                       isCommitted,
                                       isDelete,
                                       size,
                                       oldSeqno,
                                       oldIsDelete,
                                       oldSize,
                                       IsCompaction::No,
                                       compactionCallbacks);
}

void Flush::setManifest(Manifest& newManifest) {
    manifest = newManifest;
}

VB::Manifest& Flush::getManifest() const {
    return manifest;
}

bool Flush::getHistorySetting(CollectionID cid,
                              uint64_t seqno,
                              CanDeduplicate createSetting) {
    auto modification = collectionMods.find(cid);
    if (modification != collectionMods.end() &&
        modification->second.startSeqno > seqno) {
        return getHistoryFromCanDeduplicate(
                modification->second.metaData.canDeduplicate);
    }
    // No modification, or it was before the create - return input value.
    return getHistoryFromCanDeduplicate(createSetting);
}

} // namespace Collections::VB
