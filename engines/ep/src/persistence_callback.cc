/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "persistence_callback.h"

#include "bucket_logger.h"
#include "item.h"
#include "stats.h"

PersistenceCallback::PersistenceCallback(const queued_item& qi, uint64_t c)
    : queuedItem(qi), cas(c) {
}

PersistenceCallback::~PersistenceCallback() = default;

// This callback is invoked for set only.
void PersistenceCallback::operator()(
        TransactionContext& txCtx,
        KVStore::MutationSetResultState mutationResult) {
    auto& epCtx = dynamic_cast<EPTransactionContext&>(txCtx);
    auto& vbucket = epCtx.vbucket;

    switch (mutationResult) {
    case KVStore::MutationSetResultState::Insert:
    case KVStore::MutationSetResultState::Update: {
        auto handle = vbucket.lockCollections(queuedItem->getKey());
        auto res = vbucket.fetchValidValue(
                WantsDeleted::Yes,
                TrackReference::No,
                handle.valid() ? QueueExpired::Yes : QueueExpired::No,
                handle);
        auto* v = res.storedValue;
        if (v) {
            if (v->getCas() == cas) {
                // mark this item clean only if current and stored cas
                // value match
                v->markClean();
            }
            if (mutationResult == KVStore::MutationSetResultState::Insert) {
                // Insert in value-only or full eviction mode.
                ++vbucket.opsCreate;
                vbucket.incrNumTotalItems();
                vbucket.incrMetaDataDisk(*queuedItem);
            } else {
                // Update in value-only or full eviction mode.
                ++vbucket.opsUpdate;
            }
        }

        vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
        --epCtx.stats.diskQueueSize;
        epCtx.stats.totalPersisted++;
        return;
    }
    case KVStore::MutationSetResultState::DocNotFound: {
        auto handle = vbucket.lockCollections(queuedItem->getKey());
        auto res = vbucket.fetchValidValue(
                WantsDeleted::Yes,
                TrackReference::No,
                handle.valid() ? QueueExpired::Yes : QueueExpired::No,
                handle);
        if (res.storedValue) {
            EP_LOG_WARN(
                    "PersistenceCallback::callback: Persisting on "
                    "{}, seqno:{} returned: {}",
                    queuedItem->getVBucketId(),
                    res.storedValue->getBySeqno(),
                    to_string(mutationResult));
        } else {
            EP_LOG_WARN(
                    "PersistenceCallback::callback: Error persisting, a key"
                    "is missing from {}",
                    queuedItem->getVBucketId());
        }

        vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
        --epCtx.stats.diskQueueSize;
        return;
    }
    case KVStore::MutationSetResultState::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::callback: Fatal error in persisting "
                "SET on {}",
                queuedItem->getVBucketId());
        redirty(epCtx.stats, vbucket);
        return;
    }
    folly::assume_unreachable();
}

// This callback is invoked for deletions only.
//
// The boolean indicates whether the underlying storage
// successfully deleted the item.
void PersistenceCallback::operator()(TransactionContext& txCtx,
                                     KVStore::MutationStatus deleteStatus) {
    auto& epCtx = dynamic_cast<EPTransactionContext&>(txCtx);
    auto& vbucket = epCtx.vbucket;

    switch (deleteStatus) {
    case KVStore::MutationStatus::Success:
    case KVStore::MutationStatus::DocNotFound: {
        // We have successfully removed an item from the disk, we
        // may now remove it from the hash table.
        bool deleted = deleteStatus == KVStore::MutationStatus::Success;
        vbucket.deletedOnDiskCbk(*queuedItem, deleted);
        return;
    }
    case KVStore::MutationStatus::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::callback: Fatal error in persisting "
                "DELETE on {}",
                queuedItem->getVBucketId());
        redirty(epCtx.stats, vbucket);
        return;
    }
    folly::assume_unreachable();
}

void PersistenceCallback::redirty(EPStats& stats, VBucket& vbucket) {
    if (vbucket.isDeletionDeferred()) {
        // updating the member stats for the vbucket is not really necessary
        // as the vbucket is about to be deleted
        vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
        // the following is a global stat and so is worth updating
        --stats.diskQueueSize;
        return;
    }
    ++stats.flushFailed;
    vbucket.markDirty(queuedItem->getKey());
    vbucket.rejectQueue.push(queuedItem);
    ++vbucket.opsReject;
}
