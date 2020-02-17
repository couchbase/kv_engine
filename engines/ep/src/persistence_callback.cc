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

PersistenceCallback::PersistenceCallback() {
}

PersistenceCallback::~PersistenceCallback() = default;

// This callback is invoked for set only.
void PersistenceCallback::operator()(
        TransactionContext& txCtx,
        queued_item queuedItem,
        KVStore::MutationSetResultState mutationResult) {
    auto& epCtx = dynamic_cast<EPTransactionContext&>(txCtx);
    auto& vbucket = epCtx.vbucket;
    bool isInHashTable = false;
    int64_t hashTableSeqno = -1;

    { // scope for hashtable lock
        auto res = vbucket.ht.findItem(*queuedItem);
        auto* v = res.storedValue;
        if (v) {
            isInHashTable = true;
            hashTableSeqno = v->getBySeqno();
            if (v->getCas() == queuedItem->getCas()) {
                // mark this item clean only if current and stored cas
                // value match
                v->markClean();
            }
        }
    } // end of hashtable lock scope

    switch (mutationResult) {
    case KVStore::MutationSetResultState::Insert: {
        // Only increment on disk count if this value is in the hash table
        // or if the mutation wasn't committed but is an insertion and we
        // didn't find an a valid value for it. Then it must be the first
        // time we're writing a prepare for this key to disk. Thus, treat it
        // as a create.
        // This make sure we don't increment for sets that have been performed
        // for collection manifests
        if (isInHashTable || queuedItem->isPending()) {
            // We should only increment the item counter if we have persisted a
            // committed (by mutation or commit) item
            if (queuedItem->isCommitted()) {
                ++vbucket.opsCreate;
                vbucket.incrNumTotalItems();
            }
            vbucket.incrMetaDataDisk(*queuedItem);
        }

        vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
        --epCtx.stats.diskQueueSize;
        epCtx.stats.totalPersisted++;
        return;
    }
    case KVStore::MutationSetResultState::Update: {
        if (queuedItem->isCommitted()) {
            // ops_update should only count committed, not prepared items.
            ++vbucket.opsUpdate;
        }

        vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
        --epCtx.stats.diskQueueSize;
        epCtx.stats.totalPersisted++;
        return;
    }
    case KVStore::MutationSetResultState::DocNotFound: {
        if (isInHashTable) {
            EP_LOG_WARN(
                    "PersistenceCallback::callback: Persisting on "
                    "{}, seqno:{} returned: {}",
                    queuedItem->getVBucketId(),
                    hashTableSeqno,
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
        redirty(epCtx.stats, vbucket, queuedItem);
        return;
    }
    folly::assume_unreachable();
}

// This callback is invoked for deletions only.
//
// The boolean indicates whether the underlying storage
// successfully deleted the item.
void PersistenceCallback::operator()(TransactionContext& txCtx,
                                     queued_item queuedItem,
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
        redirty(epCtx.stats, vbucket, queuedItem);
        return;
    }
    folly::assume_unreachable();
}

void PersistenceCallback::redirty(EPStats& stats,
                                  VBucket& vbucket,
                                  queued_item queuedItem) {
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
    ++vbucket.opsReject;
}
