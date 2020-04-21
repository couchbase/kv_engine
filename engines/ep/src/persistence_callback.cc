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
        KVStore::FlushStateMutation mutationResult) {
    auto& epCtx = dynamic_cast<EPTransactionContext&>(txCtx);
    auto& vbucket = epCtx.vbucket;

    {
        auto res = vbucket.ht.findItem(*queuedItem);
        auto* v = res.storedValue;
        if (v) {
            if (v->getCas() == queuedItem->getCas()) {
                // mark this item clean only if current and stored cas
                // value match
                v->markClean();
            }
        }
    }

    const auto doGeneralFlushStats = [&vbucket, &queuedItem, &epCtx]() -> void {
        vbucket.doStatsForFlushing(*queuedItem, queuedItem->size());
        --epCtx.stats.diskQueueSize;
        epCtx.stats.totalPersisted++;
    };

    switch (mutationResult) {
    case KVStore::FlushStateMutation::Insert: {
        doGeneralFlushStats();

        // Only count Committed items in numTotalItems.
        if (queuedItem->isCommitted()) {
            ++vbucket.opsCreate;
            vbucket.incrNumTotalItems();
        }

        // All items which are actually flushed to disk (Mutations, prepares,
        // commits, and system events) take up space on disk so increment
        // metadata stat.
        vbucket.incrMetaDataDisk(*queuedItem);
        return;
    }
    case KVStore::FlushStateMutation::Update: {
        doGeneralFlushStats();

        if (queuedItem->isCommitted()) {
            // ops_update should only count committed, not prepared items.
            ++vbucket.opsUpdate;
        }

        return;
    }
    case KVStore::FlushStateMutation::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::set: Fatal error in persisting "
                "SET on {} seqno:{}",
                queuedItem->getVBucketId(),
                queuedItem->getBySeqno());
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
                                     KVStore::FlushStateDeletion state) {
    auto& epCtx = dynamic_cast<EPTransactionContext&>(txCtx);
    auto& vbucket = epCtx.vbucket;

    switch (state) {
    case KVStore::FlushStateDeletion::Delete:
    case KVStore::FlushStateDeletion::DocNotFound: {
        // We have successfully removed an item from the disk, we
        // may now remove it from the hash table.
        const bool deleted = (state == KVStore::FlushStateDeletion::Delete);
        vbucket.deletedOnDiskCbk(*queuedItem, deleted);
        return;
    }
    case KVStore::FlushStateDeletion::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::del: Fatal error in persisting "
                "DELETE on {} seqno:{}",
                queuedItem->getVBucketId(),
                queuedItem->getBySeqno());
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
