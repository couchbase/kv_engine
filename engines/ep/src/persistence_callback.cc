/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "persistence_callback.h"

#include "bucket_logger.h"
#include "item.h"
#include "stats.h"
#include "vbucket.h"
#include <utilities/logtags.h>

PersistenceCallback::PersistenceCallback() {
}

PersistenceCallback::~PersistenceCallback() = default;

// This callback is invoked for set only.
void PersistenceCallback::operator()(EPTransactionContext& epCtx,
                                     const Item& queuedItem,
                                     KVStore::FlushStateMutation state) {
    auto& vbucket = epCtx.vbucket;

    using State = KVStore::FlushStateMutation;

    switch (state) {
    case State::Insert:
    case State::Update: {
        // Mark clean, only if the StoredValue has the same CommittedState and
        // and Seqno (MB-39280) as the persisted item.
        {
            auto res = vbucket.ht.findItem(queuedItem);
            auto* v = res.storedValue;
            if (v && (v->getBySeqno() == queuedItem.getBySeqno())) {
                if (!v->isDirty()) {
                    // MB-41658: Found item _should_ always be dirty, but
                    // crash/warmup tests intermittently fail this check. Dump
                    // additional details to assist in diagnosing issue if it
                    // reoccurs.
                    std::stringstream itemSS;
                    itemSS << queuedItem;
                    std::stringstream svSS;
                    svSS << *v;
                    throw std::logic_error(fmt::format(
                            "PersistenceCallback::operator() - Expected "
                            "resident item matching queuedItem to be dirty!\n"
                            "\tqueuedItem:{}\n\tv:{}",
                            cb::UserData{itemSS.str()},
                            cb::UserData{svSS.str()}));
                }
                v->markClean();
            }
        }
        // Update general flush stats
        epCtx.stats.totalPersisted++;

        // Account only committed items in opsCreate/Update and numTotalItems
        if (queuedItem.isCommitted()) {
            if (state == State::Insert) {
                ++vbucket.opsCreate;
                vbucket.incrNumTotalItems();
            } else {
                ++vbucket.opsUpdate;
            }
        }

        // All inserts to disk (mutation, prepare, commit,system event) take up
        // space on disk so increment metadata stat.
        if (state == State::Insert) {
            vbucket.incrMetaDataDisk(queuedItem);
        }

        return;
    }
    case KVStore::FlushStateMutation::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::set: Fatal error in persisting "
                "SET on {} seqno:{}",
                queuedItem.getVBucketId(),
                queuedItem.getBySeqno());
        ++epCtx.stats.flushFailed;
        ++vbucket.opsReject;
        return;
    }
    folly::assume_unreachable();
}

// This callback is invoked for deletions only.
//
// The boolean indicates whether the underlying storage
// successfully deleted the item.
void PersistenceCallback::operator()(EPTransactionContext& epCtx,
                                     const Item& queuedItem,
                                     KVStore::FlushStateDeletion state) {
    auto& vbucket = epCtx.vbucket;

    switch (state) {
    case KVStore::FlushStateDeletion::Delete:
    case KVStore::FlushStateDeletion::DocNotFound: {
        // We have successfully removed an item from the disk, we
        // may now remove it from the hash table.
        const bool deleted = (state == KVStore::FlushStateDeletion::Delete);
        vbucket.deletedOnDiskCbk(queuedItem, deleted);
        return;
    }
    case KVStore::FlushStateDeletion::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::del: Fatal error in persisting "
                "DELETE on {} seqno:{}",
                queuedItem.getVBucketId(),
                queuedItem.getBySeqno());
        ++epCtx.stats.flushFailed;
        ++vbucket.opsReject;
        return;
    }
    folly::assume_unreachable();
}

EPTransactionContext::EPTransactionContext(EPStats& stats, VBucket& vbucket)
    : TransactionContext(vbucket.getId()), stats(stats), vbucket(vbucket) {
}

void EPTransactionContext::setCallback(const Item& item,
                                       KVStore::FlushStateMutation state) {
    cb(*this, item, state);
}

void EPTransactionContext::deleteCallback(const Item& item,
                                          KVStore::FlushStateDeletion state) {
    cb(*this, item, state);
}
