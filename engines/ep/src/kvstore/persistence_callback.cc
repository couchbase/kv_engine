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
#include "kvstore.h"
#include "stats.h"
#include "vbucket.h"
#include <utilities/logtags.h>

EPPersistenceCallback::EPPersistenceCallback(EPStats& stats, VBucket& vb)
    : stats(stats), vbucket(vb) {
}

// This callback is invoked for set only.
void EPPersistenceCallback::operator()(const Item& queuedItem,
                                       FlushStateMutation state) {
    using State = FlushStateMutation;

    switch (state) {
    case State::Insert:
    case State::LogicalInsert:
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
        stats.totalPersisted++;

        // Account only committed items in opsCreate/Update and numTotalItems
        if (queuedItem.isCommitted()) {
            switch (state) {
            case State::Insert:
            case State::LogicalInsert:
                ++vbucket.opsCreate;
                vbucket.incrNumTotalItems();
                break;
            case State::Update:
                ++vbucket.opsUpdate;
                break;
            case State::Failed:
                break;
            };
        }

        // All inserts to disk (mutation, prepare, commit,system event) take up
        // space on disk so increment metadata stat.
        switch (state) {
        case State::Insert:
        case State::LogicalInsert:
            vbucket.incrMetaDataDisk(queuedItem);
            break;
        case State::Update:
        case State::Failed:
            break;
        };

        return;
    }
    case FlushStateMutation::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::set: Fatal error in persisting "
                "SET on {} seqno:{}",
                queuedItem.getVBucketId(),
                queuedItem.getBySeqno());
        ++stats.flushFailed;
        ++vbucket.opsReject;
        return;
    }
    folly::assume_unreachable();
}

// This callback is invoked for deletions only.
//
// The boolean indicates whether the underlying storage
// successfully deleted the item.
void EPPersistenceCallback::operator()(const Item& queuedItem,
                                       FlushStateDeletion state) {
    switch (state) {
    case FlushStateDeletion::Delete:
        // We have successfully removed an item from the disk, we
        // may now remove it from the hash table.
        vbucket.deletedOnDiskCbk(queuedItem, true /*deleted*/);
        return;
    case FlushStateDeletion::LogicallyDocNotFound:
    case FlushStateDeletion::DocNotFound: {
        // We have successfully removed an item from the disk, we
        // may now remove it from the hash table.
        vbucket.deletedOnDiskCbk(queuedItem, false /*deleted*/);
        return;
    }
    case FlushStateDeletion::Failed:
        EP_LOG_WARN(
                "PersistenceCallback::del: Fatal error in persisting "
                "DELETE on {} seqno:{}",
                queuedItem.getVBucketId(),
                queuedItem.getBySeqno());
        ++stats.flushFailed;
        ++vbucket.opsReject;
        return;
    }
    folly::assume_unreachable();
}
