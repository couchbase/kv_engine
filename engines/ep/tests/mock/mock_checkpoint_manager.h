/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "checkpoint.h"
#include "checkpoint_config.h"
#include "checkpoint_cursor.h"
#include "checkpoint_manager.h"

/*
 * Mock of the CheckpointManager class.
 */
class MockCheckpointManager : public CheckpointManager {
public:
    MockCheckpointManager(EPStats& st,
                          VBucket& vbucket,
                          CheckpointConfig& config,
                          int64_t lastSeqno,
                          uint64_t lastSnapStart,
                          uint64_t lastSnapEnd,
                          uint64_t maxVisibleSeqno,
                          uint64_t maxPrepareSeqno,
                          FlusherCallback cb)
        : CheckpointManager(st,
                            vbucket,
                            config,
                            lastSeqno,
                            lastSnapStart,
                            lastSnapEnd,
                            maxVisibleSeqno,
                            maxPrepareSeqno,
                            cb) {
    }

    /**
     * Return the next item to be sent to a given connection.
     * @param cursor  pointer to the clients cursor, can be null
     * @param isLastMutationItem  flag indicating if the item to be returned is
     * the last mutation one in the closed checkpoint.
     * @return the next item to be sent to a given connection.
     */
    queued_item nextItem(CheckpointCursor* cursor, bool& isLastMutationItem) {
        std::lock_guard<std::mutex> lh(queueLock);
        static StoredDocKey emptyKey("", CollectionID::SystemEvent);
        if (!cursor) {
            queued_item qi(
                    new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
            return qi;
        }
        if (getOpenCheckpointId(lh) == 0) {
            queued_item qi(
                    new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
            return qi;
        }

        if (CheckpointManager::incrCursor(lh, *cursor)) {
            isLastMutationItem = isLastMutationItemInCheckpoint(*cursor);
            return *(cursor->currentPos);
        } else {
            isLastMutationItem = false;
            queued_item qi(
                    new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
            return qi;
        }
    }

    size_t getNumOfCursors() const {
        std::lock_guard<std::mutex> lh(queueLock);
        return cursors.size();
    }

    const CheckpointList& getCheckpointList() const {
        return checkpointList;
    }

    queued_item public_createCheckpointMetaItem(uint64_t checkpointId,
                                                queue_op op) {
        std::lock_guard<std::mutex> lh(queueLock);
        return createCheckpointMetaItem(checkpointId, op);
    }

    void forceNewCheckpoint() {
        std::lock_guard<std::mutex> lh(queueLock);
        addNewCheckpoint(lh);
    }

    bool incrCursor(CheckpointCursor& cursor) {
        std::lock_guard<std::mutex> lh(queueLock);
        return CheckpointManager::incrCursor(lh, cursor);
    }

    CheckpointType getOpenCheckpointType() const {
        std::lock_guard<std::mutex> lh(queueLock);
        return getOpenCheckpoint(lh).getCheckpointType();
    }

    auto getPersistenceCursorPos() const {
        std::lock_guard<std::mutex> lh(queueLock);
        return getPersistenceCursor()->currentPos;
    }
};
