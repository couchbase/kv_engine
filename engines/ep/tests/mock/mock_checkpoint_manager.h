/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#pragma once

#include "checkpoint.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"

/*
 * Mock of the CheckpointManager class.
 */
class MockCheckpointManager : public CheckpointManager {
public:
    MockCheckpointManager(EPStats& st,
                          Vbid vbucket,
                          CheckpointConfig& config,
                          int64_t lastSeqno,
                          uint64_t lastSnapStart,
                          uint64_t lastSnapEnd,
                          uint64_t maxVisibleSeqno,
                          FlusherCallback cb)
        : CheckpointManager(st,
                            vbucket,
                            config,
                            lastSeqno,
                            lastSnapStart,
                            lastSnapEnd,
                            maxVisibleSeqno,
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
        LockHolder lh(queueLock);
        static StoredDocKey emptyKey("", CollectionID::System);
        if (!cursor) {
            queued_item qi(
                    new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
            return qi;
        }
        if (getOpenCheckpointId_UNLOCKED(lh) == 0) {
            queued_item qi(
                    new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
            return qi;
        }

        if (incrCursor(*cursor)) {
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
        LockHolder lh(queueLock);
        return cursors.size();
    }

    size_t getNumCheckpoints() const {
        LockHolder lh(queueLock);
        return checkpointList.size();
    }

    const CheckpointList& getCheckpointList() const {
        return checkpointList;
    }

    queued_item public_createCheckpointItem(uint64_t id,
                                            Vbid vbid,
                                            queue_op checkpoint_op) {
        LockHolder lh(queueLock);
        return createCheckpointItem(id, vbid, checkpoint_op);
    }

    void resetConfig(CheckpointConfig& c) {
        checkpointConfig = c;
    }

    void forceNewCheckpoint() {
        LockHolder lh(queueLock);
        checkOpenCheckpoint_UNLOCKED(lh, true, 0);
    }

    bool incrCursor(CheckpointCursor& cursor) {
        return CheckpointManager::incrCursor(cursor);
    }

    CheckpointType getOpenCheckpointType() const {
        LockHolder lh(queueLock);
        return getOpenCheckpoint_UNLOCKED(lh).getCheckpointType();
    }
};
