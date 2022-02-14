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
#pragma once

#include "checkpoint.h"
#include "checkpoint_manager.h"

/**
 * Stateless class used to gain privileged access into CheckpointManager for
 * testing purposes. This is a friend class of CheckpointManager.
 */
class CheckpointManagerTestIntrospector {
public:
    static const CheckpointList& public_getCheckpointList(
            const CheckpointManager& checkpointManager) {
        return checkpointManager.checkpointList;
    }

    static CheckpointQueue& public_getOpenCheckpointQueue(
            const CheckpointManager& checkpointManager) {
        return checkpointManager.checkpointList.back()->toWrite;
    }

    static Checkpoint& public_getOpenCheckpoint(
            const CheckpointManager& manager) {
        return *manager.checkpointList.back();
    }

    static std::vector<std::string> getNonMetaItemKeys(
            const Checkpoint& checkpoint) {
        std::vector<std::string> keys;
        for (const auto& item : checkpoint.toWrite) {
            if (!item->isCheckPointMetaItem()) {
                keys.push_back(item->getKey().to_string());
            }
        }
        return keys;
    }

    static void setOpenCheckpointType(CheckpointManager& checkpointManager,
                                      CheckpointType type) {
        auto& checkpoint = *checkpointManager.checkpointList.back();
        Expects(checkpoint.getState() == CHECKPOINT_OPEN);
        checkpoint.setCheckpointType(type);
    }

    static size_t getCheckpointNumIndexEntries(const Checkpoint& checkpoint) {
        return checkpoint.committedKeyIndex.size() +
               checkpoint.preparedKeyIndex.size();
    }
};

/**
 * Stateless class used to gain privileged access into CheckpointCursor for
 * testing purposes. This is a friend class of CheckpointCursor.
 */
class CheckpointCursorIntrospector {
public:
    static const auto& getCurrentPos(const CheckpointCursor& cursor) {
        return cursor.currentPos;
    }

    static const auto& incrPos(CheckpointCursor& cursor) {
        cursor.incrPos();
        return cursor.currentPos;
    }
};
