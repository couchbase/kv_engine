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
};
