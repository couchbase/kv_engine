/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "checkpoint_iterator.h"
#include "checkpoint_types.h"
#include "storeddockey_fwd.h"

/**
 * A checkpoint cursor, representing the current position in a Checkpoint
 * series.
 *
 * CheckpointCursors are similar to STL-style iterators but for Checkpoints.
 * A consumer (DCP or persistence) will have one CheckpointCursor, initially
 * positioned at the first item they want. As they read items from the
 * Checkpoint the Cursor is advanced, allowing them to continue from where
 * they left off when they next attempt to read items.
 *
 * A CheckpointCursor has two main pieces of state:
 *
 * - currentCheckpoint - The current Checkpoint the cursor is operating on.
 * - currentPos - the position with the current Checkpoint.
 *
 * When a CheckpointCursor reaches the end of Checkpoint, the CheckpointManager
 * will move it to the next Checkpoint.
 *
 */
class CheckpointCursor {
    friend class CheckpointManager;
    friend class Checkpoint;
    friend class MockCheckpointManager;
    friend class CheckpointCursorIntrospector;

public:
    enum class Droppable : uint8_t { No, Yes };

    /**
     * @param name String representation of the cursor
     * @param checkpoint Iterator to the checkpoint where the cursor is placed
     * @param pos Item position within the checkpoint
     * @param droppable Whether checkpoint memory recovery can drop this cursor
     * @param distance Distance from checkpoint begin
     */
    CheckpointCursor(std::string n,
                     CheckpointList::iterator checkpoint,
                     ChkptQueueIterator pos,
                     Droppable droppable,
                     size_t distance);

    // The implicitly generated copy-ctor would miss to increment the
    // checkpoint-cursor count, so we make sure that nobody can accidentally
    // invoke it.
    CheckpointCursor(const CheckpointCursor& other) = delete;
    CheckpointCursor& operator=(const CheckpointCursor& other) = delete;

    /**
     * Construct by copy and assign the new name.
     *
     * @param other
     * @param name The new name
     */
    CheckpointCursor(const CheckpointCursor& other, std::string name);

    ~CheckpointCursor();

    /// @returns the id of the current checkpoint the cursor is on
    uint64_t getId() const;

    /// @returns the type of the Checkpoint that the cursor is in
    CheckpointType getCheckpointType() const;

    /**
     * Invalidates this cursor. After invalidating this cursor it should not be
     * used.
     */
    void invalidate();

    bool valid() const {
        return isValid;
    }

    const StoredDocKey& getKey() const;

    bool isDroppable() const {
        return droppable == Droppable::Yes;
    }

    void setDistance(size_t val) {
        distance = val;
    }

    void decrDistance() {
        --distance;
    }

    size_t getDistance() const {
        return distance;
    }

    ChkptQueueIterator getPos() const {
        return currentPos;
    }

    /**
     * Repositions this cursor to the given checkpoint's begin.
     *
     * @param checkpointIt Checkpoint identified by the iterator to the CM
     *  checkpoint-list
     */
    void reposition(CheckpointList::iterator checkpointIt);

private:
    /**
     * Move the cursor's iterator back one if it is not currently pointing to
     * begin.  If pointing to begin then do nothing.
     */
    void decrPos();

    /**
     * Move the cursor's iterator forward. NOP if pointing to end.
     */
    void incrPos();

    /*
     * Calculate the number of items (excluding meta-items) remaining to be
     * processed in the checkpoint the cursor is currently in.
     *
     * @return number of items remaining to be processed.
     */
    size_t getRemainingItemsCount() const;

    std::string name;
    CheckpointList::iterator currentCheckpoint;

    // Specify the current position in the checkpoint
    ChkptQueueIterator currentPos;

    // Number of times a cursor has been moved or processed.
    std::atomic<size_t> numVisits;

    /**
     * Is the cursor pointing to a valid checkpoint
     */
    bool isValid = true;

    /// Indicates whether checkpoint memory recovery can drop this cursor
    const Droppable droppable;

    // Distance from the begin of the current checkpoint
    cb::NonNegativeCounter<size_t> distance{0};

    friend bool operator<(const CheckpointCursor& a, const CheckpointCursor& b);
    friend std::ostream& operator<<(std::ostream& os,
                                    const CheckpointCursor& c);
};

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c);