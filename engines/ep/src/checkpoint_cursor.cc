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

#include "checkpoint_cursor.h"

#include "checkpoint.h"
#include <gsl/gsl-lite.hpp>
#include <utility>

CheckpointCursor::CheckpointCursor(std::string name,
                                   CheckpointList::iterator checkpoint,
                                   ChkptQueueIterator pos,
                                   Droppable droppable,
                                   size_t distance)
    : name(std::move(name)),
      currentCheckpoint(checkpoint),
      currentPos(std::move(pos)),
      numVisits(0),
      droppable(droppable),
      distance(distance),
      itemLinePosition((*currentCheckpoint)->getPositionOnItemLine() +
                       distance) {
    (*currentCheckpoint)->incNumOfCursorsInCheckpoint();
}

CheckpointCursor::CheckpointCursor(const CheckpointCursor& other,
                                   std::string name)
    : name(std::move(name)),
      currentCheckpoint(other.currentCheckpoint),
      currentPos(other.currentPos),
      numVisits(other.numVisits.load()),
      isValid(other.isValid),
      droppable(other.droppable),
      distance(other.distance),
      itemLinePosition(other.itemLinePosition) {
    if (isValid) {
        (*currentCheckpoint)->incNumOfCursorsInCheckpoint();
    }
}

CheckpointCursor::~CheckpointCursor() {
    if (isValid) {
        (*currentCheckpoint)->decNumOfCursorsInCheckpoint();
    }
}

void CheckpointCursor::invalidate() {
    (*currentCheckpoint)->decNumOfCursorsInCheckpoint();
    isValid = false;
}

DocKeyView CheckpointCursor::getKey() const {
    return (*currentPos)->getKey();
}

void CheckpointCursor::repositionAtCheckpointBegin(
        CheckpointList::iterator checkpointIt) {
    // Remove this cursor from the accounting of it's old checkpoint.
    (*currentCheckpoint)->decNumOfCursorsInCheckpoint();

    currentCheckpoint = checkpointIt;
    currentPos = (*checkpointIt)->begin();
    distance = 0;

    // Update the new checkpoint accounting
    (*checkpointIt)->incNumOfCursorsInCheckpoint();
}

void CheckpointCursor::repositionAtCheckpointStart(
        CheckpointList::iterator checkpointIt) {
    repositionAtCheckpointBegin(checkpointIt);

    // Move currentPos and distance, but do not move the itemLinePosition as
    // the usage of repositionAtCheckpointStart is not consuming items so we
    // do not expect ::getNumItems to change value (reduce)
    ++currentPos;
    ++distance;

    Ensures((*currentPos)->getOperation() == queue_op::checkpoint_start);
    Ensures(distance == 1);
}

void CheckpointCursor::decrPos() {
    Expects(currentPos != (*currentCheckpoint)->begin());
    if (currentPos != (*currentCheckpoint)->end()) {
        // currentPos is on something in the checkpoint, but don't expect to
        // decrement from empty
        Expects((*currentPos)->getOperation() != queue_op::empty);
    }
    --currentPos;
    --distance;
    --itemLinePosition;
}

void CheckpointCursor::incrPos() {
    Expects(currentPos != (*currentCheckpoint)->end());
    // increment position on the item-line but not when advancing past the
    // checkpoint_end
    if ((*currentPos)->getOperation() != queue_op::checkpoint_end) {
        ++itemLinePosition;
    }
    ++currentPos;
    ++distance;
}

size_t CheckpointCursor::getRemainingItemsInCurrentCheckpoint() const {
    return (*currentCheckpoint)->getNumItems() - distance;
}

bool operator<(const CheckpointCursor& a, const CheckpointCursor& b) {
    // Compare currentCheckpoint, bySeqno, and finally distance from start of
    // currentCheckpoint.
    // Given the underlying iterator (CheckpointCursor::currentPos) is a
    // std::list iterator, it is O(N) to compare iterators directly.
    // Therefore bySeqno (integer) initially, only falling back to iterator
    // comparison if two CheckpointCursors have the same bySeqno.
    const auto a_id = (*a.currentCheckpoint)->getId();
    const auto b_id = (*b.currentCheckpoint)->getId();
    if (a_id < b_id) {
        return true;
    }
    if (a_id > b_id) {
        return false;
    }

    // Same checkpoint; check bySeqno
    const auto a_bySeqno = (*a.currentPos)->getBySeqno();
    const auto b_bySeqno = (*b.currentPos)->getBySeqno();
    if (a_bySeqno < b_bySeqno) {
        return true;
    }
    if (a_bySeqno > b_bySeqno) {
        return false;
    }

    // Same checkpoint and seqno, use distance from start of checkpoint.
    return a.distance < b.distance;
}

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c) {
    os << "CheckpointCursor[" << &c << "] with"
       << " name:" << c.name
       << " currentCkpt:{id:" << (*c.currentCheckpoint)->getId()
       << " state:" << to_string((*c.currentCheckpoint)->getState())
       << "} currentSeq:" << (*c.currentPos)->getBySeqno()
       << " currentOp:" << to_string((*c.currentPos)->getOperation())
       << " distance:" << c.distance
       << " itemLinePosition:" << c.itemLinePosition;
    return os;
}
