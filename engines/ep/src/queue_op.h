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

#include <iosfwd>
#include <string>

/**
 * Forward declarations for types relating to Item.
 */

/// The set of possible operations which can be queued into a checkpoint.
enum class queue_op : uint8_t {
    /// Set a document key to a given value. Sets to the same key can (and
    /// typically are) de-duplicated - only the most recent queue_op::mutation
    /// in a checkpoint will be kept. This means that there's no guarantee that
    /// clients will see all intermediate values of a key. A mutation can also
    /// be a delete (if setDeleted is called), a mutation can be a typical
    /// deletion where an entire key/value is removed or a delete with value
    /// where the key is logically deleted, but some value data remains - xattrs
    /// do just that to preserve system-xattrs.
    mutation,

    /// Set a document key to a given value; but only make the change visible
    /// once it has met it's durability requirements by sufficient replicas
    /// acknowledging it.
    /// Until committed; the Pending Sync Write is not visible to clients nor
    /// to non-replica DCP consumers.
    /// See also - {commit_sync_write}.
    pending_sync_write,

    /// Commit a pending_sync_write; converting it to committed and making
    /// visible to clients. See also - {pending_sync_write}.
    commit_sync_write,

    /// Abort a pending_sync_write. This operations logically removes a pending.
    /// It is both persisted to disk and replicated via DCP.
    abort_sync_write,

    /// (meta item) Dummy op added to the start of checkpoints to simplify
    /// checkpoint logic.
    /// This is because our Checkpoints are structured such that
    /// CheckpointCursors are advanced before dereferencing them, not after -
    /// see Checkpoint documentation for details. As such we need to have an
    /// empty/dummy element at the start of each Checkpoint, so after the first
    /// advance the cursor is pointing at the 'real' first element (normally
    /// checkpoint_start).
    ///
    /// Unlike other operations, queue_op::empty is ignored for the purposes of
    /// CheckpointManager::numItems - due to it only existing as a placeholder.
    empty,

    /// (meta item) Marker for the start of a checkpoint.
    /// All checkpoints (open or closed) will start with an item of this type.
    /// Like all meta items, this doens't directly match user operations, but
    /// is used to delineate the start of a checkpoint.
    checkpoint_start,

    /// (meta item) Marker for the end of a checkpoint. Only exists in closed
    /// checkpoints, where it is always the last item in the checkpoint. The
    /// seqno of the checkpoint_end will be exclusive of the seqnos of all
    /// non-meta operations in the checkpoint (i.e. one greater); this will not
    /// be the case if the preceding item is a set_vbucket_state though (the two
    /// ops will have the same seqno).
    checkpoint_end,

    /// (meta item) Marker to persist the VBucket's state (vbucket_state) to
    /// disk. No data (value) associated with it, simply acts as a marker
    /// to ensure a non-empty persistence queue.
    set_vbucket_state,

    /// System events are created by the code to represent something triggered
    /// by the system, possibly orginating from a user's action.
    /// The flags field of system_event describes the detail along with the key
    /// which must at least be unique per event type.
    /// A system_event can also be deleted.
    system_event
};

/// Return a string representation of queue_op.
std::string to_string(queue_op op);

bool isMetaQueueOp(queue_op op);

/// Print a queue_op to an ostream (for GoogleTest).
std::ostream& operator<<(std::ostream& os, const queue_op& op);
