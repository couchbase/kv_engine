/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

    /// (meta item) Testing only op, used to mark the end of a test.
    /// TODO: Remove this, it shouldn't be necessary / included just to support
    /// testing.
    flush,

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
    /// checkpoints, where it is always the last item in the checkpoint.
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
