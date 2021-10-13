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

#include "checkpoint_iterator.h"
#include "ep_types.h"

#include <boost/container/list.hpp>
#include <platform/memory_tracking_allocator.h>

#include <functional>
#include <list>
#include <memory>

class Checkpoint;
class CheckpointManager;
class VBucket;

// List of Checkpoints used by class CheckpointManager to store Checkpoints for
// a given vBucket.
// We use the boost container (rather that the STL one) because boost provides a
// constant-complexity splice function that unfortunately std-c++ lacks.
// Splice is used in the CM code in multiple places under CM lock, so we cannot
// afford that to be O(N) as that degrades frontend throughput when the CM list
// is large.
using CheckpointList = boost::container::list<std::unique_ptr<Checkpoint>>;

// List is used for queueing mutations as vector incurs shift operations for
// de-duplication.  We template the list on a queued_item and our own
// memory allocator which allows memory usage to be tracked.
// Boost list rather than std list for the same reason as for CheckpointList.
using CheckpointQueue =
        boost::container::list<queued_item,
                               MemoryTrackingAllocator<queued_item>>;

class Vbid;
/**
 * Callback function invoked when a checkpoint becomes unreferenced; used
 * to trigger background deletion.
 */
using CheckpointDisposer =
        std::function<void(CheckpointList&&, const Vbid& vbid)>;

/**
 * As the CheckpointList has already been spliced by the time the disposer is
 * invoked, doing nothing here just leads to the checkpoints being destroyed
 * "inline" rather than in a background task.
 */
const CheckpointDisposer ImmediateCkptDisposer = [](CheckpointList&&,
                                                    const Vbid&) {};

/**
 * RAII resource, used to reset the state of the CheckpointManager after
 * flush.
 * An instance of this is returned from getItemsForPersistence(). The
 * callback is triggered as soon as the instance goes out of scope.
 */
class FlushHandle {
public:
    explicit FlushHandle(CheckpointManager& m) : manager(m) {
    }

    ~FlushHandle();

    FlushHandle(const FlushHandle&) = delete;
    FlushHandle& operator=(const FlushHandle&) = delete;
    FlushHandle(FlushHandle&&) = delete;
    FlushHandle& operator=(FlushHandle&&) = delete;

    // Signal that the flush has failed, we will release resources
    // accordingly in the dtor.
    void markFlushFailed(VBucket& vb) {
        failed = true;
        vbucket = &vb;
    }

private:
    bool failed = false;

    // VBucket to update stats post flush failure. Ptr because we'd have to pass
    // the VBucket into the depths of the CheckpointManger to set a reference on
    // construction
    VBucket* vbucket;
    CheckpointManager& manager;
};

using UniqueFlushHandle = std::unique_ptr<FlushHandle>;

// Iterator for the Checkpoint queue.  The iterator is templated on the
// queue type (CheckpointQueue).
using ChkptQueueIterator = CheckpointIterator<CheckpointQueue>;

// Flag from configuration indicating whether checkpoints should be removed
// as soon as they become eligible* ("eager"), or if they should be allowed to
// remain in the manager until other conditions are met e.g., reaching the
// checkpoint quota.
// *checkpoint is the oldest checkpoint, and is closed and not referenced by
//  any cursors.
enum class CheckpointRemoval : uint8_t {
    // Remove checkpoints as soon as possible
    Eager,
    // Leave checkpoints in memory until removal is triggered by memory usage
    Lazy
};