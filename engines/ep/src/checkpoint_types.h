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

#include <list>
#include <memory>

class Checkpoint;
class CheckpointManager;
class VBucket;

// List of Checkpoints used by class CheckpointManager to store Checkpoints for
// a given vBucket.
using CheckpointList = std::list<std::unique_ptr<Checkpoint>>;

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