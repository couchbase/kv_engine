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

#include <list>
#include <memory>

class Checkpoint;
class CheckpointManager;

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
    FlushHandle(CheckpointManager& m) : manager(m) {
    }

    ~FlushHandle();

    FlushHandle(const FlushHandle&) = delete;
    FlushHandle& operator=(const FlushHandle&) = delete;
    FlushHandle(FlushHandle&&) = delete;
    FlushHandle& operator=(FlushHandle&&) = delete;

    // Signal that the flush has failed, we will release resources
    // accordingly in the dtor.
    void markFlushFailed() {
        failed = true;
    }

private:
    bool failed = false;
    CheckpointManager& manager;
};

using UniqueFlushHandle = std::unique_ptr<FlushHandle>;