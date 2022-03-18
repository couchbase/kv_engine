/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "checkpoint_types.h"
#include <memcached/types.h>

class Configuration;
class EventuallyPersistentEngine;

std::string to_string(CheckpointRemoval mode);

/**
 * A class containing the config parameters for checkpoint.
 */

class CheckpointConfig {
public:
    explicit CheckpointConfig(Configuration& config);

    rel_time_t getCheckpointPeriod() const {
        return checkpointPeriod;
    }

    size_t getCheckpointMaxItems() const {
        return checkpointMaxItems;
    }

    size_t getMaxCheckpoints() const {
        return maxCheckpoints;
    }

    bool isItemNumBasedNewCheckpoint() const {
        return itemNumBasedNewCheckpoint;
    }

    bool isPersistenceEnabled() const {
        return persistenceEnabled;
    }

    bool isEagerCheckpointRemoval() const {
        return checkpointRemovalMode == CheckpointRemoval::Eager;
    }

    CheckpointRemoval getCheckpointRemoval() const {
        return checkpointRemovalMode;
    }

    // @todo: Test only. Remove as soon as param made dynamic in EPConfig.
    void setCheckpointRemovalMode(CheckpointRemoval mode) {
        checkpointRemovalMode = mode;
    }

protected:
    friend class EventuallyPersistentEngine;
    friend class SynchronousEPEngine;

    void setCheckpointPeriod(size_t value);
    void setCheckpointMaxItems(size_t value);
    void setMaxCheckpoints(size_t value);

    void allowItemNumBasedNewCheckpoint(bool value) {
        itemNumBasedNewCheckpoint = value;
    }

    static void addConfigChangeListener(EventuallyPersistentEngine& engine);

private:
    class ChangeListener;

    // Period of a checkpoint in terms of time in seconds.
    // Dynamic in EPConfig
    std::atomic<rel_time_t> checkpointPeriod;

    // Number of max items allowed in each checkpoint.
    // Dynamic in EPConfig
    std::atomic<size_t> checkpointMaxItems;

    // Number of max checkpoints allowed.
    // Dynamic in EPConfig
    std::atomic<size_t> maxCheckpoints;

    // Flag indicating if a new checkpoint is created once the number of items
    // in the current checkpoint is greater than the max number allowed.
    // Dynamic in EPConfig
    std::atomic<bool> itemNumBasedNewCheckpoint;

    // Flag indicating if persistence is enabled.
    bool persistenceEnabled;

    CheckpointRemoval checkpointRemovalMode;
};
