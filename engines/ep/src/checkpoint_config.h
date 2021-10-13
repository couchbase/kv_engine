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
    // Test only
    CheckpointConfig();

    CheckpointConfig(rel_time_t period,
                     size_t max_items,
                     size_t max_ckpts,
                     bool item_based_new_ckpt,
                     bool persistence_enabled,
                     CheckpointRemoval checkpoint_removal_mode);

    explicit CheckpointConfig(EventuallyPersistentEngine& e);

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

protected:
    friend class EventuallyPersistentEngine;
    friend class SynchronousEPEngine;

    /**
     * Helper constructor from config. Only called by other constructors.
     */
    explicit CheckpointConfig(Configuration& config);

    bool validateCheckpointMaxItemsParam(size_t checkpoint_max_items);
    bool validateCheckpointPeriodParam(size_t checkpoint_period);

    void setCheckpointPeriod(size_t value);
    void setCheckpointMaxItems(size_t value);
    void setMaxCheckpoints(size_t value);

    void allowItemNumBasedNewCheckpoint(bool value) {
        itemNumBasedNewCheckpoint = value;
    }

    static void addConfigChangeListener(EventuallyPersistentEngine& engine);

private:
    class ChangeListener;

    // Period of a checkpoint in terms of time in sec
    rel_time_t checkpointPeriod;

    // Number of max items allowed in each checkpoint
    size_t checkpointMaxItems;

    // Number of max checkpoints allowed
    size_t maxCheckpoints;

    // Flag indicating if a new checkpoint is created once the number of items
    // in the current checkpoint is greater than the max number allowed.
    bool itemNumBasedNewCheckpoint;

    // Flag indicating if persistence is enabled.
    bool persistenceEnabled;

    CheckpointRemoval checkpointRemovalMode = CheckpointRemoval::Eager;
};
