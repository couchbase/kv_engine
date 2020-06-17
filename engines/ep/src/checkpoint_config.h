/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <memcached/types.h>

class EventuallyPersistentEngine;

/**
 * A class containing the config parameters for checkpoint.
 */

class CheckpointConfig {
public:
    CheckpointConfig();

    CheckpointConfig(rel_time_t period,
                     size_t max_items,
                     size_t max_ckpts,
                     bool item_based_new_ckpt,
                     bool keep_closed_ckpts,
                     bool persistence_enabled);

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

    bool canKeepClosedCheckpoints() const {
        return keepClosedCheckpoints;
    }

    bool isPersistenceEnabled() const {
        return persistenceEnabled;
    }

protected:
    friend class CheckpointConfigChangeListener;
    friend class EventuallyPersistentEngine;

    bool validateCheckpointMaxItemsParam(size_t checkpoint_max_items);
    bool validateCheckpointPeriodParam(size_t checkpoint_period);
    bool validateMaxCheckpointsParam(size_t max_checkpoints);

    void setCheckpointPeriod(size_t value);
    void setCheckpointMaxItems(size_t value);
    void setMaxCheckpoints(size_t value);

    void allowItemNumBasedNewCheckpoint(bool value) {
        itemNumBasedNewCheckpoint = value;
    }

    void allowKeepClosedCheckpoints(bool value) {
        keepClosedCheckpoints = value;
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
    // in the current
    // checkpoint is greater than the max number allowed.
    bool itemNumBasedNewCheckpoint;
    // Flag indicating if closed checkpoints should be kept in memory if the
    // current memory usage
    // below the high water mark.
    bool keepClosedCheckpoints;

    // Flag indicating if persistence is enabled.
    bool persistenceEnabled;
};
