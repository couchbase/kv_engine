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

/**
 * A class containing the config parameters for checkpoint.
 */

class CheckpointConfig {
public:
    explicit CheckpointConfig(Configuration& config);

    size_t getMaxCheckpoints() const {
        return maxCheckpoints;
    }

    bool isPersistenceEnabled() const {
        return persistenceEnabled;
    }

    size_t getCheckpointMaxSize() const {
        return checkpointMaxSize;
    }

    void setCheckpointMaxSize(size_t value);

protected:
    friend class EventuallyPersistentEngine;
    friend class SynchronousEPEngine;

    void setMaxCheckpoints(size_t value);

    static void addConfigChangeListener(EventuallyPersistentEngine& engine);

private:
    class ChangeListener;

    // Number of max checkpoints allowed.
    // Dynamic in EPConfig
    std::atomic<size_t> maxCheckpoints;

    // Flag indicating if persistence is enabled.
    const bool persistenceEnabled;

    /**
     * Maximum size (in bytes) for a single checkpoint.
     */
    std::atomic<size_t> checkpointMaxSize;
};
