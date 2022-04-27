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

#include "checkpoint_config.h"

#include "bucket_logger.h"
#include "configuration.h"
#include "ep_engine.h"

/**
 * A listener class to update checkpoint related configs at runtime.
 */
class CheckpointConfig::ChangeListener : public ValueChangedListener {
public:
    explicit ChangeListener(CheckpointConfig& c) : config(c) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key.compare("max_checkpoints") == 0) {
            config.setMaxCheckpoints(value);
        }
    }

private:
    CheckpointConfig& config;
};

CheckpointConfig::CheckpointConfig(Configuration& config)
    : maxCheckpoints(config.getMaxCheckpoints()),
      persistenceEnabled(config.getBucketType() == "persistent"),
      checkpointMaxSize(config.getCheckpointMaxSize()) {
}

void CheckpointConfig::addConfigChangeListener(
        EventuallyPersistentEngine& engine) {
    Configuration& configuration = engine.getConfiguration();
    configuration.addValueChangedListener(
            "max_checkpoints",
            std::make_unique<ChangeListener>(engine.getCheckpointConfig()));
}

void CheckpointConfig::setMaxCheckpoints(size_t value) {
    Expects(value >= 2);
    maxCheckpoints = value;
}

void CheckpointConfig::setCheckpointMaxSize(size_t value) {
    Expects(value >= 1);
    checkpointMaxSize = value;
}
