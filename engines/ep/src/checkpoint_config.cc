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

std::string to_string(CheckpointRemoval mode) {
    switch (mode) {
    case CheckpointRemoval::Eager:
        return "eager";
    case CheckpointRemoval::Lazy:
        return "lazy";
    }
    return "<unknown>";
}

CheckpointRemoval mode_from_string(std::string_view strMode) {
    if (strMode == "eager") {
        return CheckpointRemoval::Eager;
    } else if (strMode == "lazy") {
        return CheckpointRemoval::Lazy;
    }
    throw std::invalid_argument("Invalid CheckpointRemoval mode: " +
                                std::string(strMode));
}

std::ostream& operator<<(std::ostream& os, const CheckpointRemoval& mode) {
    return os << to_string(mode);
}

/**
 * A listener class to update checkpoint related configs at runtime.
 */
class CheckpointConfig::ChangeListener : public ValueChangedListener {
public:
    explicit ChangeListener(const Configuration& config,
                            CheckpointConfig& ckptConfig)
        : config(config), checkpointConfig(ckptConfig) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "chk_period") {
            checkpointConfig.setCheckpointPeriod(value);
        } else if (key == "chk_max_items") {
            checkpointConfig.setCheckpointMaxItems(value);
        } else if (key == "max_checkpoints") {
            const auto newMaxCheckpoints =
                    value * config.getMaxCheckpointsHardLimitMultiplier();
            checkpointConfig.setMaxCheckpoints(newMaxCheckpoints);
        } else if (key == "max_checkpoints_hard_limit_multiplier") {
            const auto newMaxCheckpoints = config.getMaxCheckpoints() * value;
            checkpointConfig.setMaxCheckpoints(newMaxCheckpoints);
        }
    }

    void booleanValueChanged(const std::string& key, bool value) override {
        if (key.compare("item_num_based_new_chk") == 0) {
            checkpointConfig.allowItemNumBasedNewCheckpoint(value);
        }
    }

private:
    const Configuration& config;
    CheckpointConfig& checkpointConfig;
};

CheckpointConfig::CheckpointConfig(Configuration& config)
    : checkpointPeriod(config.getChkPeriod()),
      checkpointMaxItems(config.getChkMaxItems()),
      maxCheckpoints(config.getMaxCheckpoints() *
                     config.getMaxCheckpointsHardLimitMultiplier()),
      itemNumBasedNewCheckpoint(config.isItemNumBasedNewChk()),
      persistenceEnabled(config.getBucketType() == "persistent"),
      checkpointRemovalMode(
              mode_from_string(config.getCheckpointRemovalMode())) {
}

void CheckpointConfig::addConfigChangeListener(
        EventuallyPersistentEngine& engine) {
    auto& config = engine.getConfiguration();
    auto& ckptConfig = engine.getCheckpointConfig();
    config.addValueChangedListener(
            "chk_period", std::make_unique<ChangeListener>(config, ckptConfig));
    config.addValueChangedListener(
            "chk_max_items",
            std::make_unique<ChangeListener>(config, ckptConfig));
    config.addValueChangedListener(
            "max_checkpoints",
            std::make_unique<ChangeListener>(config, ckptConfig));
    config.addValueChangedListener(
            "max_checkpoints_hard_limit_multiplier",
            std::make_unique<ChangeListener>(config, ckptConfig));
    config.addValueChangedListener(
            "item_num_based_new_chk",
            std::make_unique<ChangeListener>(config, ckptConfig));
}

void CheckpointConfig::setCheckpointPeriod(size_t value) {
    Expects(value > 0);
    checkpointPeriod = static_cast<rel_time_t>(value);
}

void CheckpointConfig::setCheckpointMaxItems(size_t value) {
    Expects(value > 0);
    checkpointMaxItems = value;
}

void CheckpointConfig::setMaxCheckpoints(size_t value) {
    Expects(value >= 2);
    maxCheckpoints = value;
}
