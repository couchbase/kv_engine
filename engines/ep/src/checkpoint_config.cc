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
    explicit ChangeListener(CheckpointConfig& c) : config(c) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key.compare("chk_period") == 0) {
            config.setCheckpointPeriod(value);
        } else if (key.compare("chk_max_items") == 0) {
            config.setCheckpointMaxItems(value);
        } else if (key.compare("max_checkpoints") == 0) {
            config.setMaxCheckpoints(value);
        }
    }

    void booleanValueChanged(const std::string& key, bool value) override {
        if (key.compare("item_num_based_new_chk") == 0) {
            config.allowItemNumBasedNewCheckpoint(value);
        }
    }

private:
    CheckpointConfig& config;
};

CheckpointConfig::CheckpointConfig(rel_time_t period,
                                   size_t max_items,
                                   size_t max_ckpts,
                                   bool item_based_new_ckpt,
                                   bool persistence_enabled,
                                   CheckpointRemoval checkpoint_removal_mode)
    : checkpointPeriod(period),
      checkpointMaxItems(max_items),
      maxCheckpoints(max_ckpts),
      itemNumBasedNewCheckpoint(item_based_new_ckpt),
      persistenceEnabled(persistence_enabled),
      checkpointRemovalMode(checkpoint_removal_mode) {
}

CheckpointConfig::CheckpointConfig(Configuration& config)
    : CheckpointConfig(config.getChkPeriod(),
                       config.getChkMaxItems(),
                       config.getMaxCheckpoints(),
                       config.isItemNumBasedNewChk(),
                       config.getBucketType() == "persistent",
                       mode_from_string(config.getCheckpointRemovalMode())) {
}

void CheckpointConfig::addConfigChangeListener(
        EventuallyPersistentEngine& engine) {
    Configuration& configuration = engine.getConfiguration();
    configuration.addValueChangedListener(
            "chk_period",
            std::make_unique<ChangeListener>(engine.getCheckpointConfig()));
    configuration.addValueChangedListener(
            "chk_max_items",
            std::make_unique<ChangeListener>(engine.getCheckpointConfig()));
    configuration.addValueChangedListener(
            "max_checkpoints",
            std::make_unique<ChangeListener>(engine.getCheckpointConfig()));
    configuration.addValueChangedListener(
            "item_num_based_new_chk",
            std::make_unique<ChangeListener>(engine.getCheckpointConfig()));
}

bool CheckpointConfig::validateCheckpointMaxItemsParam(
        size_t checkpoint_max_items) {
    if (checkpoint_max_items < MIN_CHECKPOINT_ITEMS ||
        checkpoint_max_items > MAX_CHECKPOINT_ITEMS) {
        EP_LOG_WARN(
                "New checkpoint_max_items param value {} is not ranged "
                "between the min allowed value {} and max value {}",
                checkpoint_max_items,
                MIN_CHECKPOINT_ITEMS,
                MAX_CHECKPOINT_ITEMS);
        return false;
    }
    return true;
}

bool CheckpointConfig::validateCheckpointPeriodParam(size_t checkpoint_period) {
    if (checkpoint_period < MIN_CHECKPOINT_PERIOD ||
        checkpoint_period > MAX_CHECKPOINT_PERIOD) {
        EP_LOG_WARN(
                "New checkpoint_period param value {} is not ranged "
                "between the min allowed value {} and max value {}",
                checkpoint_period,
                MIN_CHECKPOINT_PERIOD,
                MAX_CHECKPOINT_PERIOD);
        return false;
    }
    return true;
}

void CheckpointConfig::setCheckpointPeriod(size_t value) {
    if (!validateCheckpointPeriodParam(value)) {
        value = DEFAULT_CHECKPOINT_PERIOD;
    }
    checkpointPeriod = static_cast<rel_time_t>(value);
}

void CheckpointConfig::setCheckpointMaxItems(size_t value) {
    if (!validateCheckpointMaxItemsParam(value)) {
        value = DEFAULT_CHECKPOINT_ITEMS;
    }
    checkpointMaxItems = value;
}

void CheckpointConfig::setMaxCheckpoints(size_t value) {
    Expects(value >= 2);
    maxCheckpoints = value;
}
