/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "item_freq_decayer.h"

/*
 * Mock of the ItemFreqDecayerTask class.
 */
class MockItemFreqDecayerTask : public ItemFreqDecayerTask {
public:
    MockItemFreqDecayerTask(EventuallyPersistentEngine* e, uint16_t percentage_)
        : ItemFreqDecayerTask(e, percentage_) {
    }

    // Provide ability to read the completed flag
    bool isCompleted() const {
        return completed;
    }

    void wakeup() override {
        wakeupCalled = true;
        ItemFreqDecayerTask::wakeup();
    }

    // Set the ChunkDuration to 0ms so that it ensures that assuming there
    // are more than ProgressTracker:INITIAL_VISIT_COUNT_CHECK documents in
    // the hash table it will require multiple passes to visit all items.
    std::chrono::milliseconds getChunkDuration() const override {
        return std::chrono::milliseconds(0);
    }

    bool wakeupCalled{false};
};
