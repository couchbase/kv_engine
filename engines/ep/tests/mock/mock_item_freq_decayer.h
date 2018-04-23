/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
