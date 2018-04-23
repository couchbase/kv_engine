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

#include "../mock/mock_item_freq_decayer.h"
#include "ep_bucket.h"
#include "ep_engine.h"

/*
 * Mock of the EPBucket class.
 */
class MockEPBucket : public EPBucket {
public:
    MockEPBucket(EventuallyPersistentEngine& theEngine) : EPBucket(theEngine) {
    }

    void createItemFreqDecayerTask() {
        Configuration& config = engine.getConfiguration();
        itemFreqDecayerTask = std::make_shared<MockItemFreqDecayerTask>(
                &engine, config.getItemFreqDecayerPercent());
    }

    void disableItemFreqDecayerTask() {
        ExecutorPool::get()->cancel(itemFreqDecayerTask->getId());
    }

    MockItemFreqDecayerTask* getMockItemFreqDecayerTask() {
        return dynamic_cast<MockItemFreqDecayerTask*>(
                itemFreqDecayerTask.get());
    }
};
