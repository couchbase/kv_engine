/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "globaltask.h"
#include "kv_bucket_iface.h"

class ItemFreqDecayerVisitor;
class PauseResumeVBAdapter;

/**
 * The task is responsible for running a visitor that iterates over all
 * documents in a given hash table, decaying the frequency count of each
 * document by a given percentage.
 */
class ItemFreqDecayerTask : public GlobalTask {
public:
    ItemFreqDecayerTask(EventuallyPersistentEngine* e, uint16_t percentage_);

    ~ItemFreqDecayerTask() override;

    bool run() noexcept override;

    void stop();

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override;

    // Request that the temFreqDecayerTask is woken up to run
    // Made virtual so can be overridden in mock version used in testing.
    virtual void wakeup();

protected:
    // bool used to indicate whether the task's visitor has finished visiting
    // all the documents in the given hash table.
    bool completed;

    // Upper limit on how long each ager chunk can run for, before
    // being paused.
    virtual std::chrono::milliseconds getChunkDuration() const;

private:
    // Returns the underlying AgeVisitor instance.
    ItemFreqDecayerVisitor& getItemFreqDecayerVisitor();

    // Opaque marker indicating how far through the epStore we have visited.
    KVBucketIface::Position epstore_position;

    // Atomic bool used to ensure that the task is not trigger multiple times
    std::atomic<bool> notified;

    // Defines by that percentage the values should be aged. 0 means that no
    // aging is performed, whilst 100 means that the values are reset.
    uint16_t percentage;

    /**
     * Visitor adapter which supports pausing & resuming (records how far
     * though a VBucket is has got). unique_ptr as we re-create it for each
     * complete pass.
     */
    std::unique_ptr<PauseResumeVBAdapter> prAdapter;
};
