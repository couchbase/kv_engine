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

#include "kv_bucket_iface.h"
#include <executor/globaltask.h>

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

    bool run() override;

    void stop();

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

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
