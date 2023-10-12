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

#include "ep_task.h"
#include "kv_bucket_iface.h"
#include <folly/Synchronized.h>
#include <platform/semaphore.h>
#include <utilities/weak_ptr_bag.h>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

class ItemFreqDecayerVisitor;
class PauseResumeVBAdapter;

class ItemFreqDecayerTask;
class CrossBucketItemFreqDecayer;

/**
 * Creates ItemFreqDecayerTask instances. Tasks created for quota sharing
 * engine configurations are weakly referenced and strong references can be
 * obtained by calling getTasksForQuotaSharing.
 */
class ItemFreqDecayerTaskManager {
public:
    static ItemFreqDecayerTaskManager& get();

    /**
     * Create an ItemFreqDecayerTask (factory method).
     *
     * @param percentage Defines by what percentage the values should be aged.
     *                   0 means that no aging is performed, whilst 100 means
     *                   that all the values are reset.
     */
    std::shared_ptr<ItemFreqDecayerTask> create(EventuallyPersistentEngine& e,
                                                uint16_t percentage);

    /**
     * Get the ItemFreqDecayerTask instances created for quota sharing buckets
     * which are currently alive in the program.
     */
    std::vector<std::shared_ptr<ItemFreqDecayerTask>> getTasksForQuotaSharing()
            const;

    /**
     * Return the cross bucket decayer runner.
     */
    std::shared_ptr<CrossBucketItemFreqDecayer> getCrossBucketDecayer() const;

private:
    ItemFreqDecayerTaskManager();

    /**
     * Called when a UC ItemFreqDecayerTask has completed.
     */
    static void signalCompleted(ItemFreqDecayerTask& task);

    /**
     * The ItemFreqDecayerTasks created for quota sharing buckets.
     */
    WeakPtrBag<ItemFreqDecayerTask, std::mutex> tasksForQuotaSharing;

    /**
     * The cross-bucket decayer runner.
     */
    const std::shared_ptr<CrossBucketItemFreqDecayer> crossBucketDecayer;
};

/**
 * Runs the ItemFreqDecayerTask for quota sharing buckets.
 */
class CrossBucketItemFreqDecayer {
public:
    /**
     * Run the ItemFreqDecayerTasks.
     */
    void schedule();

    /**
     * Signalled when an ItemFreqDecayerTask has completed execution.
     */
    void itemDecayerCompleted(ItemFreqDecayerTask& task);

private:
    std::atomic_bool notified{false};
    /**
     * The set of tasks we're still waiting to complete before we can schedule
     * again.
     *
     * The tasks' destructors will signal the ItemFreqDecayerTaskManager, which
     * will in turn call itemDecayerCompleted, where we remove the destroyed
     * task pointers from this set (so we don't have dangling pointers).
     */
    folly::Synchronized<std::unordered_set<ItemFreqDecayerTask*>, std::mutex>
            pendingSubTasks;
};

/**
 * The task is responsible for running a visitor that iterates over all
 * documents in a given hash table, decaying the frequency count of each
 * document by a given percentage.
 */
class ItemFreqDecayerTask : public EpTask {
public:
    /**
     * Create an ItemFreqDecayerTask.
     *
     * @param e A reference to the ep-engine.
     * @param percentage Defines by what percentage the values should be aged.
     *                   0 means that no aging is performed, whilst 100 means
     *                   that all the values are reset.
     * @param scheduleNow If true, sets the waketime of the tasks to now.
     *                    Otherwise, sets it to time_point::max().
     */
    ItemFreqDecayerTask(EventuallyPersistentEngine& e,
                        uint16_t percentage_,
                        bool scheduleNow = true);

    ~ItemFreqDecayerTask() override;

    bool run() override;

    void stop();

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

    // Request that the temFreqDecayerTask is woken up to run
    // Made virtual so can be overridden in mock version used in testing.
    virtual void wakeup();

protected:
    virtual void onCompleted() {
    }

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
