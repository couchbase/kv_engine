/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_engine.h"
#include "vb_adapters.h"
#include <memcached/server_bucket_iface.h>
#include <platform/semaphore.h>
#include <atomic>
#include <deque>
#include <utility>
#include <vector>

/**
 * An "adapter" which can orchestrate the execution of a set of visitors across
 * a set of buckets.
 *
 * Each visitor will be allowed to visit one vBucket, and then paused until
 * woken up again according to the ScheduleOrder specified.
 */
class CrossBucketVisitorAdapter
    : public std::enable_shared_from_this<CrossBucketVisitorAdapter> {
public:
    /**
     * Bucket -> Visitor map
     */
    using VisitorMap = std::vector<
            std::pair<AssociatedBucketHandle,
                      std::unique_ptr<InterruptableVBucketVisitor>>>;

    /**
     * The order in which to schedule the visitors runs among buckets
     */
    enum class ScheduleOrder {
        /**
         * Run bucket A's visitor to completion, and only then start bucket B's
         * visitor.
         */
        Sequential,
        /**
         * Progress bucket A's visitor by one vBucket, then progress bucket B's
         * visitor and continue until all visitors have completed.
         */
        RoundRobin,
    };

    /**
     * Create an instance of the CrossBucketVisitorAdapter.
     * @param serverBucketApi The server bucket interface to use
     * @param id The TaskId to use for each visitor task
     * @param label The label to use for each visitor task
     * @param maxExpectedDuration The expected duration of a vBucket visit
     * @param semaphore An optional semaphore object. This adapter will release
     *                  one token to the semaphore once completed.
     */
    CrossBucketVisitorAdapter(
            ServerBucketIface& serverBucketApi,
            ScheduleOrder order,
            TaskId id,
            std::string_view label,
            std::chrono::microseconds maxExpectedDuration,
            std::shared_ptr<cb::Semaphore> semaphore = nullptr);

    /**
     * Starts running the visitors.
     * @param visitors A map of bucket -> visitor
     * @param order The order in which to schedule the visitors
     * @param randomShuffle If true, the relative order between the visitors
     *                      will be randomised. If false, the iteration order
     *                      of the visitor map will be used.
     */
    void scheduleNow(VisitorMap visitors, bool randomShuffle = true);

    /**
     * Have all visitors completed.
     * @return true if all buckets' visitors have completed
     */
    bool hasCompleted() const {
        return completed;
    }

    /**
     * Testing hook which is called back after scheduleNext schedules a task, so
     * we can test what happens if an unexpected task signals completion.
     */
    TestingHook<std::deque<std::weak_ptr<EpTask>>&, EpTask*> scheduleNextHook;

private:
    /**
     * Schedule the next visitor task to run, according to the ScheduleOrder.
     */
    void scheduleNext();

    /**
     * Create a SingleSteppingVisitorAdapter task for that bucket.
     */
    std::shared_ptr<SingleSteppingVisitorAdapter> schedule(
            KVBucket& bucket,
            std::unique_ptr<InterruptableVBucketVisitor> visitor);

    /**
     * Callback from a Bucket's visitor task.
     */
    void onVisitorRunCompleted(EventuallyPersistentEngine& engine,
                               const SingleSteppingVisitorAdapter& task,
                               bool runAgain);

    ServerBucketIface& serverBucketApi;
    const ScheduleOrder order;
    const TaskId id;
    const std::string label;
    const std::chrono::microseconds maxExpectedDuration;

    /**
     * Set to true once all visitors have completed.
     */
    std::atomic_bool completed;

    /**
     * A mutex on the list of tasks to run - orderedTasks, and the task to
     * expect - expectedTask.
     *
     * Mutex is recursive so that we can test the adapter from single-threaded
     * tests.
     */
    std::recursive_mutex schedulingMutex;
    /**
     * The list of tasks we will wake up in the order specified by the
     * ScheduleOrder.
     */
    std::deque<std::weak_ptr<EpTask>> orderedTasks;

    /**
     * The task that was scheduled and is expected to run and callback.
     */
    EpTask* expectedTask;

    /*
     * Optional semaphore to release once completed.
     */
    std::shared_ptr<cb::Semaphore> semaphore;
};
