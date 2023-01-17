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

#include "cross_bucket_visitor_adapter.h"

#include "bucket_logger.h"
#include "kv_bucket.h"
#include "vb_visitors.h"
#include <executor/executorpool.h>
#include <executor/globaltask.h>
#include <fmt/format.h>
#include <random>

/**
 * Shuffles the range of elements using an internal random number generator.
 */
template <typename RandomAccessIt>
void randomShuffle(RandomAccessIt first, RandomAccessIt last) {
    static std::mt19937 g = [] {
        std::random_device rd;
        return std::mt19937(rd());
    }();
    std::shuffle(first, last, g);
}

CrossBucketVisitorAdapter::CrossBucketVisitorAdapter(
        ServerBucketIface& serverBucketApi,
        CrossBucketVisitorAdapter::ScheduleOrder order,
        TaskId id,
        std::string_view label,
        std::chrono::microseconds maxExpectedDuration,
        std::shared_ptr<cb::Semaphore> semaphore)
    : serverBucketApi(serverBucketApi),
      order(order),
      id(id),
      label(label),
      maxExpectedDuration(maxExpectedDuration),
      completed(false),
      expectedTask(nullptr),
      semaphore(std::move(semaphore)) {
}

void CrossBucketVisitorAdapter::scheduleNow(
        CrossBucketVisitorAdapter::VisitorMap visitors, bool randomShuffle_) {
    Expects(!expectedTask && !completed);
    // Populate the list of tasks we've scheduled
    for (auto& [engine, visitor] : visitors) {
        auto& ep = dynamic_cast<EventuallyPersistentEngine&>(*engine.get());
        orderedTasks.push_back(schedule(*ep.getKVBucket(), std::move(visitor)));
    }

    if (randomShuffle_) {
        // Q: Why are we shuffling the list of tasks to wake up?
        // Well, there is no natural ordering between buckets and we don't
        // really want one. The iteration order of the VisitorMap is
        // unspecified, but not necessarily random. Also, when scheduling more
        // taxing visitors, like the PagingVisitors, we don't want to always
        // start with bucket A's vBucket.
        randomShuffle(orderedTasks.begin(), orderedTasks.end());
    }

    EP_LOG_DEBUG("Cross-bucket visitor: '{}' created with {} tasks.",
                 label,
                 orderedTasks.size());

    scheduleNext();
}

void CrossBucketVisitorAdapter::scheduleNext() {
    std::lock_guard lock(schedulingMutex);
    while (!orderedTasks.empty()) {
        auto task = orderedTasks.front().lock();
        orderedTasks.pop_front();
        if (!task) {
            // The task object has been destroyed, so we can ignore it
            continue;
        }

        if (auto handle =
                    serverBucketApi.tryAssociateBucket(task->getEngine())) {
            // Cool, the task and engine are both active.
            // We can schedule the task.
            ExecutorPool::get()->wake(task->getId());
            // Remember which task we scheduled so that we can avoid spurious
            // wake ups from the engine shutting down.
            expectedTask = task.get();
            EP_LOG_DEBUG("Cross-bucket visitor: Scheduling next task '{}' ({})",
                         task->getDescription(),
                         (void*)task.get());

            switch (order) {
            case ScheduleOrder::RoundRobin:
                // Run again after all other tasks have had a chance to run.
                orderedTasks.push_back(task);
                break;
            case ScheduleOrder::Sequential:
                // Run again and again until the task completes.
                orderedTasks.push_front(task);
                break;
            }

            scheduleNextHook(orderedTasks, expectedTask);
            return;
        } else {
            EP_LOG_DEBUG(
                    "Cross-bucket visitor: Task '{}' ({}) will not be "
                    "scheduled, as its engine is not available.",
                    task->getDescription(),
                    (void*)task.get());
        }
    }

    EP_LOG_DEBUG("Cross-bucket visitor: '{}' completed.", label);
    expectedTask = nullptr;
    completed = true;
    if (semaphore) {
        semaphore->release();
    }
}

std::shared_ptr<SingleSteppingVisitorAdapter>
CrossBucketVisitorAdapter::schedule(
        KVBucket& bucket,
        std::unique_ptr<InterruptableVBucketVisitor> visitor) {
    auto callback = [&bucket, self = shared_from_this()](auto& task,
                                                         auto runAgain) {
        auto& t = static_cast<const SingleSteppingVisitorAdapter&>(task);
        self->onVisitorRunCompleted(bucket.getEPEngine(), t, runAgain);
    };
    auto lbl = fmt::format("{} ({})", label, bucket.getEPEngine().getName());
    auto task = std::make_shared<SingleSteppingVisitorAdapter>(
            &bucket, id, std::move(visitor), lbl.c_str(), callback);
    task->setMaxExpectedDuration(maxExpectedDuration);

    ExecutorPool::get()->schedule(task);
    return task;
}

void CrossBucketVisitorAdapter::onVisitorRunCompleted(
        EventuallyPersistentEngine& engine,
        const SingleSteppingVisitorAdapter& task,
        bool runAgain) {
    {
        // Make sure we're not scheduling anything else while we check
        // whether this is the correct task to expect.
        std::unique_lock lock(schedulingMutex);
        // Check whether this is the expected task, to avoid spurious
        // wake-ups from engines shutting down and waking up their tasks.
        if (expectedTask != &task) {
            Expects(engine.getEpStats().isShutdown && !runAgain);
            EP_LOG_DEBUG(
                    "Cross-bucket visitor '{}' ({}) was signalled by an "
                    "unexpected task '{}' ({}) (expected: '{}' ({})). The "
                    "engine is shutting-down.",
                    label,
                    (void*)this,
                    task.getDescription(),
                    (void*)&task,
                    expectedTask->getDescription(),
                    (void*)expectedTask);
            // Remove the task from the queue. It is no longer managed by the
            // adaptor.
            orderedTasks.erase(std::remove_if(orderedTasks.begin(),
                                              orderedTasks.end(),
                                              [&task](const auto& t) {
                                                  return t.lock().get() ==
                                                         &task;
                                              }),
                               orderedTasks.end());
            return;
        }
    }

    if (!runAgain) {
        EP_LOG_DEBUG(
                "Cross-bucket visitor: Task '{}' ({}) completed and will not "
                "run again.",
                task.getDescription(),
                (void*)&task);
        std::lock_guard lock(schedulingMutex);
        // Won't run the task again. Remove from the queue.
        switch (order) {
        case ScheduleOrder::Sequential:
            orderedTasks.pop_front();
            break;
        case ScheduleOrder::RoundRobin:
            orderedTasks.pop_back();
            break;
        }
    }
    // Okay, time to wake up the next task
    scheduleNext();
}
