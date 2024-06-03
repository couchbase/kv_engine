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

#include "active_stream_checkpoint_processor_task.h"

#include "dcp/producer.h"
#include "ep_engine.h"
#include <executor/executorpool.h>

#include <statistics/cbstat_collector.h>

#include <climits>

ActiveStreamCheckpointProcessorTask::ActiveStreamCheckpointProcessorTask(
        EventuallyPersistentEngine& e, std::shared_ptr<DcpProducer> p)
    : GlobalTask(
              &e, TaskId::ActiveStreamCheckpointProcessorTask, INT_MAX, false),
      description("Process checkpoint(s) for DCP producer " + p->getName()),
      queue(e.getConfiguration().getMaxVbuckets()),
      notified(false),
      maxDuration(std::chrono::microseconds(
              e.getConfiguration().getDcpProducerProcessorRunDurationUs())),
      producerPtr(p) {
}

bool ActiveStreamCheckpointProcessorTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    // Setup that we will sleep forever when done.
    snooze(INT_MAX);

    // Clear the notification flag
    notified.store(false);

    const auto start = std::chrono::steady_clock::now();
    do {
        if (streams.empty()) {
            streams = queuePop();
        }
    } while (!streams.empty() &&
             ((processStreams(start) - start) < maxDuration) &&
             moreStreamsAvailable());

    // Now check if we were re-notified or there are still checkpoints
    bool expected = true;
    if (notified.compare_exchange_strong(expected, false) ||
        moreStreamsAvailable()) {
        // wakeUp, essentially yielding and allowing other tasks a go
        wakeUp();
    }

    return true;
}

std::chrono::steady_clock::time_point
ActiveStreamCheckpointProcessorTask::processStreams(
        std::chrono::steady_clock::time_point start) {
    std::chrono::steady_clock::time_point now = start;
    while (!streams.empty()) {
        auto& stream = *streams.back().get();
        stream.nextCheckpointItemTask();
        streams.pop_back();
        // check current runtime of the task
        now = std::chrono::steady_clock::now();
        if ((now - start) > maxDuration) {
            // time is up
            break;
        }
    }
    return now;
}

void ActiveStreamCheckpointProcessorTask::wakeup() {
    ExecutorPool::get()->wake(getId());
}

void ActiveStreamCheckpointProcessorTask::schedule(
        std::shared_ptr<ActiveStream> stream) {
    if (!queue.pushUnique(stream->getVBucket())) {
        // Return if already in queue, no need to notify the task
        return;
    }

    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        wakeup();
    }
}

void ActiveStreamCheckpointProcessorTask::addStats(const std::string& name,
                                                   const AddStatFn& add_stat,
                                                   const CookieIface* c) const {
    auto prefix = name + ":ckpt_processor_queue_";
    queue.addStats(prefix, add_stat, c);

    add_casted_stat((prefix + "notified").c_str(), notified, add_stat, c);
}

std::vector<std::shared_ptr<ActiveStream>>
ActiveStreamCheckpointProcessorTask::queuePop() {
    Vbid vbid = Vbid(0);
    auto ready = queue.popFront(vbid);
    if (!ready) {
        // no item (i.e. queue empty).
        return {};
    }

    /* findStream acquires DcpProducer::streamsMutex, hence called
       without acquiring workQueueLock */
    auto producer = producerPtr.lock();
    if (producer) {
        return producer->getStreams(vbid);
    }
    return {};
}
