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
#include "executorpool.h"

#include <statistics/cbstat_collector.h>

#include <climits>

ActiveStreamCheckpointProcessorTask::ActiveStreamCheckpointProcessorTask(
        EventuallyPersistentEngine& e, std::shared_ptr<DcpProducer> p)
    : GlobalTask(
              &e, TaskId::ActiveStreamCheckpointProcessorTask, INT_MAX, false),
      description("Process checkpoint(s) for DCP producer " + p->getName()),
      queue(e.getConfiguration().getMaxVbuckets()),
      notified(false),
      iterationsBeforeYield(
              e.getConfiguration().getDcpProducerSnapshotMarkerYieldLimit()),
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

    size_t iterations = 0;
    do {
        auto streams = queuePop();

        if (streams) {
            for (auto rh = streams->rlock(); !rh.end(); rh.next()) {
                auto* as = static_cast<ActiveStream*>(rh.get().get());
                as->nextCheckpointItemTask();
            }
        } else {
            break;
        }
        iterations++;
    } while (!queueEmpty() && iterations < iterationsBeforeYield);

    // Now check if we were re-notified or there are still checkpoints
    bool expected = true;
    if (notified.compare_exchange_strong(expected, false) || !queueEmpty()) {
        // wakeUp, essentially yielding and allowing other tasks a go
        wakeUp();
    }

    return true;
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
                                                   const void* c) const {
    auto prefix = name + ":ckpt_processor_queue_";
    queue.addStats(prefix, add_stat, c);

    add_casted_stat((prefix + "notified").c_str(), notified, add_stat, c);
}

std::shared_ptr<StreamContainer<std::shared_ptr<ActiveStream>>>
ActiveStreamCheckpointProcessorTask::queuePop() {
    Vbid vbid = Vbid(0);
    auto ready = queue.popFront(vbid);
    if (!ready) {
        // no item (i.e. queue empty).
        return nullptr;
    }

    /* findStream acquires DcpProducer::streamsMutex, hence called
       without acquiring workQueueLock */
    auto producer = producerPtr.lock();
    if (producer) {
        return producer->findStreams(vbid);
    }
    return nullptr;
}
