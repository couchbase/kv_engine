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

#include "active_stream_checkpoint_processor_task.h"

#include "dcp/producer.h"
#include "ep_engine.h"
#include "executorpool.h"

#include <climits>

ActiveStreamCheckpointProcessorTask::ActiveStreamCheckpointProcessorTask(
        EventuallyPersistentEngine& e, std::shared_ptr<DcpProducer> p)
    : GlobalTask(
              &e, TaskId::ActiveStreamCheckpointProcessorTask, INT_MAX, false),
      description("Process checkpoint(s) for DCP producer " + p->getName()),
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
                ActiveStream* as = static_cast<ActiveStream*>(rh.get().get());
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
    pushUnique(stream->getVBucket());

    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        wakeup();
    }
}

void ActiveStreamCheckpointProcessorTask::cancelTask() {
    LockHolder lh(workQueueLock);
    while (!queue.empty()) {
        queue.pop();
    }
    queuedVbuckets.clear();
}

void ActiveStreamCheckpointProcessorTask::addStats(const std::string& name,
                                                   const AddStatFn& add_stat,
                                                   const void* c) const {
    // Take a copy of the queue data under lock; then format it to stats.
    std::queue<Vbid> qCopy;
    std::unordered_set<Vbid> qMapCopy;
    {
        LockHolder lh(workQueueLock);
        qCopy = queue;
        qMapCopy = queuedVbuckets;
    }

    auto prefix = name + ":ckpt_processor_";
    add_casted_stat((prefix + "queue_size").c_str(), qCopy.size(), add_stat, c);
    add_casted_stat(
            (prefix + "queue_map_size").c_str(), qMapCopy.size(), add_stat, c);

    // Form a comma-separated string of the queue's contents.
    std::string contents;
    while (!qCopy.empty()) {
        contents += std::to_string(qCopy.front().get()) + ",";
        qCopy.pop();
    }
    if (!contents.empty()) {
        contents.pop_back();
    }
    add_casted_stat(
            (prefix + "queue_contents").c_str(), contents.c_str(), add_stat, c);

    // Form a comma-separated string of the queue map's contents.
    std::string qMapContents;
    for (auto& vbid : qMapCopy) {
        qMapContents += std::to_string(vbid.get()) + ",";
    }
    if (!qMapContents.empty()) {
        qMapContents.pop_back();
    }
    add_casted_stat((prefix + "queue_map_contents").c_str(),
                    qMapContents.c_str(),
                    add_stat,
                    c);

    add_casted_stat((prefix + "notified").c_str(), notified, add_stat, c);
}
