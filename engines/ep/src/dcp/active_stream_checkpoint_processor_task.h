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

#include "dcp/active_stream.h"
#include "dcp/producer.h"
#include "globaltask.h"
#include "vbucket.h"

#include <memcached/engine_common.h>

#include <queue>
#include <unordered_set>

class ActiveStream;
class DcpProducer;

class ActiveStreamCheckpointProcessorTask : public GlobalTask {
public:
    ActiveStreamCheckpointProcessorTask(EventuallyPersistentEngine& e,
                                        std::shared_ptr<DcpProducer> p);

    std::string getDescription() {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Empirical evidence suggests this task runs under 100ms 99.9999% of
        // the time.
        return std::chrono::milliseconds(100);
    }

    bool run();
    void schedule(std::shared_ptr<ActiveStream> stream);
    void wakeup();

    /* Clears the queues and resets the producer reference */
    void cancelTask();

    /* Returns the number of unique streams waiting to be processed */
    size_t queueSize() {
        LockHolder lh(workQueueLock);
        return queue.size();
    }

    /// Outputs statistics related to this task via the given callback.
    void addStats(const std::string& name,
                  ADD_STAT add_stat,
                  const void* c) const;

private:
    std::shared_ptr<StreamContainer<std::shared_ptr<Stream>>> queuePop() {
        Vbid vbid = Vbid(0);
        {
            LockHolder lh(workQueueLock);
            if (queue.empty()) {
                return nullptr;
            }
            vbid = queue.front();
            queue.pop();
            queuedVbuckets.erase(vbid);
        }

        /* findStream acquires DcpProducer::streamsMutex, hence called
           without acquiring workQueueLock */
        auto producer = producerPtr.lock();
        if (producer) {
            return producer->findStreams(vbid);
        }
        return nullptr;
    }

    bool queueEmpty() {
        LockHolder lh(workQueueLock);
        return queue.empty();
    }

    void pushUnique(Vbid vbid) {
        LockHolder lh(workQueueLock);
        if (queuedVbuckets.count(vbid) == 0) {
            queue.push(vbid);
            queuedVbuckets.insert(vbid);
        }
    }

    /// Human-readable description of this task.
    const std::string description;

    /// Guards queue && queuedVbuckets
    mutable std::mutex workQueueLock;

    /**
     * Maintain a queue of unique vbucket ids for which stream should be
     * processed.
     * There's no need to have the same stream in the queue more than once
     *
     * The streams are kept in the 'streams map' of the producer object. We
     * should not hold a shared reference (even a weak ref) to the stream object
     * here because 'streams map' is the actual owner. If we hold a weak ref
     * here and the streams map replaces the stream for the vbucket id with a
     * new one, then we would end up not updating it here as we append to the
     * queue only if there is no entry for the vbucket in the queue.
     */
    std::queue<Vbid> queue;
    std::unordered_set<Vbid> queuedVbuckets;

    std::atomic<bool> notified;
    const size_t iterationsBeforeYield;

    const std::weak_ptr<DcpProducer> producerPtr;
};
