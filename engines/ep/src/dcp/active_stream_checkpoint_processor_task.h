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

#include "dcp/active_stream.h"
#include "ep_task.h"
#include "vb_ready_queue.h"

#include <memcached/engine_common.h>

#include <queue>
#include <string>
#include <unordered_set>

class ActiveStream;
class DcpProducer;
class Stream;
template <class E>
class StreamContainer;

class ActiveStreamCheckpointProcessorTask : public EpNotifiableTask {
public:
    ActiveStreamCheckpointProcessorTask(EventuallyPersistentEngine& e,
                                        std::shared_ptr<DcpProducer> p);

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Empirical evidence from perf runs suggests this task runs under
        // 210ms 99.9999% of the time.
        return std::chrono::milliseconds(210);
    }

    bool runInner(bool manuallyNotified) override;
    void schedule(Vbid vbid);

    /* Returns the number of unique streams waiting to be processed */
    size_t queueSize() {
        return queue.size();
    }

    /// Outputs statistics related to this task via the given callback.
    void addStats(const std::string& name,
                  const AddStatFn& add_stat,
                  CookieIface& c) const;

    size_t getStreamsSize() const {
        return streams.size();
    }

private:
    /**
     * Pop the front of the queue and return the streams ready processing
     * @return vector of ready ActiveStreams
     */
    std::vector<std::shared_ptr<ActiveStream>> queuePop();

    bool moreStreamsAvailable() {
        return !queue.empty();
    }

    /**
     * For each stream in streams call nextCheckpointItemTask, each processed
     * stream is removed from the streams vector.
     * Processing will stop if the duration since start exceeds maxDuration.
     *
     * @param start A start time for use in duration calculations
     * @return the last read time_point, i.e. "now"
     */
    cb::time::steady_clock::time_point processStreams(
            const cb::time::steady_clock::time_point start);

    /// Human-readable description of this task.
    const std::string description;

    /*
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
    VBReadyQueue queue;

    std::atomic<bool> notified;

    // maximum duration for the run loop
    const std::chrono::microseconds maxDuration;

    const std::weak_ptr<DcpProducer> producerPtr;

    /**
     * The container of streams that are to be worked on, allowing a yield if
     * run exceeds maxDuration.
     */
    std::vector<std::shared_ptr<ActiveStream>> streams;
};
