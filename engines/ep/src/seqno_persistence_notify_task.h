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

#include "vb_ready_queue.h"

#include <executor/globaltask.h>

class KVBucket;

/**
 * Task that will notify expired (and also completed) SeqnoPersistenceRequests.
 * The task has a sleep time set so that it wakes up at the deadline of the
 * next expiring request. If no requests exist, task sleeps indefinitely.
 */
class SeqnoPersistenceNotifyTask : public GlobalTask {
public:
    SeqnoPersistenceNotifyTask(KVBucket& bucket);

    bool run() override;

    std::string getDescription() const override {
        return "SeqnoPersistenceNotifyTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::seconds(1);
    }

    /**
     * Add a VBucket that has an outstanding SeqnoPersistenceRequest with the
     * given deadline for completion.
     *
     * This may adjust the snooze time of the SeqnoPersistenceNotifyTask to meet
     * the deadline given.
     *
     * @param vbid ID of vbucket with new SeqnoPersistenceRequest
     * @param deadline The deadline the SeqnoPersistenceRequest must be
     *        notified by.
     */
    void addVbucket(Vbid vbid, std::chrono::steady_clock::time_point deadline);

protected:
    void processVbuckets();

    KVBucket& bucket;
    VBReadyQueue vbuckets;

    /**
     * This mutex serialise the wakeup adjustment code between run() and
     * addVbucket(), ensuring that run and addVbucket don't 'undo' each others
     * update
     */
    std::mutex adjustWakeUp;
};