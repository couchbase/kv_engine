/*
 *   Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <executor/globaltask.h>
#include <memcached/vbucket.h>
#include <relaxed_atomic.h>
#include <chrono>
#include <deque>
#include <memory>

class KVBucket;
class InterruptableVBucketVisitor;

/**
 * VBucket Callback Adaptor is a helper task used to implement visitAsync().
 *
 * It is used to assist in visiting multiple vBuckets, without creating a
 * separate task (and associated task overhead) for each vBucket individually.
 *
 * The set of vBuckets to visit is obtained by applying
 * VBucketVisitor::getVBucketFilter() to the set of vBuckets the Bucket has.
 */
class VBCBAdaptor : public GlobalTask {
public:
    VBCBAdaptor(KVBucket* s,
                TaskId id,
                std::unique_ptr<InterruptableVBucketVisitor> v,
                const char* l,
                bool shutdown);
    VBCBAdaptor(const VBCBAdaptor&) = delete;
    const VBCBAdaptor& operator=(const VBCBAdaptor&) = delete;

    std::string getDescription() const override;

    /// Set the maximum expected duration for this task.
    void setMaxExpectedDuration(std::chrono::microseconds duration) {
        maxDuration = duration;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return maxDuration;
    }

    /**
     * Execute the VBCBAdapter task using our visitor.
     *
     * Calls the visitVBucket() method of the visitor object for each vBucket.
     * Before each visitVBucket() call, calls shouldInterrupt() to check if
     * visiting should be paused or stopped.
     * If paused, will sleep for 0s, yielding execution back to the executor -
     * to allow any higher priority tasks to run. When run() is called again,
     * will resume from the vBucket it paused at.
     * While if stopped, the task will just complete and return to the executor.
     */
    bool run() override;

private:
    KVBucket* store;
    std::unique_ptr<InterruptableVBucketVisitor> visitor;
    const char* label;
    std::chrono::microseconds maxDuration;

    /**
     * VBuckets the visitor has not yet visited.
     * Vbs will be sorted according to visitor->getVBucketComparator().
     * Once visited, vbuckets will be removed, so the visitor can resume after
     * pausing at the first element.
     */
    std::deque<Vbid> vbucketsToVisit;

    /**
     * Current VBucket.
     * This value starts as "None" and is only changed to another value when
     * we attempt to work on a valid vbucket.
     *
     * RelaxedAtomic as this is used by getDescription to generate the task
     * description, which can be called by threads other than the one executing.
     */
    const Vbid::id_type None = std::numeric_limits<Vbid::id_type>::max();
    cb::RelaxedAtomic<Vbid::id_type> currentvb{None};
};