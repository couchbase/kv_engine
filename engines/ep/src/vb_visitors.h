/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "comparators.h"
#include "hash_table.h"
#include "vb_filter.h"
#include "vbucket_fwd.h"

#include <folly/Chrono.h>

using namespace std::chrono_literals;

class HashTableVisitor;

/**
 * Base class for a VBucket visitor.
 *
 * Implemented by objects which wish to visit vBuckets in a Bucket - see
 * KVBucketIface::visit().
 *
 * A filter may be specified to constain the set of vBuckets which will be
 * visited (for example to only visit vBuckets belonging to a give shard). By
 * default all vBuckets are visited.
 */
class VBucketVisitor {
public:
    VBucketVisitor();

    VBucketVisitor(const VBucketFilter& filter);

    virtual ~VBucketVisitor();

    /**
     * Begin visiting a bucket.
     *
     * @param vb the vbucket we are beginning to visit. Passed as const
     *        shared_ptr which allows caller to retain reference count if
     *        desired, but not reseat the shared_ptr.
     */
    virtual void visitBucket(const VBucketPtr& vb) = 0;

    const VBucketFilter& getVBucketFilter() {
        return vBucketFilter;
    }

    void setVBucketFilter(VBucketFilter filter) {
        vBucketFilter = std::move(filter);
    }

    /**
     * Get a comparator used to order the vbucket IDs based on visitor-specific
     * criteria, if necessary. This can be used to specify the order the visitor
     * wishes to visit vbuckets.
     *
     * Default behaviour is to visit vbuckets in ascending order.
     */
    virtual std::function<bool(const Vbid&, const Vbid&)> getVBucketComparator()
            const {
        return cb::less<Vbid>();
    }

protected:
    VBucketFilter vBucketFilter;
};

/**
 * A base class for VBucket visitor class which supports pausing after
 * processing a vBucket, to be later resumed at the next vBucket.
 *
 * This is used by KVBucketIface::visitAsync() to allow costly visitors to
 * pause after some amount of work, sleeping for a period before resuming.
 *
 */
class PausableVBucketVisitor : public VBucketVisitor {
public:
    void visitBucket(const VBucketPtr& vb) override = 0;

    /**
     * Called when starting to visit vBuckets, both on initial visit and also
     * subsequently if visiting was previously paused.
     */
    virtual void begin(){};

    /**
     * Called after all vbuckets have been visited.
     */
    virtual void complete(){};

    /**
     * Return true if visiting vbuckets should be paused temporarily.
     *
     * Default implementation pauses if the chunk has been running for
     * more than maxChunkDuration.
     */
    virtual bool pauseVisitor() = 0;
};

/**
 * A base class for a vBucket visitor which pauses after a given duration of
 * time has been spent executing (maxChunkDuration).
 */
class CappedDurationVBucketVisitor : public PausableVBucketVisitor {
    void visitBucket(const VBucketPtr& vb) override = 0;

    void begin() override;
    bool pauseVisitor() override;

protected:
    /**
     * Clock used for timing vBucket visits, to decide when to pause.
     * This clock is read once per vBucket visited, which if there is
     * little / no work for that vBucket can be _very_
     * frequently. Given the maxChunkDuration is 25 milliseconds, we
     * don't need the full resolution (and potential cost) provided by
     * chrono::steady_clock - which on Linux uses CLOCK_MONOTONIC and
     * typically gives ~1ns resolution.
     *
     * Instead we can use a coarser (1ms) but much cheaper clock such
     * as folly's coarse_stready_clock (CLOCK_MONOTONIC_COARSE on
     * Linux).
     *
     * (For example, with the HPET clocksource on Linux 4.15
     * CLOCK_MONOTONIC requires a syscall to read, whereas
     * CLOCK_MONOTONIC_COARSE can be handled in the userspace VDSO).
     */
    using Clock = folly::chrono::coarse_steady_clock;

    /**
     * Target maximum duration to run the visitor before pausing (yielding),
     * to avoid blocking other higher priority tasks.
     * Note: chunk duration is only checked at vBucket boundaries, so
     * this limit isn't guaranteed - we may exceed it by the duration of a
     * single vBucket visit.
     */
    const Clock::duration maxChunkDuration = 25ms;

    /// At what time did the current chunk of vBuckets start visiting?
    Clock::time_point chunkStart;
};

/**
 * Base class for visiting VBuckets with pause/resume support.
 */
class PauseResumeVBVisitor {
public:
    virtual ~PauseResumeVBVisitor() {
    }

    /**
     * Visit a VBucket within an epStore.
     *
     * @param vb a reference to the VBucket.
     * @return True if visiting should continue, otherwise false.
     */
    virtual bool visit(VBucket& vb) = 0;
};

/**
 * VBucket-aware variant of HashTableVisitor - abstract base class for
 * visiting StoredValues, where the visitor needs to know which VBucket is
 * being visited.
 *
 * setCurrentVBucket() will be called to inform the visitor of the current
 * VBucket (whenever the Bucket being visited moves to a new VBucket).
 */
class VBucketAwareHTVisitor : public HashTableVisitor {
public:
    bool visit(const HashTable::HashBucketLock& lh,
               StoredValue& v) override = 0;

    /**
     * Inform the visitor of the current vBucket.
     * Called (before visit()) whenever we move to a different VBucket.
     */
    virtual void setCurrentVBucket(VBucket& vb) {
    }
};

/**
 * Adapts a VBucketAwareHTVisitor, recording the position into the
 * HashTable the visit reached when it paused; and resumes Visiting from that
 * position when visit() is next called.
 */
class PauseResumeVBAdapter : public PauseResumeVBVisitor {
public:
    PauseResumeVBAdapter(std::unique_ptr<VBucketAwareHTVisitor> htVisitor);

    /**
     * Visit a VBucket within an epStore. Records the place where the visit
     * stops (when the wrapped htVisitor returns false), for later resuming
     * from *approximately* the same place.
     */
    bool visit(VBucket& vb) override;

    /// Returns the current hashtable position.
    HashTable::Position getHashtablePosition() const {
        return hashtable_position;
    }

    /// Returns the wrapped HashTable visitor.
    VBucketAwareHTVisitor& getHTVisitor() {
        return *htVisitor;
    }

private:
    // The HashTable visitor to apply to each VBucket's HashTable.
    std::unique_ptr<VBucketAwareHTVisitor> htVisitor;

    // When resuming, which vbucket should we start from?
    Vbid resume_vbucket_id = Vbid(0);

    // When pausing / resuming, hashtable position to use.
    HashTable::Position hashtable_position;
};
