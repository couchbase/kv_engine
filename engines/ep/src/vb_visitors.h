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

#include "config.h"

#include "hash_table.h"
#include "vb_filter.h"
#include "vbucket_fwd.h"

class HashTableVisitor;

/**
 * vbucket-aware hashtable visitor.
 *
 * Implemented by objects which wish to visit all vBuckets in a Bucket - see
 * KVBucketIface::visit() and KVBucket::visitAsync().
 *
 * In the simplest use-case, implement the visitBucket() method of this
 * interface and call KVBucket::visit(VBucketVisitor&). The visitBucket()
 * method will then be called for each vBucket in sequence.
 *
 * For more advanced use-cases (including use with KVBucketIface::visitAsync())
 * the non-pure virtual methods can be overridden - typically to allow
 * visiting to be paused (and subsequently later resumed).
 */
class VBucketVisitor {
public:
    VBucketVisitor() = default;

    VBucketVisitor(const VBucketFilter& filter) : vBucketFilter(filter) {
    }

    virtual ~VBucketVisitor() = default;

    /**
     * Begin visiting a bucket.
     *
     * @param vb the vbucket we are beginning to visit
     */
    virtual void visitBucket(VBucketPtr& vb) = 0;

    const VBucketFilter& getVBucketFilter() {
        return vBucketFilter;
    }

    /**
     * Called after all vbuckets have been visited.
     */
    virtual void complete() {
    }

    /**
     * Return true if visiting vbuckets should be paused temporarily.
     */
    virtual bool pauseVisitor() {
        return false;
    }

protected:
    VBucketFilter vBucketFilter;
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
