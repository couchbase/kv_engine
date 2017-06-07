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

class HashTableVisitor;
class VBucket;

using VBucketPtr = std::shared_ptr<VBucket>;

/**
 * vbucket-aware hashtable visitor.
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
     * Visit a hashtable within an epStore.
     *
     * @param vbucket_id ID of the vbucket being visited.
     * @param ht a reference to the hashtable.
     * @return True if visiting should continue, otherwise false.
     */
    virtual bool visit(uint16_t vbucket_id, HashTable& ht) = 0;
};

/**
 * Adapts a HashTableVisitor, recording the position into the
 * HashTable the visit reached when it paused; and resumes Visiting from that
 * position when visit() is next called.
 */
class PauseResumeVBAdapter : public PauseResumeVBVisitor {
public:
    PauseResumeVBAdapter(std::unique_ptr<HashTableVisitor> htVisitor);

    /**
     * Visit a hashtable within an epStore.
     *
     * @param vbucket_id ID of the vbucket being visited.
     * @param ht a reference to the hashtable.
     * @return True if visiting should continue (in the given HashTable),
     *         otherwise false if the HashTable is complete.
     */
    bool visit(uint16_t vbucket_id, HashTable& ht);

    /// Returns the current hashtable position.
    HashTable::Position getHashtablePosition() const {
        return hashtable_position;
    }

    /// Returns the wrapped HashTable visitor.
    HashTableVisitor& getHTVisitor() {
        return *htVisitor;
    }

private:
    // The HashTable visitor to apply to each VBucket's HashTable.
    std::unique_ptr<HashTableVisitor> htVisitor;

    // When resuming, which vbucket should we start from?
    uint16_t resume_vbucket_id = 0;

    // When pausing / resuming, hashtable position to use.
    HashTable::Position hashtable_position;
};
