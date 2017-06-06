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

#include "vb_filter.h"

class HashTable;
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
 * Base class for visiting an epStore with pause/resume support.
 */
class PauseResumeEPStoreVisitor {
public:
    virtual ~PauseResumeEPStoreVisitor() {
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
