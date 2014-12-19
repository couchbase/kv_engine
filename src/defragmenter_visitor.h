/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#ifndef DEFRAGMENTER_VISITOR_H_
#define DEFRAGMENTER_VISITOR_H_

#include "ep.h"

class ProgressTracker;

/** Defragmentation visitor - visit all objects and defragment
 *
 */
class DefragmentVisitor : public PauseResumeEPStoreVisitor,
                          public PauseResumeHashTableVisitor {
public:
    DefragmentVisitor(uint8_t age_threshold_);

    ~DefragmentVisitor();

    // Set the deadline at which point the visitor will pause visiting.
    void setDeadline(hrtime_t deadline_);

    // Implementation of PauseResumeEPStoreVisitor interface:
    virtual bool visit(uint16_t vbucket_id, HashTable& ht);

    // Implementation of PauseResumeHashTableVisitor interface:
    virtual bool visit(StoredValue& v);

    // Returns the current hashtable position.
    HashTable::Position getHashtablePosition() const;

    // Resets any held stats to zero.
    void clearStats();

    // Returns the number of documents that have been defragmented.
    size_t getDefragCount() const;

    // Returns the number of documents that have been visited.
    size_t getVisitedCount() const;

private:
    /* Configuration parameters */

    // Size of the largest size class from the allocator.
    const size_t max_size_class;

    // How old a blob must be to consider it for defragmentation.
    const uint8_t age_threshold;

    /* Runtime state */

    // Estimates how far we have got, and when we should pause.
    ProgressTracker* progressTracker;

    // When resuming, which vbucket should we start from?
    uint16_t resume_vbucket_id;

    // When pausing / resuming, hashtable position to use.
    HashTable::Position hashtable_position;

    /* Statistics */
    // Count of how many documents have been defrag'd.
    size_t defrag_count;
    // How many documents have been visited.
    size_t visited_count;
};

#endif /* DEFRAGMENTER_VISITOR_H_ */
