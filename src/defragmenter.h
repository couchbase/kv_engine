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

#ifndef DEFRAGMENTER_H_
#define DEFRAGMENTER_H_

#include "config.h"

#include "tasks.h"

class EPStats;

/** Task responsible for defragmenting items in memory.
 *
 * Background
 * ==========
 *
 * Our underlying memory allocator library allocates memory from a set of size
 * classes, dedicating a given page to only be used for that specific size.
 * While this is fast and efficient to allocate, it can result in heap
 * fragmentation. Consider two 4K pages assigned to a particular size of
 * allocation (e.g. 32 bytes):
 *
 * 1. We store sufficient objects of this size to use up both pages:
 *    256 * 32bytes = 8192.
 * 2. We then delete (or maybe resize) the majority of them, leaving only two
 *    objects remaining - but crucially *one on each of the two pages*.
 *
 * As a result, while mem_used should be ~64 bytes, our actual resident set size
 * will still be 8192 bytes.
 *
 * This kind of problem is compounded across different size classes.
 *
 * This is particularly problematic for workloads where the average document
 * size slowly grows over time, or where there is a minority of documents which
 * are 'static' and are never grow or shrink in size, meaning they will never
 * be re-allocated and will stay at the original address they were allocated.
 *
 * Algorithm
 * =========
 *
 * We solve this problem by having a background task which will walk across
 * documents and 'move' them to a new location. As we are not in control of the
 * allocator this is done in a somewhat naive way - for each of the objects
 * making up a document we simply allocate a new object (of the same size),
 * copy to the existing object to the new one and free the old object, relying
 * on the underlying allocator being smart enough to choose a "better"
 * location.
 *
 * (Note: jemalloc guarantees that it will always return the lowest possible
 * address for an allocation, and so the aforementioned naive scheme should
 * work well. TCMalloc may or may not work as well...)
 *
 * Policy
 * ======
 *
 * TODO: TBD
 *
 */
class DefragmenterTask : public GlobalTask {
public:
    DefragmenterTask(EventuallyPersistentEngine* e, EPStats& stats_,
                     size_t sleep_time_);

    bool run(void);

    void stop(void);

    std::string getDescription();

private:
    /// Return the current number of mapped bytes from the allocator.
    size_t get_mapped_bytes();

    /// Reference to EP stats, used to check on mem_used.
    EPStats &stats;

    /// Duration (in seconds) defragmenter should sleep for between iterations.
    size_t sleep_time;
};


#endif /* DEFRAGMENTER_H_ */
