/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "ep_task.h"
#include "kv_bucket_iface.h"
#include "pid_controller.h"

namespace cb {
class FragmentationStats;
}

class DefragmentVisitor;
class EPStats;
class PauseResumeVBAdapter;

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
 * *WORK IN PROGRESS*
 *
 * From a position outside the memory allocator is is hard
 * (impossible?) to know exactly which objects reside in a
 * sparsely-populated page and hence should be defragmented by
 * reallocating them to a more populous page.  Therefore we use a
 * number of heuristics to attempt to infer which objects would be
 * suitable candidates:
 *
 * 1. Document age - record when an object was last allocated and
 *    consider documents for defrag when they reach a particular age
 *    (measured in number of defragmenter sweeps they have existed
 *    for).
 *
 * 2. Document size - Skip documents which are larger than the largest
 *    size class, or are zero-sized.
 *
 * An additional policy consideration is how to locate
 * candidate documents. In a large instance, the simple act of
 * visiting each element in the HashTable is a expensive operation -
 * for example a no-op visitor for HashTable with 100k items takes
 * ~50ms. Real-world HashTables could easily have many 100s of
 * millions of items in them, so performing a full HashTable walk every
 * time would be very costly, forcing the walk to be relatively infrequent.
 *
 * Instead, we limit the duration of each defragmention invocation (chunk),
 * pause, and then later start the next chunk form where we left off.
 */
class DefragmenterTask : public EpTask {
public:
    DefragmenterTask(EventuallyPersistentEngine& e, EPStats& stats_);

    bool run() override;

    void stop();

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

    /// The duration of time the task was snoozed for during the last run.
    std::chrono::milliseconds getCurrentSleepTime() const;

    /// Maximum allocation size the defragmenter should consider
    static size_t getMaxValueSize();

    /// Type returned by auto control
    struct SleepTimeAndRunState {
        std::chrono::duration<double> sleepTime{0};
        bool runDefragger{false};
    };

protected:
    /// Main function called from run when defragmenter is enabled
    std::chrono::duration<double> defrag();

    /**
     * Calculate the current defragmenter sleep time and run state.
     * @param fragStats the bucket's current fragmentation state
     * @return SleepTimeAndRunState based on mode and current fragmentation
     */
    SleepTimeAndRunState calculateSleepTimeAndRunState(
            const cb::FragmentationStats& fragStats);

    /**
     * Calculate the sleep time using a mapping of fragmentation to the sleep
     * time.
     * @param fragStats the bucket's current fragmentation state
     * @return SleepTimeAndRunState based on current fragmentation
     */
    SleepTimeAndRunState calculateSleepLinear(
            const cb::FragmentationStats& fragStats);

    /**
     * Calculate the sleep time using a PID controller that will reduce the
     * sleep time whilst fragmentation is above preferred minimum
     * @param fragStats the bucket's current fragmentation state
     * @return SleepTimeAndRunState based on current fragmentation
     */

    SleepTimeAndRunState calculateSleepPID(
            const cb::FragmentationStats& fragStats);

    /**
     *  steps the PID and allows for a test sub-class to override realtime
     * @param pv The current process-variable the PID is tracking
     * @return The PID's output
     */
    virtual float stepPid(float pv);

    /**
     * The auto controller modes do not use raw fragmentation, but a 'scored'
     * value that is weighted by the ratio of RSS/high-water mark.
     *
     * This is done because bucket fragmentation alone is not a great guide for
     * how aggressive we would want to defragment the hash-table. For example
     * an empty or very low utilised bucket, can easily reach high fragmentation
     * percentages, yet running the defragmenter at high frequency has little
     * effect. Using the RSS/high-water mark ratio means helps to guide our
     * defragmentation sleep changes only when:
     * 1) There are items to defragment
     * 2) When the bucket is actually using a good chunk of its quota
     *
     * The current implementation of this returns the following example values:
     * Here we consider allocated:=500 and rss:=650, this gives a fragmentation
     * ratio of 0.23.
     *
     * Then with a high-water mark of n this function returns:
     *    n    | score
     *    600  | 0.23 (note this case, rss exceeds n, we fix weight to 1).
     *    1000 | 0.1495
     *    2000 | 0.074
     *    3000 | 0.0498
     *    5000 | 0.0299
     *
     * @return the scored fragmentation
     */
    float getScoredFragmentation(const cb::FragmentationStats& fragStats) const;

    // Minimum age (measured in defragmenter task passes) that a document
    // must be to be considered for defragmentation.
    uint8_t getAgeThreshold() const;

    // Minimum age (measured in defragmenter task passes) that a StoredValue
    // must be to be considered for defragmentation.
    uint8_t getStoredValueAgeThreshold() const;

    // Upper limit on how long each defragmention chunk can run for, before
    // being paused.
    std::chrono::milliseconds getChunkDuration() const;

    /// Returns the underlying DefragmentVisitor instance.
    DefragmentVisitor& getDefragVisitor();

    /// Update the EPStats from the visitor
    void updateStats(DefragmentVisitor& visitor);

    /// Reference to EP stats, used to check on mem_used.
    EPStats &stats;

    // Opaque marker indicating how far through the epStore we have visited.
    KVBucketIface::Position epstore_position;

    /**
     * Visitor adapter which supports pausing & resuming (records how far
     * though a VBucket is has got). unique_ptr as we re-create it for each
     * complete pass.
     */
    std::unique_ptr<PauseResumeVBAdapter> prAdapter;

    /**
     * PID which is used for sleep calculations when deframenter_mode==auto_pid
     */
    PIDController<> pid;

    /// A function so we can reset the PID on config changes
    std::function<bool(PIDControllerImpl&)> pidReset;

    /// The duration of time the task was snoozed for during the last run.
    std::chrono::milliseconds currentSleepTime{0};
};
