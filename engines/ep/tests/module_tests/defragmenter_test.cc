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

#include "defragmenter_test.h"

#include "checkpoint_manager.h"
#include "defragmenter.h"
#include "defragmenter_visitor.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "test_helpers.h"
#include "vbucket.h"

#include <valgrind/valgrind.h>
#include <chrono>
#include <thread>

/*
 * Return the resident bytes for the client
 */
static size_t get_mapped_bytes(const cb::ArenaMallocClient& client) {
    return cb::ArenaMalloc::getFragmentationStats(client).getResidentBytes();
}

/* Waits for mapped memory value to drop below the specified value, or for
 * the maximum_sleep_time to be reached.
 * @returns True if the mapped memory value dropped, or false if the maximum
 * sleep time was reached.
 */
static bool wait_for_mapped_below(const cb::ArenaMallocClient& client,
                                  size_t mapped_threshold,
                                  std::chrono::microseconds max_sleep_time) {
    std::chrono::microseconds sleepTime{128};
    std::chrono::microseconds totalSleepTime;

    while (get_mapped_bytes(client) > mapped_threshold) {
        std::this_thread::sleep_for(sleepTime);
        totalSleepTime += sleepTime;
        if (totalSleepTime > max_sleep_time) {
            return false;
        }
        sleepTime *= 2;
    }
    return true;
}

// Initialise the base class and the keyPattern, the keyPattern determines the
// key length, which should be large for StoredValue tests
DefragmenterTest::DefragmenterTest()
    : VBucketTestBase(VBType::Persistent, getEvictionPolicy()),
      keyPattern(isModeStoredValue() ? keyPattern2 : keyPattern1) {
}

DefragmenterTest::~DefragmenterTest() {
}

EvictionPolicy DefragmenterTest::getEvictionPolicy() const {
    return std::get<0>(GetParam());
}

bool DefragmenterTest::isModeStoredValue() const {
    return std::get<1>(GetParam());
}

void DefragmenterTest::setDocs(size_t docSize, size_t num_docs) {
    std::string data(docSize, 'x');
    for (unsigned int i = 0; i < num_docs; i++ ) {
        // Deliberately using C-style int-to-string conversion (instead of
        // stringstream) to minimize heap pollution.
        // 0 pad the key so they should all be the same length (and hit same
        // jemalloc bin)
        snprintf(keyScratch, sizeof(keyScratch), keyPattern, i);
        // Use DocKey to minimize heap pollution
        Item item(DocKey(keyScratch, DocKeyEncodesCollectionId::No),
                  0,
                  0,
                  data.data(),
                  data.size());
        auto rv = public_processSet(item, 0);
        EXPECT_EQ(rv, MutationStatus::WasClean);
    }
}

void DefragmenterTest::fragment(size_t num_docs, size_t& num_remaining) {
    num_remaining = num_docs;
    const size_t LOG_PAGE_SIZE = 12; // 4K page
    {
        typedef std::unordered_map<uintptr_t, std::vector<int> > page_to_keys_t;
        page_to_keys_t page_to_keys;

        // Build a map of pages to keys
        for (unsigned int i = 0; i < num_docs; i++ ) {
            /// Use stack and DocKey to minimuze heap pollution
            snprintf(keyScratch, sizeof(keyScratch), keyPattern, i);
            auto* item =
                    vbucket->ht
                            .findForRead(DocKey(keyScratch,
                                                DocKeyEncodesCollectionId::No))
                            .storedValue;
            ASSERT_NE(nullptr, item);

            uintptr_t address =
                    isModeStoredValue()
                            ? uintptr_t(item)
                            : uintptr_t(item->getValue()->getData());
            const uintptr_t page = address >> LOG_PAGE_SIZE;
            page_to_keys[page].emplace_back(i);
        }

        // Now remove all but one document from each page.
        for (auto& page_to_key : page_to_keys) {
            // Free all but one document on this page.
            while (page_to_key.second.size() > 1) {
                auto doc_id = page_to_key.second.back();
                // Use DocKey to minimize heap pollution
                snprintf(keyScratch, sizeof(keyScratch), keyPattern, doc_id);
                auto res = vbucket->ht.findForWrite(
                        DocKey(keyScratch, DocKeyEncodesCollectionId::No));
                ASSERT_TRUE(res.storedValue);
                vbucket->ht.unlocked_del(res.lock, res.storedValue);
                page_to_key.second.pop_back();
                num_remaining--;
            }
        }
    }
}


// Create a number of documents, spanning at least two or more pages, then
// delete most (but not all) of them - crucially ensure that one document from
// each page is still present. This will result in the rest of that page
// being 'wasted'. Then verify that after defragmentation the actual memory
// usage drops down to (close to) mem_used.

/* The Defragmenter (and hence it's unit tests) depend on using jemalloc as the
 * memory allocator.
 */
#if defined(HAVE_JEMALLOC)
TEST_P(DefragmenterTest, MappedMemory) {
#else
TEST_P(DefragmenterTest, DISABLED_MappedMemory) {
#endif

    /*
      MB-22016:
      Disable this test for valgrind as it currently triggers some problems
      within jemalloc that will get detected much further down the set of
      unit-tests.

      The problem appears to be the toggling of thread cache, I suspect that
      jemalloc is writing some data when the thread cache is off (during the
      defrag) and then accessing that data differently with thread cache on.

      The will link to an issue on jemalloc to see if there is anything to be
      changed.
    */
    if (RUNNING_ON_VALGRIND) {
        printf("DefragmenterTest.MappedMemory is currently disabled for valgrind\n");
        return;
    }

    // Sanity check - need memory tracker to be able to check our memory usage.
    ASSERT_TRUE(cb::ArenaMalloc::canTrackAllocations())
            << "Memory tracker not enabled - cannot continue";

    // 0. Get baseline memory usage (before creating any objects).
    size_t mem_used_0 = global_stats.getPreciseTotalMemoryUsed();
    size_t mapped_0 = get_mapped_bytes(global_stats.arena);

    // 1. Create a number of documents.
    // When testing value de-fragment - Size doesn't matter too much, main thing
    // is we create enough to span multiple pages (so we can later leave 'holes'
    // when they are deleted). When testing StoredValue de-fragment use a zero
    // size so value's are ignored.
    const size_t size = isModeStoredValue() ? 0 : 512;
    const size_t num_docs = 5000;
    setDocs(size, num_docs);

    // Since public_processSet causes queueDirty to be run for each item above
    // we need to clear checkpointManager from having any references to the
    // items we want to delete
    vbucket->checkpointManager->clear();
    // Record memory usage after creation.
    size_t mem_used_1 = global_stats.getPreciseTotalMemoryUsed();

    cb::ArenaMalloc::releaseMemory(global_stats.arena);
    size_t mapped_1 = get_mapped_bytes(global_stats.arena);

    // Sanity check - mem_used should be at least size * count bytes larger than
    // initial.
    EXPECT_GE(mem_used_1, mem_used_0 + (size * num_docs))
            << "mem_used:" << mem_used_1
            << " smaller than expected:" << mem_used_0 + (size * num_docs)
            << " after creating documents";

    // 2. Determine how many documents are in each page, and then remove all but
    //    one from each page.
    size_t num_remaining = num_docs;
    fragment(num_docs, num_remaining);

    // Release free memory back to OS to minimize our footprint after
    // removing the documents above.
    cb::ArenaMalloc::releaseMemory(global_stats.arena);

    // Sanity check - mem_used should have reduced down by approximately how
    // many documents were removed.
    // Allow some extra, to handle any increase in data structure sizes used
    // to actually manage the objects.
    const double fuzz_factor = 1.15;
    const size_t all_docs_size = mem_used_1 - mem_used_0;
    const size_t remaining_size = (all_docs_size / num_docs) * num_remaining;
    const size_t expected_mem = (mem_used_0 + remaining_size) * fuzz_factor;

    unsigned int retries;
    const int RETRY_LIMIT = 100;
    for (retries = 0; retries < RETRY_LIMIT; retries++) {
        if (global_stats.getPreciseTotalMemoryUsed() < expected_mem) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    if (retries == RETRY_LIMIT) {
        FAIL() << "Exceeded retry count waiting for mem_used be below "
               << expected_mem << " current mem_used: "
               << global_stats.getPreciseTotalMemoryUsed();
    }

    size_t mapped_2 = get_mapped_bytes(global_stats.arena);

    // Sanity check (2) - mapped memory should still be high - at least 70% of
    // the value after creation, before delete.
    const size_t current_mapped = mapped_2 - mapped_0;
    const size_t previous_mapped = mapped_1 - mapped_0;

    EXPECT_GE(current_mapped, size_t(0.70 * double(previous_mapped)))
            << "current_mapped memory (which is " << current_mapped
            << ") is lower than 70% of previous mapped (which is "
            << previous_mapped << "). ";

    // 3. Enable defragmenter and trigger defragmentation
    auto defragVisitor = std::make_unique<DefragmentVisitor>(
            DefragmenterTask::getMaxValueSize());

    if (isModeStoredValue()) {
        // Force visiting of every item by setting a large deadline
        defragVisitor->setDeadline(std::chrono::steady_clock::now() +
                                   std::chrono::hours(5));
        // And set the age, which enables SV defragging
        defragVisitor->setStoredValueAgeThreshold(0);
    }

    PauseResumeVBAdapter prAdapter(std::move(defragVisitor));
    prAdapter.visit(*vbucket);

    cb::ArenaMalloc::releaseMemory(global_stats.arena);

    // Check that mapped memory has decreased after defragmentation - should be
    // less than 50% of the amount before defrag (this is pretty conservative,
    // but it's hard to accurately predict the whole-application size).
    // Give it 10 seconds to drop.
    const size_t expected_mapped = ((mapped_2 - mapped_0) * 0.5) + mapped_0;

    auto& visitor = dynamic_cast<DefragmentVisitor&>(prAdapter.getHTVisitor());
    EXPECT_TRUE(wait_for_mapped_below(
            global_stats.arena, expected_mapped, std::chrono::seconds(1)))
            << "Mapped memory (" << get_mapped_bytes(global_stats.arena)
            << ") didn't fall below "
            << "estimate (" << expected_mapped << ") after the defragmentater "
            << "visited " << visitor.getVisitedCount() << " items "
            << "and moved " << visitor.getDefragCount() << " items!";

    if (isModeStoredValue()) {
        EXPECT_EQ(num_remaining, visitor.getStoredValueDefragCount());
    }
}

// Check that the defragmenter doesn't increase the memory used. The specific
// case we are testing here is what happens when a reference to the blobs is
// also held by the Checkpoint Manager. See MB-23263.
/* The Defragmenter (and hence it's unit tests) depend on using jemalloc as the
 * memory allocator.
 */
#if defined(HAVE_JEMALLOC)
TEST_P(DefragmenterTest, RefCountMemUsage) {
#else
TEST_P(DefragmenterTest, DISABLED_RefCountMemUsage) {
#endif
    // Currently not adapted for StoredValue defragging
    if (isModeStoredValue()) {
        return;
    }

    /*
     * Similarly to the MappedMemory test, this test appears to cause issues
     * with Valgrind. So it is disabled under Valgrind for now. I've included
     * the same explanation below for completeness.
     *
      MB-22016:
      Disable this test for valgrind as it currently triggers some problems
      within jemalloc that will get detected much further down the set of
      unit-tests.

      The problem appears to be the toggling of thread cache, I suspect that
      jemalloc is writing some data when the thread cache is off (during the
      defrag) and then accessing that data differently with thread cache on.

      The will link to an issue on jemalloc to see if there is anything to be
      changed.
    */
    if (RUNNING_ON_VALGRIND) {
        printf("DefragmenterTest.RefCountMemUsage is currently disabled for"
               " valgrind\n");
        return;
    }

    // Sanity check - need memory tracker to be able to check our memory usage.
    ASSERT_TRUE(cb::ArenaMalloc::canTrackAllocations())
            << "Memory tracker not enabled - cannot continue";

    // 1. Create a small number of documents to record the checkpoint manager
    // overhead with
    const size_t size = 3500;
    const size_t num_docs = 5000;
    setDocs(size, num_docs);

    // 2. Determine how many documents are in each page, and then remove all but
    //    one from each page.
    size_t num_remaining = num_docs;
    fragment(num_docs, num_remaining);

    // Release free memory back to OS to minimize our footprint after
    // removing the documents above.
    cb::ArenaMalloc::releaseMemory(global_stats.arena);

    size_t mem_used_before_defrag = global_stats.getPreciseTotalMemoryUsed();

    // The refcounts of all blobs should at least 2 at this point as the
    // CheckpointManager will also be holding references to them.

    // 3. Enable defragmenter and trigger defragmentation but let the
    // defragmenter drop out of scope afterwards to avoid interfering
    // with the memory measurements.
    {
        cb::ArenaMalloc::switchToClient(global_stats.arena, false);

        PauseResumeVBAdapter prAdapter(std::make_unique<DefragmentVisitor>(
                DefragmenterTask::getMaxValueSize()));
        prAdapter.visit(*vbucket);

        cb::ArenaMalloc::switchToClient(global_stats.arena, true);

        cb::ArenaMalloc::releaseMemory(global_stats.arena);

        auto& visitor =
                dynamic_cast<DefragmentVisitor&>(prAdapter.getHTVisitor());
        ASSERT_EQ(visitor.getVisitedCount(), num_remaining);
        ASSERT_EQ(visitor.getDefragCount(), 0);
    }

    size_t mem_used_after_defrag = global_stats.getPreciseTotalMemoryUsed();

    EXPECT_LE(mem_used_after_defrag, mem_used_before_defrag);
}

#if defined(HAVE_JEMALLOC)
TEST_P(DefragmenterTest, MaxDefragValueSize) {
#else
TEST_P(DefragmenterTest, DISABLED_MaxDefragValueSize) {
#endif

    EXPECT_EQ(14336, DefragmenterTask::getMaxValueSize());
}

class DefragmenterTaskTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override {
        // Hard wire the PID to these values so we can change the config without
        // having to fix the PID test.
        config_string +=
                "defragmenter_auto_lower_threshold=0.07;"
                "defragmenter_auto_pid_p=3.0;"
                "defragmenter_auto_pid_i=0.0000197;"
                "defragmenter_auto_pid_d=0.0;"
                "defragmenter_auto_pid_dt=30000;"
                "defragmenter_auto_max_sleep=10.0;"
                "defragmenter_auto_max_sleep=2.0";
        SingleThreadedKVBucketTest::SetUp();
    }
};

class MockDefragmenterTask : public DefragmenterTask {
public:
    MockDefragmenterTask(EventuallyPersistentEngine* engine, EPStats& stats)
        : DefragmenterTask(engine, stats),
          testPid(engine->getConfiguration()
                          .getDefragmenterAutoLowerThreshold(),
                  engine->getConfiguration().getDefragmenterAutoPidP(),
                  engine->getConfiguration().getDefragmenterAutoPidI(),
                  engine->getConfiguration().getDefragmenterAutoPidD(),
                  std::chrono::milliseconds{
                          engine->getConfiguration()
                                  .getDefragmenterAutoPidDt()}) {
    }

    DefragmenterTask::SleepTimeAndRunState public_calculateSleepPID(
            const cb::FragmentationStats& fragStats) {
        return calculateSleepPID(fragStats);
    }

    DefragmenterTask::SleepTimeAndRunState public_calculateSleepLinear(
            const cb::FragmentationStats& fragStats) {
        return calculateSleepLinear(fragStats);
    }

    struct MockDefragmenterTaskClock {
        static std::chrono::time_point<std::chrono::steady_clock> now() {
            static std::chrono::milliseconds currentTime{0};
            currentTime += step;
            return std::chrono::time_point<std::chrono::steady_clock>{
                    std::chrono::steady_clock::duration{currentTime}};
        }
        static std::chrono::milliseconds step;
    };

    // override stepPid so we can move time
    float stepPid(float pv) override {
        return testPid.step(pv, pidReset);
    }

    PIDController<MockDefragmenterTaskClock> testPid;
};

std::chrono::milliseconds MockDefragmenterTask::MockDefragmenterTaskClock::step;

// Test that as the PID runs (and we move time forwards) that it assists in the
// recalculation of sleep time, reducing whilst fragmentation is above threshold
TEST_F(DefragmenterTaskTest, autoCalculateSleep_PID) {
    // set hwm to some low number
    engine->getEpStats().setHighWaterMark(750);
    auto task = std::make_unique<MockDefragmenterTask>(engine.get(),
                                                       engine->getEpStats());

    auto& conf = engine->getConfiguration();
    // Set the step to equal the PID duration - this means each step changes
    // the output.
    MockDefragmenterTask::MockDefragmenterTaskClock::step =
            std::chrono::milliseconds(conf.getDefragmenterAutoPidDt());

    // Fragmentation is 0%, the PID won't adjust sleepTime and it should say
    // false for run
    auto state =
            task->public_calculateSleepPID(cb::FragmentationStats{100, 100});
    EXPECT_EQ(conf.getDefragmenterAutoMaxSleep(), state.sleepTime.count());
    EXPECT_FALSE(state.runDefragger);

    // Calculate sleep with "high fragmentation".
    // We don't just chose simple numbers like 100/130 (30%) as we have to be
    // closer to the low water mark (750) for the PID to begin calculations
    state = task->public_calculateSleepPID(cb::FragmentationStats{400, 490});
    EXPECT_GT(conf.getDefragmenterAutoMaxSleep(), state.sleepTime.count());

    // Don't expect the PID to have pulled below the min in just one call
    EXPECT_LT(conf.getDefragmenterAutoMinSleep(), state.sleepTime.count());
    EXPECT_TRUE(state.runDefragger);

    // Now loop a few times with some heavy fragmentation, this will pull down
    // to min sleep in a number of steps (checked manually and it is 5 steps)
    std::chrono::duration<double> sleep(conf.getDefragmenterAutoMaxSleep());
    int iterations = 0;
    do {
        state = task->public_calculateSleepPID(
                cb::FragmentationStats{380, 700});
        EXPECT_TRUE(state.runDefragger);
        // Each step should see a reduction
        EXPECT_LT(state.sleepTime, sleep);
        sleep = state.sleepTime;
        iterations++;
    } while (state.sleepTime.count() > conf.getDefragmenterAutoMinSleep());
    EXPECT_EQ(iterations, 5);

    // PID never goes below min
    state = task->public_calculateSleepPID(cb::FragmentationStats{10, 5000});
    EXPECT_EQ(conf.getDefragmenterAutoMinSleep(), state.sleepTime.count());
    EXPECT_TRUE(state.runDefragger);

    // Finally reconfigure.
    // Test the PID spots a config change - let's set P I D to 0, so the PID
    // returns 0 on the next step meaning it will now move back to maxSleep
    conf.setDefragmenterAutoPidP(0.0);
    conf.setDefragmenterAutoPidI(0.0);
    conf.setDefragmenterAutoPidD(0.0);

    // High fragmentation yet PID is 'disabled' so returns max sleep
    state = task->public_calculateSleepPID(cb::FragmentationStats{400, 600});
    EXPECT_EQ(conf.getDefragmenterAutoMaxSleep(), state.sleepTime.count());

    // The high fragmentation so the defragger should run
    EXPECT_TRUE(state.runDefragger);
}

TEST_F(DefragmenterTaskTest, autoCalculateSleep) {
    // set hwm to some low number
    engine->getEpStats().setHighWaterMark(750);
    auto task = std::make_unique<MockDefragmenterTask>(engine.get(),
                                                       engine->getEpStats());
    auto& conf = engine->getConfiguration();

    // Fragmentation is 0%, no change from max sleep
    auto state =
            task->public_calculateSleepLinear(cb::FragmentationStats{100, 100});
    EXPECT_EQ(conf.getDefragmenterAutoMaxSleep(), state.sleepTime.count());
    EXPECT_FALSE(state.runDefragger);

    // Set fragmentation to be high, expect to see a reduction in sleep
    state = task->public_calculateSleepLinear(cb::FragmentationStats{100, 170});
    // sleep less than max
    EXPECT_LT(state.sleepTime.count(), conf.getDefragmenterAutoMaxSleep());

    // The numbers chosen, expect to be above min
    EXPECT_LT(conf.getDefragmenterAutoMinSleep(), state.sleepTime.count());
    EXPECT_TRUE(state.runDefragger);

    // Now back to max
    state = task->public_calculateSleepLinear(cb::FragmentationStats{100, 101});
    EXPECT_EQ(conf.getDefragmenterAutoMaxSleep(), state.sleepTime.count());
    EXPECT_FALSE(state.runDefragger);

    // Finally dire fragmentation, and we drop to min sleep
    state = task->public_calculateSleepLinear(
            cb::FragmentationStats{100, 50000});
    EXPECT_EQ(conf.getDefragmenterAutoMinSleep(), state.sleepTime.count());
    EXPECT_TRUE(state.runDefragger);
}

INSTANTIATE_TEST_SUITE_P(
        FullAndValueEviction,
        DefragmenterTest,
        ::testing::Combine(::testing::Values(EvictionPolicy::Value,
                                             EvictionPolicy::Full),
                           ::testing::Bool()),
        [](const ::testing::TestParamInfo<std::tuple<EvictionPolicy, bool>>&
                   info) {
            std::string name = to_string(std::get<0>(info.param));
            if (std::get<1>(info.param)) {
                name += "_DefragStoredValue";
            }
            return name;
        });
