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

#include "defragmenter_test.h"

#include "checkpoint_manager.h"
#include "daemon/alloc_hooks.h"
#include "defragmenter.h"
#include "defragmenter_visitor.h"
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
    return cb::ArenaMalloc::getFragmentationStats(client).second;
}

/* Waits for mapped memory value to drop below the specified value, or for
 * the maximum_sleep_time to be reached.
 * @returns True if the mapped memory value dropped, or false if the maximum
 * sleep time was reached.
 */
static bool wait_for_mapped_below(const cb::ArenaMallocClient& client,
                                  size_t mapped_threshold,
                                  useconds_t max_sleep_time) {
    useconds_t sleepTime = 128;
    useconds_t totalSleepTime = 0;

    while (get_mapped_bytes(client) > mapped_threshold) {
        std::this_thread::sleep_for(std::chrono::microseconds(sleepTime));
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
        for (page_to_keys_t::iterator kv = page_to_keys.begin();
             kv != page_to_keys.end();
             kv++) {
            // Free all but one document on this page.
            while (kv->second.size() > 1) {
                auto doc_id = kv->second.back();
                // Use DocKey to minimize heap pollution
                snprintf(keyScratch, sizeof(keyScratch), keyPattern, doc_id);
                auto res = vbucket->ht.findForWrite(
                        DocKey(keyScratch, DocKeyEncodesCollectionId::No));
                ASSERT_TRUE(res.storedValue);
                vbucket->ht.unlocked_del(res.lock, res.storedValue);
                kv->second.pop_back();
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
    vbucket->checkpointManager->clear(vbucket->getState());
    // Record memory usage after creation.
    size_t mem_used_1 = global_stats.getPreciseTotalMemoryUsed();

    AllocHooks::release_free_memory();
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
    AllocHooks::release_free_memory();

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

    EXPECT_GE(current_mapped, size_t(0.80 * double(previous_mapped)))
            << "current_mapped memory (which is " << current_mapped
            << ") is lower than 80% of previous mapped (which is "
            << previous_mapped << "). ";

    // 3. Enable defragmenter and trigger defragmentation
    auto defragVisitor = std::make_unique<DefragmentVisitor>(
            DefragmenterTask::getMaxValueSize(
                    get_mock_server_api()->alloc_hooks));

    if (isModeStoredValue()) {
        // Force visiting of every item by setting a large deadline
        defragVisitor->setDeadline(std::chrono::steady_clock::now() +
                                   std::chrono::hours(5));
        // And set the age, which enables SV defragging
        defragVisitor->setStoredValueAgeThreshold(0);
    }

    PauseResumeVBAdapter prAdapter(std::move(defragVisitor));
    prAdapter.visit(*vbucket);

    AllocHooks::release_free_memory();

    // Check that mapped memory has decreased after defragmentation - should be
    // less than 50% of the amount before defrag (this is pretty conservative,
    // but it's hard to accurately predict the whole-application size).
    // Give it 10 seconds to drop.
    const size_t expected_mapped = ((mapped_2 - mapped_0) * 0.5) + mapped_0;

    auto& visitor = dynamic_cast<DefragmentVisitor&>(prAdapter.getHTVisitor());
    EXPECT_TRUE(wait_for_mapped_below(
            global_stats.arena, expected_mapped, 1 * 1000 * 1000))
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
    AllocHooks::release_free_memory();

    size_t mem_used_before_defrag = global_stats.getPreciseTotalMemoryUsed();

    // The refcounts of all blobs should at least 2 at this point as the
    // CheckpointManager will also be holding references to them.

    // 3. Enable defragmenter and trigger defragmentation but let the
    // defragmenter drop out of scope afterwards to avoid interfering
    // with the memory measurements.
    {
        AllocHooks::enable_thread_cache(false);

        PauseResumeVBAdapter prAdapter(std::make_unique<DefragmentVisitor>(
                DefragmenterTask::getMaxValueSize(
                        get_mock_server_api()->alloc_hooks)));
        prAdapter.visit(*vbucket);

        AllocHooks::enable_thread_cache(true);
        AllocHooks::release_free_memory();

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

    EXPECT_EQ(14336,
              DefragmenterTask::getMaxValueSize(
                      get_mock_server_api()->alloc_hooks));
}

INSTANTIATE_TEST_CASE_P(
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
