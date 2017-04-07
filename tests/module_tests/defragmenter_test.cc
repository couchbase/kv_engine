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

#include "daemon/alloc_hooks.h"
#include "defragmenter_visitor.h"

#include <gtest/gtest.h>
#include <valgrind/valgrind.h>


/* Return how many bytes the memory allocator has mapped in RAM - essentially
 * application-allocated bytes plus memory in allocators own data structures
 * & freelists. This is an approximation of the the application's RSS.
 */
static size_t get_mapped_bytes(void) {
    size_t mapped_bytes;
    allocator_stats stats = {0};
    std::vector<allocator_ext_stat> extra_stats(AllocHooks::get_extra_stats_size());
    stats.ext_stats_size = extra_stats.size();
    stats.ext_stats = extra_stats.data();

    AllocHooks::get_allocator_stats(&stats);
    mapped_bytes = stats.heap_size - stats.free_mapped_size
        - stats.free_unmapped_size;
    return mapped_bytes;
}

/* Waits for mapped memory value to drop below the specified value, or for
 * the maximum_sleep_time to be reached.
 * @returns True if the mapped memory value dropped, or false if the maximum
 * sleep time was reached.
 */
static bool wait_for_mapped_below(size_t mapped_threshold,
                                  useconds_t max_sleep_time) {
    useconds_t sleepTime = 128;
    useconds_t totalSleepTime = 0;

    while (get_mapped_bytes() > mapped_threshold) {
        usleep(sleepTime);
        totalSleepTime += sleepTime;
        if (totalSleepTime > max_sleep_time) {
            return false;
        }
        sleepTime *= 2;
    }
    return true;
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
    ASSERT_TRUE(MemoryTracker::trackingMemoryAllocations())
        << "Memory tracker not enabled - cannot continue";

    // 0. Get baseline memory usage (before creating any objects).
    size_t mem_used_0 = mem_used.load();
    size_t mapped_0 = get_mapped_bytes();

    // 1. Create a number of small documents. Doesn't really matter that
    //    they are small, main thing is we create enough to span multiple
    //    pages (so we can later leave 'holes' when they are deleted).
    const size_t size = 128;
    const size_t num_docs = 50000;
    std::string data(size, 'x');
    for (unsigned int i = 0; i < num_docs; i++ ) {
        // Deliberately using C-style int-to-string conversion (instead of
        // stringstream) to minimize heap pollution.
        char key[16];
        snprintf(key, sizeof(key), "%d", i);
        // Use DocKey to minimize heap pollution
        Item item(DocKey(key, DocNamespace::DefaultCollection),
                  0,
                  0,
                  data.data(),
                  data.size());
        auto rv = public_processSet(item, 0);
        EXPECT_EQ(rv, MutationStatus::WasClean);
    }

    // Since public_processSet causes queueDirty to be run for each item above
    // we need to clear checkpointManager from having any references to the
    // items we want to delete
    vbucket->checkpointManager.clear(vbucket->getState());
    // Record memory usage after creation.
    size_t mem_used_1 = mem_used.load();
    size_t mapped_1 = get_mapped_bytes();

    // Sanity check - mem_used should be at least size * count bytes larger than
    // initial.
    EXPECT_GE(mem_used_1, mem_used_0 + (size * num_docs))
        << "mem_used smaller than expected after creating documents";

    // 2. Determine how many documents are in each page, and then remove all but
    //    one from each page.
    size_t num_remaining = num_docs;
    const size_t LOG_PAGE_SIZE = 12; // 4K page
    {
        typedef std::unordered_map<uintptr_t, std::vector<int> > page_to_keys_t;
        page_to_keys_t page_to_keys;

        // Build a map of pages to keys
        for (unsigned int i = 0; i < num_docs; i++ ) {
            /// Use stack and DocKey to minimuze heap pollution
            char key[16];
            snprintf(key, sizeof(key), "%d", i);
            auto* item = vbucket->ht.find(
                    DocKey(key, DocNamespace::DefaultCollection),
                    TrackReference::Yes,
                    WantsDeleted::No);
            ASSERT_NE(nullptr, item);

            const uintptr_t page =
                uintptr_t(item->getValue()->getData()) >> LOG_PAGE_SIZE;
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
                char key[16];
                snprintf(key, sizeof(key), "%d", doc_id);
                ASSERT_TRUE(vbucket->deleteKey(
                        DocKey(key, DocNamespace::DefaultCollection)));
                kv->second.pop_back();
                num_remaining--;
            }
        }
    }

    // Release free memory back to OS to minimize our footprint after
    // removing the documents above.
    AllocHooks::release_free_memory();

    // Sanity check - mem_used should have reduced down by approximately how
    // many documents were removed.
    // Allow some extra, to handle any increase in data structure sizes used
    // to actually manage the objects.
    const double fuzz_factor = 1.05;
    const size_t all_docs_size = mem_used_1 - mem_used_0;
    const size_t remaining_size = (all_docs_size / num_docs) * num_remaining;
    const size_t expected_mem = (mem_used_0 + remaining_size) * fuzz_factor;

    unsigned int retries;
    const int RETRY_LIMIT = 100;
    for (retries = 0; retries < RETRY_LIMIT; retries++) {
        if (mem_used.load() < expected_mem) {
            break;
        }
        usleep(100);
    }
    if (retries == RETRY_LIMIT) {
        FAIL() << "Exceeded retry count waiting for mem_used be below "
               << expected_mem << " current mem_used: " << mem_used.load();
    }

    size_t mapped_2 = get_mapped_bytes();

    // Sanity check (2) - mapped memory should still be high - at least 90% of
    // the value after creation, before delete.
    const size_t current_mapped = mapped_2 - mapped_0;
    const size_t previous_mapped = mapped_1 - mapped_0;

    EXPECT_GE(current_mapped, 0.9 * double(previous_mapped))
        << "current_mapped memory (which is " << current_mapped
        << ") is lower than 90% of previous mapped (which is "
        << previous_mapped << "). ";

    // 3. Enable defragmenter and trigger defragmentation
    AllocHooks::enable_thread_cache(false);

    DefragmentVisitor visitor(0);
    visitor.visit(vbucket->getId(), vbucket->ht);

    AllocHooks::enable_thread_cache(true);
    AllocHooks::release_free_memory();

    // Check that mapped memory has decreased after defragmentation - should be
    // less than 50% of the amount before defrag (this is pretty conservative,
    // but it's hard to accurately predict the whole-application size).
    // Give it 10 seconds to drop.
    const size_t expected_mapped = ((mapped_2 - mapped_0) * 0.5) + mapped_0;

    EXPECT_TRUE(wait_for_mapped_below(expected_mapped, 1 * 1000 * 1000))
        << "Mapped memory (" << get_mapped_bytes() << ") didn't fall below "
        << "estimate (" <<  expected_mapped << ") after the defragmentater "
        << "visited " << visitor.getVisitedCount() << " items "
        << "and moved " << visitor.getDefragCount() << " items!";
}

INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        DefragmenterTest,
        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            if (info.param == VALUE_ONLY) {
                return "VALUE_ONLY";
            } else {
                return "FULL_EVICTION";
            }
        });
