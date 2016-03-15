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

#include "defragmenter_visitor.h"

#include "daemon/alloc_hooks.h"

#include <gtest/gtest.h>
#include <iomanip>
#include <locale>

#ifdef HAVE_JEMALLOC
/* Global replacement of operators new and delete when using jemalloc.
 *
 * This isn't needed for functionality reasons (the code works
 * correctly without these), but to improve interoperability with
 * Valgrind. The issue encountered is that without these replacements,
 * the application will call the standard symbols in libstdc++, which
 * Valgrind intercepts, and replaces with it's own implementation
 * (which doesn't call 'our' je_malloc). As a consequence when the
 * memory tracking code (objectregistery.cc) later calls
 * je_malloc_usable_size() on the returned pointer from Valgrind's
 * operator new, we encounter invalid memory accesses as jemalloc is
 * trying to introspect a non-jemalloc allocation.
 *
 * By replacing operator new/delete in the executable program, we call
 * our own malloc/free (see alloc_hooks.c) which directly call
 * jemalloc.  Note that Valgrind reporting still works, as je_malloc
 * is able to detect which running in Valgrind and makes the
 * appropriate calls to Valgrind to inform it of allocations / frees.
 */
void* operator new(std::size_t count ) {
    return malloc(count);
}

void operator delete(void* ptr ) noexcept
{
    free(ptr);
}
#endif // HAVE_JEMALLOC


static time_t start_time;

static time_t mock_abstime(const rel_time_t exptime) {
    return start_time + exptime;
}

static rel_time_t mock_current_time(void) {
    rel_time_t result = (rel_time_t)(time(NULL) - start_time);
    return result;
}

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) {
        (void) dummy;
    }
};

/* Fill the bucket with the given number of docs. Returns the rate at which
 * items were added.
 */
static size_t populateVbucket(VBucket& vbucket, size_t ndocs) {

    /* Set the hashTable to a sensible size */
    vbucket.ht.resize(ndocs);

    /* Store items */
    char value[256];
    hrtime_t start = gethrtime();
    for (size_t i = 0; i < ndocs; i++) {
        std::stringstream ss;
        ss << "key" << i;
        const std::string key = ss.str();
        Item item(key.c_str(), key.length(), 0, 0, value, sizeof(value));
        vbucket.ht.add(item, VALUE_ONLY);
    }
    hrtime_t end = gethrtime();

    // Let hashTable set itself to correct size, post-fill
    vbucket.ht.resize();

    double duration_s = (end - start) / double(1000 * 1000 * 1000);
    return size_t(ndocs / duration_s);
}

/* Measure the rate at which the defragmenter can defragment documents, using
 * the given age threshold.
 *
 * Setup a Defragmenter, then time how long it takes to visit them all
 * documents in the given vbucket, npasses times.
 */
static size_t benchmarkDefragment(VBucket& vbucket, size_t passes,
                                  uint8_t age_threshold,
                                  size_t chunk_duration_ms) {
    // Create and run visitor for the specified number of iterations, with
    // the given age.
    DefragmentVisitor visitor(age_threshold);
    hrtime_t start = gethrtime();
    for (size_t i = 0; i < passes; i++) {
        // Loop until we get to the end; this may take multiple chunks depending
        // on the chunk_duration.
        HashTable::Position pos;
        while (pos != vbucket.ht.endPosition()) {
            visitor.setDeadline(gethrtime() +
                                 (chunk_duration_ms * 1000 * 1000));
            pos = vbucket.ht.pauseResumeVisit(visitor, pos);
        }
    }
    hrtime_t end = gethrtime();
    size_t visited = visitor.getVisitedCount();

    double duration_s = (end - start) / double(1000 * 1000 * 1000);
    return size_t(visited / duration_s);
}

class DefragmenterBenchmarkTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        /* Create the vbucket */
        std::shared_ptr<Callback<uint16_t> > cb(new DummyCB());
        vbucket.reset(new VBucket(0, vbucket_state_active, stats, config,
                                  nullptr, 0, 0, 0, nullptr, cb));
    }

    static void TearDownTestCase() {
        vbucket.reset();
    }

    static EPStats stats;
    static CheckpointConfig config;
    static std::unique_ptr<VBucket> vbucket;
};

EPStats DefragmenterBenchmarkTest::stats;
CheckpointConfig DefragmenterBenchmarkTest::config;
std::unique_ptr<VBucket> DefragmenterBenchmarkTest::vbucket;


TEST_F(DefragmenterBenchmarkTest, Populate) {
    size_t populateRate = populateVbucket(*vbucket, 500000);
    RecordProperty("items_per_sec", populateRate);
}

TEST_F(DefragmenterBenchmarkTest, Visit) {
    const size_t one_minute = 60 * 1000;
    size_t visit_rate = benchmarkDefragment(*vbucket, 1,
                                            std::numeric_limits<uint8_t>::max(),
                                            one_minute);
    RecordProperty("items_per_sec", visit_rate);
}

TEST_F(DefragmenterBenchmarkTest, DefragAlways) {
    const size_t one_minute = 60 * 1000;
    size_t defrag_always_rate = benchmarkDefragment(*vbucket, 1, 0,
                                                    one_minute);
    RecordProperty("items_per_sec", defrag_always_rate);
}

TEST_F(DefragmenterBenchmarkTest, DefragAge10) {
    const size_t one_minute = 60 * 1000;
    size_t defrag_age10_rate = benchmarkDefragment(*vbucket, 1, 10,
                                                   one_minute);
    RecordProperty("items_per_sec", defrag_age10_rate);
}

TEST_F(DefragmenterBenchmarkTest, DefragAge10_20ms) {
    size_t defrag_age10_20ms_rate = benchmarkDefragment(*vbucket, 1, 10, 20);
    RecordProperty("items_per_sec", defrag_age10_20ms_rate);
}


/* Return how many bytes the memory allocator has mapped in RAM - essentially
 * application-allocated bytes plus memory in allocators own data structures
 * & freelists. This is an approximation of the the application's RSS.
 */
static size_t get_mapped_bytes(void) {
    size_t mapped_bytes;
    allocator_stats stats = {0};
    std::vector<allocator_ext_stat> extra_stats(mc_get_extra_stats_size());
    stats.ext_stats_size = extra_stats.size();
    stats.ext_stats = extra_stats.data();

    mc_get_allocator_stats(&stats);
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

ALLOCATOR_HOOKS_API* getHooksApi(void) {
    static ALLOCATOR_HOOKS_API hooksApi;
    hooksApi.add_new_hook = mc_add_new_hook;
    hooksApi.remove_new_hook = mc_remove_new_hook;
    hooksApi.add_delete_hook = mc_add_delete_hook;
    hooksApi.remove_delete_hook = mc_remove_delete_hook;
    hooksApi.get_extra_stats_size = mc_get_extra_stats_size;
    hooksApi.get_allocator_stats = mc_get_allocator_stats;
    hooksApi.get_allocation_size = mc_get_allocation_size;
    return &hooksApi;
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
TEST(DefragmenterTest, MappedMemory) {
#else
TEST(DefragmenterTest, DISABLED_MappedMemory) {
#endif

    // Instantiate memory tracker (singleton created on-demand).
    (void)MemoryTracker::getInstance();

    // Setup object registry. As we do not create a full ep-engine, we
    // use the "initial_tracking" for all memory tracking".
    std::atomic<size_t> mem_used(0);
    ObjectRegistry::setStats(&mem_used);

    // Sanity check - need memory tracker to be able to check our memory usage.
    ASSERT_TRUE(MemoryTracker::trackingMemoryAllocations())
        << "Memory tracker not enabled - cannot continue";

    // 0. Get baseline memory usage (before creating any objects).
    size_t mem_used_0 = mem_used.load();
    size_t mapped_0 = get_mapped_bytes();

    /* Create the vbucket */
    std::shared_ptr<Callback<uint16_t> > cb(new DummyCB());
    EPStats stats;
    CheckpointConfig config;
    VBucket vbucket(0, vbucket_state_active, stats, config, nullptr, 0, 0, 0,
                    nullptr, cb);

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

        Item item(key, strlen(key), 0, 0, data.data(), data.size());;
        vbucket.ht.add(item, VALUE_ONLY);
    }

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
            std::string key(std::to_string(i));
            auto* item = vbucket.ht.find(key);
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
                std::string key(std::to_string(doc_id));

                ASSERT_TRUE(vbucket.ht.del(key));
                kv->second.pop_back();
                num_remaining--;
            }
        }
    }

    // Release free memory back to OS to minimize our footprint after
    // removing the documents above.
    mc_release_free_memory();

    // Sanity check - mem_used should have reduced down by approximately how
    // many documents were removed.
    // Allow some extra, to handle any increase in data structure sizes used
    // to actually manage the objects.
    const double fuzz_factor = 1.2;
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
               << expected_mem;
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
    mc_enable_thread_cache(false);

    DefragmentVisitor visitor(0);
    visitor.visit(vbucket.getId(), vbucket.ht);

    mc_enable_thread_cache(true);
    mc_release_free_memory();

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

    MemoryTracker::destroyInstance();
}


static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=1";

int main(int argc, char** argv) {
    /* Setup mock time functions */
    start_time = time(0);
    ep_abs_time = mock_abstime;
    ep_current_time = mock_current_time;

    putenv(allow_no_stats_env);

    // Set number of hashtable locks equal to current JSON config default.
    HashTable::setDefaultNumLocks(47);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
