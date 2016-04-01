/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"

#include <atomic>
#if defined(HAVE_MEMALIGN)
#include <malloc.h>
#endif

#include "daemon/alloc_hooks.h"

#include <gtest/gtest.h>

// Test pointer in global scope to prevent compiler optimizing malloc/free away
// via DCE.
char* p;

class MemoryTrackerTest : public ::testing::Test {
public:
    // callback function for when memory is allocated.
    static void NewHook(const void* ptr, size_t) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size += mc_get_allocation_size(p);
        }
    }

    // callback function for when memory is deleted.
    static void DeleteHook(const void* ptr) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size -= mc_get_allocation_size(p);
        }
    }

    // function executed by our accounting test thread.
    static void AccountingTestThread(void* arg);

    static std::atomic_size_t alloc_size;
};

std::atomic_size_t MemoryTrackerTest::alloc_size;


void MemoryTrackerTest::AccountingTestThread(void* arg) {
    alloc_size = 0;

    // Test new & delete //////////////////////////////////////////////////
    p = new char();
    EXPECT_GT(alloc_size, 0);
    delete p;
    EXPECT_EQ(0, alloc_size);

    // Test new[] & delete[] //////////////////////////////////////////////
    p = new char[100];
    EXPECT_GE(alloc_size, 100);
    delete []p;
    EXPECT_EQ(0, alloc_size);

    // Test malloc() / free() /////////////////////////////////////////////
    p = static_cast<char*>(malloc(sizeof(char) * 10));
    EXPECT_GE(alloc_size, 10);
    free(p);
    EXPECT_EQ(0, alloc_size);

    // Test realloc() /////////////////////////////////////////////////////
    p = static_cast<char*>(malloc(1));
    EXPECT_GE(alloc_size, 1);

    // Allocator may round up allocation sizes; so it's hard to
    // accurately predict how much alloc_size will increase. Hence
    // we just increase by a "large" amount and check at least half that
    // increment.
    size_t prev_size = alloc_size;
    p = static_cast<char*>(realloc(p, sizeof(char) * 100));
    EXPECT_GE(alloc_size, (prev_size + 50));

    prev_size = alloc_size;
    p = static_cast<char*>(realloc(p, 1));
    EXPECT_LT(alloc_size, prev_size);

    prev_size = alloc_size;
    char* q = static_cast<char*>(realloc(NULL, 10));
    EXPECT_GE(alloc_size, prev_size + 10);

    free(p);
    free(q);
    EXPECT_EQ(0, alloc_size);

    // Test calloc() //////////////////////////////////////////////////////
    p = static_cast<char*>(calloc(sizeof(char), 20));
    EXPECT_GE(alloc_size, 20);
    free(p);
    EXPECT_EQ(0, alloc_size);

    // Test indirect use of malloc() via strdup() /////////////////////////
    p = strdup("random string");
    EXPECT_GE(alloc_size, sizeof("random string"));
    free(p);
    EXPECT_EQ(0, alloc_size);

#if defined(HAVE_MEMALIGN)
    // Test memalign //////////////////////////////////////////////////////
    p = static_cast<char*>(memalign(16, 64));
    EXPECT_GE(alloc_size, 64);
    free(p);
    EXPECT_EQ(0, alloc_size);

    // Test posix_memalign ////////////////////////////////////////////////
    void* ptr;
    cb_assert(posix_memalign(&ptr, 16, 64) == 0);
    EXPECT_GE(alloc_size, 64);
    free(ptr);
    EXPECT_EQ(0, alloc_size);
#endif
}

// Test that the various memory allocation / deletion functions are correctly
// accounted for, when run in a parallel thread.
TEST_F(MemoryTrackerTest, Accounting) {
    mc_add_new_hook(NewHook);
    mc_add_delete_hook(DeleteHook);

    cb_thread_t tid;
    ASSERT_EQ(0, cb_create_thread(&tid, AccountingTestThread, 0, 0));
    ASSERT_EQ(0, cb_join_thread(tid));

    mc_remove_new_hook(NewHook);
    mc_remove_delete_hook(DeleteHook);
}


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    init_alloc_hooks();

    return RUN_ALL_TESTS();
}
