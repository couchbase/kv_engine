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
#include "daemon/alloc_hooks.h"

#include <folly/portability/GTest.h>
#include <platform/cb_arena_malloc.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>

#if defined(HAVE_MALLOC_USABLE_SIZE)
#include <malloc.h>
#endif

// Test pointer in global scope to prevent compiler optimizing malloc/free away
// via DCE.
char* p;

class MemoryTrackerTest : public ::testing::Test {
public:
    MemoryTrackerTest() {
        client = cb::ArenaMalloc::registerClient();
    }
    ~MemoryTrackerTest() {
        cb::ArenaMalloc::unregisterClient(client);
    }

    // function executed by our accounting test thread.
    void AccountingTestThread();
    std::thread thread;
    cb::ArenaMallocClient client;
};

void MemoryTrackerTest::AccountingTestThread() {
    cb::ArenaMallocGuard guard(client);

    // Test new & delete //////////////////////////////////////////////////
    p = new char();
    EXPECT_GT(cb::ArenaMalloc::getPreciseAllocated(client), 0);
    delete p;
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test sized delete //////////////////////////////////////////////////
    p = new char();
    EXPECT_GT(cb::ArenaMalloc::getPreciseAllocated(client), 0);
    operator delete(p, sizeof(char));
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test new[] & delete[] //////////////////////////////////////////////
    p = new char[100];
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), 100);
    delete []p;
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test sized delete[] ////////////////////////////////////////////////
    p = new char[100];
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), 100);
    operator delete[](p, sizeof(char) * 100);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test nothrow new, with normal delete
    p = new (std::nothrow) char;
    EXPECT_GT(cb::ArenaMalloc::getPreciseAllocated(client), 0);
    delete (p);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test nothrow new[], with normal delete[]
    p = new (std::nothrow) char[100];
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), 100);
    delete[] p;
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test new, with nothrow delete
    p = new char();
    EXPECT_GT(cb::ArenaMalloc::getPreciseAllocated(client), 0);
    operator delete(p, std::nothrow);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test new[], with nothrow delete[]
    p = new char[100];
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), 100);
    operator delete[](p, std::nothrow);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test cb_malloc() / cb_free() /////////////////////////////////////////////
    p = static_cast<char*>(cb_malloc(sizeof(char) * 10));
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), 10);
    cb_free(p);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test cb_realloc() /////////////////////////////////////////////////////
    p = static_cast<char*>(cb_malloc(1));
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), 1);

    // Allocator may round up allocation sizes; so it's hard to
    // accurately predict how much cb::ArenaMalloc::getPreciseAllocated(client)
    // will increase. Hence we just increase by a "large" amount and check at
    // least half that increment.
    size_t prev_size = cb::ArenaMalloc::getPreciseAllocated(client);
    p = static_cast<char*>(cb_realloc(p, sizeof(char) * 100));
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), (prev_size + 50));

    prev_size = cb::ArenaMalloc::getPreciseAllocated(client);
    p = static_cast<char*>(cb_realloc(p, 1));
    EXPECT_LT(cb::ArenaMalloc::getPreciseAllocated(client), prev_size);

    prev_size = cb::ArenaMalloc::getPreciseAllocated(client);
    char* q = static_cast<char*>(cb_realloc(NULL, 10));
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), prev_size + 10);

    cb_free(p);
    cb_free(q);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test cb_calloc() //////////////////////////////////////////////////////
    p = static_cast<char*>(cb_calloc(sizeof(char), 20));
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client), 20);
    cb_free(p);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));

    // Test indirect use of malloc() via cb_strdup() /////////////////////////
    p = cb_strdup("random string");
    EXPECT_GE(cb::ArenaMalloc::getPreciseAllocated(client),
              sizeof("random string"));
    cb_free(p);
    EXPECT_EQ(0, cb::ArenaMalloc::getPreciseAllocated(client));
}

// Test that the various memory allocation / deletion functions are correctly
// accounted for, when run in a parallel thread.
TEST_F(MemoryTrackerTest, Accounting) {
    thread = std::thread(&MemoryTrackerTest::AccountingTestThread, this);
    thread.join();
}

/* Test that malloc_usable_size is correctly interposed when using a
 * non-system allocator, as otherwise, our global new replacement could
 * lead to memory being allocated with jemalloc, but the system
 * malloc_usable_size being called with it.
 * We compare the result of malloc_usable_size to the result of
 * AllocHooks::get_allocation_size, which under jemalloc
 * maps to je_malloc_usable_size.
 * If these differ, or this test segfaults, it is suspicious and
 * worth investigating.
 * NB: ASAN is not helpful here as it does not work well with jemalloc
 */
#if defined(HAVE_MALLOC_USABLE_SIZE)
TEST_F(MemoryTrackerTest, DISABLED_mallocUsableSize) {
    // Allocate some data
    auto* ptr = new char[1];

    size_t allocHooksResult = AllocHooks::get_allocation_size(ptr);
    size_t directCallResult = malloc_usable_size(ptr);

    EXPECT_EQ(allocHooksResult, directCallResult);

    delete[] ptr;
}
#endif

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    AllocHooks::initialize();

    return RUN_ALL_TESTS();
}
