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
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <platform/platform_thread.h>

#include <atomic>
#include <cstring>
#include <string>

#if defined(HAVE_MALLOC_USABLE_SIZE)
#include <malloc.h>
#endif

// Test pointer in global scope to prevent compiler optimizing malloc/free away
// via DCE.
char* p;

/// Path to plugin to be loaded via dlopen.
const char* pluginPath = nullptr;

class MemoryTrackerTest : public ::testing::Test {
public:
    // callback function for when memory is allocated.
    static void NewHook(const void* ptr, size_t) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size += AllocHooks::get_allocation_size(p);
        }
    }

    // callback function for when memory is deleted.
    static void DeleteHook(const void* ptr) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size -= AllocHooks::get_allocation_size(p);
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

    // Test sized delete //////////////////////////////////////////////////
    p = new char();
    EXPECT_GT(alloc_size, 0);
    operator delete(p, sizeof(char));
    EXPECT_EQ(0, alloc_size);

    // Test new[] & delete[] //////////////////////////////////////////////
    p = new char[100];
    EXPECT_GE(alloc_size, 100);
    delete []p;
    EXPECT_EQ(0, alloc_size);

    // Test sized delete[] ////////////////////////////////////////////////
    p = new char[100];
    EXPECT_GE(alloc_size, 100);
    operator delete[](p, sizeof(char) * 100);
    EXPECT_EQ(0, alloc_size);

    // Test nothrow new, with normal delete
    p = new (std::nothrow) char;
    EXPECT_GT(alloc_size, 0);
    delete (p);
    EXPECT_EQ(0, alloc_size);

    // Test nothrow new[], with normal delete[]
    p = new (std::nothrow) char[100];
    EXPECT_GE(alloc_size, 100);
    delete[] p;
    EXPECT_EQ(0, alloc_size);

    // Test new, with nothrow delete
    p = new char();
    EXPECT_GT(alloc_size, 0);
    operator delete(p, std::nothrow);
    EXPECT_EQ(0, alloc_size);

    // Test new[], with nothrow delete[]
    p = new char[100];
    EXPECT_GE(alloc_size, 100);
    operator delete[](p, std::nothrow);
    EXPECT_EQ(0, alloc_size);

    // Test cb_malloc() / cb_free() /////////////////////////////////////////////
    p = static_cast<char*>(cb_malloc(sizeof(char) * 10));
    EXPECT_GE(alloc_size, 10);
    cb_free(p);
    EXPECT_EQ(0, alloc_size);

    // Test cb_realloc() /////////////////////////////////////////////////////
    p = static_cast<char*>(cb_malloc(1));
    EXPECT_GE(alloc_size, 1);

    // Allocator may round up allocation sizes; so it's hard to
    // accurately predict how much alloc_size will increase. Hence
    // we just increase by a "large" amount and check at least half that
    // increment.
    size_t prev_size = alloc_size;
    p = static_cast<char*>(cb_realloc(p, sizeof(char) * 100));
    EXPECT_GE(alloc_size, (prev_size + 50));

    prev_size = alloc_size;
    p = static_cast<char*>(cb_realloc(p, 1));
    EXPECT_LT(alloc_size, prev_size);

    prev_size = alloc_size;
    char* q = static_cast<char*>(cb_realloc(NULL, 10));
    EXPECT_GE(alloc_size, prev_size + 10);

    cb_free(p);
    cb_free(q);
    EXPECT_EQ(0, alloc_size);

    // Test cb_calloc() //////////////////////////////////////////////////////
    p = static_cast<char*>(cb_calloc(sizeof(char), 20));
    EXPECT_GE(alloc_size, 20);
    cb_free(p);
    EXPECT_EQ(0, alloc_size);

    // Test indirect use of malloc() via cb_strdup() /////////////////////////
    p = cb_strdup("random string");
    EXPECT_GE(alloc_size, sizeof("random string"));
    cb_free(p);
    EXPECT_EQ(0, alloc_size);

    // Test memory allocations performed from another shared library loaded
    // at runtime.
    auto plugin = cb::io::loadLibrary(pluginPath);

    // dlopen()ing a plugin can allocate memory. Reset alloc_size.
    alloc_size = 0;

    typedef void* (*plugin_malloc_t)(size_t);
    auto plugin_malloc = plugin->find<plugin_malloc_t>("plugin_malloc");
    p = static_cast<char*>(plugin_malloc(100));
    EXPECT_GE(alloc_size, 100);

    typedef void (*plugin_free_t)(void*);
    auto plugin_free = plugin->find<plugin_free_t>("plugin_free");
    plugin_free(p);
    EXPECT_EQ(0, alloc_size);

    typedef char* (*plugin_new_char_t)(size_t);
    auto plugin_new_char =
            plugin->find<plugin_new_char_t>("plugin_new_char_array");
    p = plugin_new_char(200);
    EXPECT_GE(alloc_size, 200);

    typedef void (*plugin_delete_array_t)(char*);
    auto plugin_delete_char =
            plugin->find<plugin_delete_array_t>("plugin_delete_array");
    plugin_delete_char(p);
    EXPECT_EQ(0, alloc_size);

    typedef std::string* (*plugin_new_string_t)(const char*);
    auto plugin_new_string =
            plugin->find<plugin_new_string_t>("plugin_new_string");
    auto* string = plugin_new_string("duplicate_string");
    EXPECT_GE(alloc_size, 16);

    typedef void(*plugin_delete_string_t)(std::string* ptr);
    auto plugin_delete_string =
            plugin->find<plugin_delete_string_t>("plugin_delete_string");
    plugin_delete_string(string);
    EXPECT_EQ(0, alloc_size);
}

// Test that the various memory allocation / deletion functions are correctly
// accounted for, when run in a parallel thread.
TEST_F(MemoryTrackerTest, Accounting) {
    AllocHooks::add_new_hook(NewHook);
    AllocHooks::add_delete_hook(DeleteHook);

    cb_thread_t tid;
    ASSERT_EQ(0, cb_create_thread(&tid, AccountingTestThread, 0, 0));
    ASSERT_EQ(0, cb_join_thread(tid));

    AllocHooks::remove_new_hook(NewHook);
    AllocHooks::remove_delete_hook(DeleteHook);
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
TEST_F(MemoryTrackerTest, mallocUsableSize) {
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

    if (argc != 2) {
        std::cerr << "Usage: <memory_tracking_test> "
                     "<path_to_memory_tracking_plugin>\n";
        return 1;
    }
    pluginPath = argv[1];

    AllocHooks::initialize();

    return RUN_ALL_TESTS();
}
