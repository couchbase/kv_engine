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
#include <cstring>
#if defined(HAVE_MEMALIGN)
#include <malloc.h>
#endif

#include "daemon/alloc_hooks.h"

std::atomic_size_t alloc_size;

// Test pointer in global scope to prevent compiler optimizing malloc/free away
// via DCE.
char* p;

extern "C" {
    static void NewHook(const void* ptr, size_t) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size += AllocHooks::get_allocation_size(p);
        }
    }

    static void DeleteHook(const void* ptr) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size -= AllocHooks::get_allocation_size(p);
        }
    }

    static void TestThread(void* arg) {
        alloc_size = 0;

        // Test new & delete //////////////////////////////////////////////////
        p = new char();
        cb_assert(alloc_size > 0);
        delete p;
        cb_assert(alloc_size == 0);

        // Test new[] & delete[] //////////////////////////////////////////////
        p = new char[100];
        cb_assert(alloc_size >= 100);
        delete []p;
        cb_assert(alloc_size == 0);

        // Test malloc() / free() /////////////////////////////////////////////
        p = static_cast<char*>(malloc(sizeof(char) * 10));
        cb_assert(alloc_size >= 10);
        free(p);
        cb_assert(alloc_size == 0);

        // Test realloc() /////////////////////////////////////////////////////
        p = static_cast<char*>(malloc(1));
        cb_assert(alloc_size >= 1);

        // Allocator may round up allocation sizes; so it's hard to
        // accurately predict how much alloc_size will increase. Hence
        // we just increase by a "large" amount and check at least half that
        // increment.
        size_t prev_size = alloc_size;
        p = static_cast<char*>(realloc(p, sizeof(char) * 100));
        cb_assert(alloc_size >= (prev_size + 50));

        prev_size = alloc_size;
        p = static_cast<char*>(realloc(p, 1));
        cb_assert(alloc_size < prev_size);

        prev_size = alloc_size;
        char* q = static_cast<char*>(realloc(NULL, 10));
        cb_assert(alloc_size >= prev_size + 10);

        free(p);
        free(q);
        cb_assert(alloc_size == 0);

        // Test calloc() //////////////////////////////////////////////////////
        p = static_cast<char*>(calloc(sizeof(char), 20));
        cb_assert(alloc_size >= 20);
        free(p);
        cb_assert(alloc_size == 0);

        // Test indirect use of malloc() via strdup() /////////////////////////
        p = strdup("random string");
        cb_assert(alloc_size >= sizeof("random string"));
        free(p);
        cb_assert(alloc_size == 0);

#if defined(HAVE_MEMALIGN)
        // Test memalign //////////////////////////////////////////////////////
        p = static_cast<char*>(memalign(16, 64));
        cb_assert(alloc_size >= 64);
        free(p);
        cb_assert(alloc_size == 0);

        // Test posix_memalign ////////////////////////////////////////////////
        void* ptr;
        cb_assert(posix_memalign(&ptr, 16, 64) == 0);
        cb_assert(alloc_size >= 64);
        free(ptr);
        cb_assert(alloc_size == 0);
#endif
    }
}

int main(void) {
    AllocHooks::initialize();

   AllocHooks::add_new_hook(NewHook);
   AllocHooks::add_delete_hook(DeleteHook);

   cb_thread_t tid;
   cb_assert(cb_create_thread(&tid, TestThread, 0, 0) == 0);
   cb_assert(cb_join_thread(tid) == 0);

   AllocHooks::remove_new_hook(NewHook);
   AllocHooks::remove_delete_hook(DeleteHook);

   return 0;
}
