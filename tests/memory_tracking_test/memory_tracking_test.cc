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
#include <platform/cb_malloc.h>
#include <string>

#include "daemon/alloc_hooks.h"

std::atomic_size_t alloc_size;

// Test pointer in global scope to prevent compiler optimizing malloc/free away
// via DCE.
char* p;

// Helper function to lookup a symbol in a plugin and return a correctly-typed
// function pointer.
template <typename T>
T get_plugin_symbol(cb_dlhandle_t handle, const char* symbol_name) {
    char* errmsg;
    T symbol = reinterpret_cast<T>(cb_dlsym(handle, symbol_name, &errmsg));
    cb_assert(symbol != nullptr);
    return symbol;
}


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

        // Test cb_malloc() / cb_free() /////////////////////////////////////////////
        p = static_cast<char*>(cb_malloc(sizeof(char) * 10));
        cb_assert(alloc_size >= 10);
        cb_free(p);
        cb_assert(alloc_size == 0);

        // Test cb_realloc() /////////////////////////////////////////////////////
        p = static_cast<char*>(cb_malloc(1));
        cb_assert(alloc_size >= 1);

        // Allocator may round up allocation sizes; so it's hard to
        // accurately predict how much alloc_size will increase. Hence
        // we just increase by a "large" amount and check at least half that
        // increment.
        size_t prev_size = alloc_size;
        p = static_cast<char*>(cb_realloc(p, sizeof(char) * 100));
        cb_assert(alloc_size >= (prev_size + 50));

        prev_size = alloc_size;
        p = static_cast<char*>(cb_realloc(p, 1));
        cb_assert(alloc_size < prev_size);

        prev_size = alloc_size;
        char* q = static_cast<char*>(cb_realloc(NULL, 10));
        cb_assert(alloc_size >= prev_size + 10);

        cb_free(p);
        cb_free(q);
        cb_assert(alloc_size == 0);

        // Test cb_calloc() //////////////////////////////////////////////////////
        p = static_cast<char*>(cb_calloc(sizeof(char), 20));
        cb_assert(alloc_size >= 20);
        cb_free(p);
        cb_assert(alloc_size == 0);

        // Test indirect use of cb_malloc() via cb_strdup() /////////////////////////
        p = cb_strdup("random string");
        cb_assert(alloc_size >= sizeof("random string"));
        cb_free(p);
        cb_assert(alloc_size == 0);

        // Test memory allocations performed from another shared library loaded
        // at runtime.
        char* errmsg = nullptr;
        cb_dlhandle_t plugin = cb_dlopen("memcached_memory_tracking_plugin",
                                         &errmsg);
        cb_assert(plugin != nullptr);

        // dlopen()ing a plugin can allocate memory. Reset alloc_size.
        alloc_size = 0;

        typedef void* (*plugin_malloc_t)(size_t);
        auto plugin_malloc =
            get_plugin_symbol<plugin_malloc_t>(plugin, "plugin_malloc");
        p = static_cast<char*>(plugin_malloc(100));
        cb_assert(alloc_size >= 100);

        typedef void (*plugin_free_t)(void*);
        auto plugin_free =
            get_plugin_symbol<plugin_free_t>(plugin, "plugin_free");
        plugin_free(p);
        cb_assert(alloc_size == 0);

        typedef char* (*plugin_new_char_t)(size_t);
        auto plugin_new_char =
            get_plugin_symbol<plugin_new_char_t>(plugin, "plugin_new_char_array");
        p = plugin_new_char(200);
        cb_assert(alloc_size >= 200);

        typedef void (*plugin_delete_array_t)(char*);
        auto plugin_delete_char =
            get_plugin_symbol<plugin_delete_array_t>(plugin, "plugin_delete_array");
        plugin_delete_char(p);
        cb_assert(alloc_size == 0);

        typedef std::string* (*plugin_new_string_t)(const char*);
        auto plugin_new_string =
            get_plugin_symbol<plugin_new_string_t>(plugin, "plugin_new_string");
        auto* string = plugin_new_string("duplicate_string");
        cb_assert(alloc_size > 16);

        typedef void(*plugin_delete_string_t)(std::string* ptr);
        auto plugin_delete_string =
            get_plugin_symbol<plugin_delete_string_t>(plugin, "plugin_delete_string");
        plugin_delete_string(string);
        cb_assert(alloc_size == 0);

        cb_dlclose(plugin);
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
