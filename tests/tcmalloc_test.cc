/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
//
// This small test program demonstrate a crash on Windows in cb_create_thread
// (Inside CreateThread)
//
#include "config.h"
#include <cassert>

#include "daemon/alloc_hooks.h"

size_t alloc_size;

extern "C" {
    static void NewHook(const void* ptr, size_t) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size = mc_get_allocation_size(p);
        }
    }

    static void DeleteHook(const void* ptr) {
        if (ptr != NULL) {
            void* p = const_cast<void*>(ptr);
            alloc_size = mc_get_allocation_size(p);
        }
    }

    static void TestThread(void* arg) {
        alloc_size = 0;

        char *p = new char[100];
        assert(alloc_size > 0);
        size_t allocated = alloc_size;

        alloc_size = 0;
        delete []p;

        assert(alloc_size == allocated);
    }
}

int main(void) {
   init_alloc_hooks();

   mc_add_new_hook(NewHook);
   mc_add_delete_hook(DeleteHook);

   cb_thread_t tid;
   assert(cb_create_thread(&tid, TestThread, 0, 0) == 0);
   assert(cb_join_thread(tid) == 0);

   mc_remove_new_hook(NewHook);
   mc_remove_delete_hook(DeleteHook);

   return 0;
}
