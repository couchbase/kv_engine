/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_MEMORY_TRACKER_H_
#define SRC_MEMORY_TRACKER_H_ 1

#include "config.h"

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>

#include <memcached/allocator_hooks.h>

#include "atomic.h"

/**
 * This class is used by ep-engine to hook into memcached's memory tracking
 * capabilities.
 */
class MemoryTracker {
public:
    ~MemoryTracker();

    /* Creates the singleton instance of the MemoryTracker (if it doesn't exist).
     * Thread-safe, so ok for multiple threads to attempt to create at the
     * same time.
     * @return The MemoryTracker singleton.
     */
    static MemoryTracker* getInstance(const ALLOCATOR_HOOKS_API& hook_api_);

    static void destroyInstance();

    void getAllocatorStats(std::map<std::string, size_t> &alloc_stats);

    static bool trackingMemoryAllocations();

    void updateStats();

    void getDetailedStats(char* buffer, int size);

    size_t getFragmentation();

    size_t getTotalBytesAllocated();

    size_t getTotalHeapBytes();

private:
    MemoryTracker(const ALLOCATOR_HOOKS_API& hooks_api_);

    // Helper function for construction - connects the tracker
    // to the memory allocator via alloc_hooks.
    void connectHooks();

    // Function for the stats updater main loop.
    static void statsThreadMainLoop(void* arg);

    static void NewHook(const void* ptr, size_t);
    static void DeleteHook(const void* ptr);

    // Wheter or not we have the ability to accurately track memory allocations
    static std::atomic<bool> tracking;
    // Singleton memory tracker and mutex guarding it's creation.
    static std::atomic<MemoryTracker*> instance;
    static std::mutex instance_mutex;

    cb_thread_t statsThreadId;
    allocator_stats stats;

    // Mutex guarding the shutdown condvar.
    std::mutex mutex;
    // Condition variable used to signal shutdown to the stats thread.
    std::condition_variable shutdown_cv;

    // Memory allocator hooks API to use (needed by New / Delete hook
    // functions)
    ALLOCATOR_HOOKS_API hooks_api;
};

#endif  // SRC_MEMORY_TRACKER_H_
