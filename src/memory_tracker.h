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

    static MemoryTracker* getInstance();
    static void destroyInstance();

    void getAllocatorStats(std::map<std::string, size_t> &alloc_stats);

    static bool trackingMemoryAllocations();

    void updateStats();

    void getDetailedStats(char* buffer, int size);

    size_t getFragmentation();

    size_t getTotalBytesAllocated();

    size_t getTotalHeapBytes();

private:
    MemoryTracker();

    // Function for the stats updater main loop.
    static void statsThreadMainLoop(void* arg);

    // Wheter or not we have the ability to accurately track memory allocations
    static bool tracking;
    // Singleton memory tracker and mutex guarding it's creation.
    static std::atomic<MemoryTracker*> instance;
    static std::mutex instance_mutex;

    cb_thread_t statsThreadId;
    allocator_stats stats;

    // Mutex guarding the shutdown condvar.
    std::mutex mutex;
    // Condition variable used to signal shutdown to the stats thread.
    std::condition_variable shutdown_cv;
};

#endif  // SRC_MEMORY_TRACKER_H_
