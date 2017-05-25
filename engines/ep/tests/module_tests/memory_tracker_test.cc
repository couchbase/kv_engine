#include "config.h"

#include "memory_tracker.h"
#include "mock_hooks_api.h"

#include <thread>

#include <gtest/gtest.h>


// Test that creating the singleton MemoryTracker is thread-safe (MB-18940)
TEST(MemoryTracker, SingletonIsThreadSafe_MB18940) {
    // Call getInstance in two different threads in parallel, should only
    // have one MemoryTracker created.
    // Note: Relies on ThreadSanitizer or similar to detect data race (or
    // getting lucky that you fail on first attempt). Consider running
    // multiple times (--gtest_repeat=XXX) to expose issue more easily.

    MemoryTracker* instance1;
    MemoryTracker* instance2;

    auto alloc_hooks = *getHooksApi();

    // Lambda which returns (via reference param) the result of getInstance().
    auto get_instance = [alloc_hooks](MemoryTracker*& t) {
        t = MemoryTracker::getInstance(alloc_hooks);
    };

    // Spin up two threads to both call getInstance (and hence attempt to
    // create the singleton).
    std::thread t1{get_instance, std::ref(instance1)};
    std::thread t2{get_instance, std::ref(instance2)};

    t1.join();
    t2.join();

    MemoryTracker::destroyInstance();

    EXPECT_EQ(instance1, instance2);
}
