/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vb_ready_queue.h"

#include <benchmark/benchmark.h>
#include <folly/Random.h>
#include <folly/portability/GTest.h>
#include <platform/syncobject.h>
#include <platform/sysinfo.h>
#include <vbucket.h>

#include <future>

class VBReadyQueueBench : public benchmark::Fixture {
public:
    // Consumer side benchmark functions are public as we run them in
    // non-GoogleBenchmark threads.

    /**
     * Consumer side for our MPSC VBReadyQueue benchmark.
     */
    void runConsumer(benchmark::State& state) {
        double popTime = 0;

        // Run until we are told to stop. Acquire memory order to ensure that we
        // don't throw due to pop returning false (i.e. queue is empty)
        while (!shouldConsumerStop()) {
            Vbid vbid;

            auto start = std::chrono::steady_clock::now();
            auto popped = queue.popFront(vbid);
            auto end = std::chrono::steady_clock::now();

            if (popped) {
                popTime += std::chrono::duration<double>(end - start).count();
            } else {
                // Sleep until we are notified.
                std::unique_lock<std::mutex> lh(syncObject);
                syncObject.wait(lh, [this]() {
                    // Deal with any wakeup. First check finished, we
                    // must not go back to sleep if we are finished.
                    if (shouldConsumerStop()) {
                        // Wake
                        return true;
                    }

                    // Second, check queue size (to handle any spurious
                    // wakeup).
                    return !queue.empty();
                });
            }

            // If we were asleep, finished may have changed so check again
            if (shouldConsumerStop()) {
                break;
            }
        }

        state.counters["PopTime"] = popTime / state.iterations();
    }

    /**
     * Consumer side for our single element SPSC sanity test. Requires some
     * specific thread gating for sanity checks so separate from the other
     * runConsumer functions for reduced complexity.
     */
    void runConsumerSanityOneElement(benchmark::State& state) {
        double popTime = 0;

        // Run until we are told to stop. Uses the queueSize instead of finished
        // because we don't need both.
        while (queueSize.load(std::memory_order_acquire) !=
               std::numeric_limits<size_t>::max()) {
            {
                // Sleep until we are notified.
                std::unique_lock<std::mutex> lh(syncObject);
                // Nothing in the queue, now we wait. We have to check this
                // under lock before sleeping to prevent us from racing with the
                // producer and getting both threads stuck waiting.
                if (queueSize.load(std::memory_order_acquire) == 0) {
                    syncObject.wait(lh, [this]() {
                        // In sanity mode we have to track this separately
                        // because queue.Empty() (the condition we check in
                        // normal mode) will return a sequentially consistent
                        // value for our lock free VBReadyQueue which adds
                        // additional thread gating that would prevent this
                        // sanity check from encountering memory ordering issues
                        return queueSize.load(std::memory_order_acquire) != 0;
                    });
                }
            }

            // If we were asleep, we may have finished so check again and break
            // if done
            if (shouldConsumerStop()) {
                break;
            }

            Vbid vbid;

            auto start = std::chrono::steady_clock::now();
            auto popped = queue.popFront(vbid);
            auto end = std::chrono::steady_clock::now();

            // Running in sanity mode - notify the condVar to run the producer
            // again
            queueSize.fetch_sub(1, std::memory_order_acq_rel);
            syncObject.notify_one();

            EXPECT_TRUE(popped);
            popTime += std::chrono::duration<double>(end - start).count();
        }

        state.counters["PopTime"] = popTime / state.iterations();
    }

    /**
     * Consumer side for our SPSC sanity test that runs on the entire range of
     * Vbids. The producer should attempt to enqueue values 0-1023 and the
     * consumer should dequeue them concurrently. Returns a value via promise to
     * the thread creator so that we can compare pushed and popped stats.
     */
    void runConsumerSanityManyElements(benchmark::State& state,
                                       std::promise<size_t>&& p) {
        double popTime = 0;
        size_t poppedCount = 0;

        {
            // Sleep until we are notified.
            std::unique_lock<std::mutex> lh(syncObject);
            syncObject.wait(lh, [this]() {
                // In sanity mode we have to track this separately
                // because queue.Empty() (the condition we check in
                // normal mode) will return a sequentially consistent
                // value for our lock free VBReadyQueue which adds
                // additional thread gating that would prevent this
                // sanity check from encountering memory ordering issues
                return queueSize.load(std::memory_order_acquire) != 0;
            });
        }
        auto size = queueSize.load(std::memory_order_acquire);
        EXPECT_LT(0, size);
        EXPECT_NE(std::numeric_limits<size_t>::max(), size);

        Vbid vbid;
        while (true) {
            auto start = std::chrono::steady_clock::now();
            auto popped = queue.popFront(vbid);
            auto end = std::chrono::steady_clock::now();

            if (popped) {
                queueSize.fetch_sub(1, std::memory_order_acq_rel);

                ++poppedCount;
                popTime += std::chrono::duration<double>(end - start).count();

                // We've popped everything. Notify the producer to loop around
                // again (if it has not been told to stop)
                if (vbid == Vbid(1023)) {
                    // Ideally I would now check that queueSize is 0, but it may
                    // be -1 as we may not yet have incremented queueSize in the
                    // producer.
                    EXPECT_LE(0, queueSize.load(std::memory_order_acquire));
                    std::unique_lock<std::mutex> lh(syncObject);
                    syncObject.notify_one();
                }
            } else {
                {
                    // Sleep until we are notified.
                    std::unique_lock<std::mutex> lh(syncObject);
                    syncObject.wait(lh, [this]() {
                        return queueSize.load(std::memory_order_acquire) != 0;
                    });

                    // If we were asleep, we may have finished so check again
                    if (shouldConsumerStop()) {
                        break;
                    }
                }
            }
        }

        state.counters["PopTime"] = popTime / state.iterations();

        // Set our return value (via promise)
        p.set_value(poppedCount);
    }

protected:
    /**
     * Producer side for our MPSC benchmark. Tries to push something into the
     * readyQ regardless of whether or not it existed in there before. This is
     * analagous to us attempting to put a VB into the ready queue of a producer
     * when we notify a new seqno.
     */
    void runProducer(benchmark::State& state, std::function<Vbid()> indexFn) {
        double pushTime = 0;
        while (state.KeepRunning()) {
            auto index = indexFn();

            auto start = std::chrono::steady_clock::now();
            queue.pushUnique(index);
            auto end = std::chrono::steady_clock::now();

            syncObject.notify_one();

            pushTime += std::chrono::duration<double>(end - start).count();
        }

        state.counters["PushTime"] = pushTime / state.iterations();
    }

    /**
     * Producer side for our single element SPSC sanity test. Requires some
     * specific thread gating for sanity checks so separate from the other
     * runProducer function for reduced complexity.
     */
    void runProducerSanityOneElement(benchmark::State& state,
                                     std::function<Vbid()> indexFn) {
        double pushTime = 0;
        while (state.KeepRunning()) {
            auto index = indexFn();

            auto start = std::chrono::steady_clock::now();
            auto pushed = queue.pushUnique(index);
            auto end = std::chrono::steady_clock::now();

            EXPECT_TRUE(pushed);

            // Wait for the consumer to tell us to run again.
            std::unique_lock<std::mutex> lh(syncObject);

            // Bump the queueSize under lock (required to ensure that we don't
            // get the consumer stuck
            queueSize.fetch_add(1, std::memory_order_acq_rel);
            syncObject.notify_one();
            syncObject.wait(lh, [this, &state]() {
                return queueSize.load(std::memory_order_acquire) == 0;
            });

            pushTime += std::chrono::duration<double>(end - start).count();
        }

        state.counters["PushTime"] = pushTime / state.iterations();
    }

    /**
     * Producer side for our SPSC sanity test that operates over the entire
     * range of Vbids.
     */
    size_t runProducerSanityManyElements(benchmark::State& state) {
        double pushTime = 0;
        size_t pushedCount = 0;

        while (state.KeepRunning()) {
            // Push each possible Vbid to the queue
            Vbid vbid;
            for (auto i = 0; i < 1024; i++) {
                vbid = Vbid(i);
                auto start = std::chrono::steady_clock::now();
                auto pushed = queue.pushUnique(vbid);
                auto end = std::chrono::steady_clock::now();

                ++pushedCount;
                queueSize.fetch_add(1, std::memory_order_acq_rel);

                if (pushed) {
                    // Notify the consumer, it may have already processed the
                    // item if it ran before we notified it, but we don't really
                    // care as it will just go back to sleep after attempting to
                    // pop something
                    std::unique_lock<std::mutex> lh(syncObject);
                    syncObject.notify_one();
                }

                pushTime += std::chrono::duration<double>(end - start).count();

                if (i == 1023) {
                    // We've pushed everything (last loop iteration).
                    std::unique_lock<std::mutex> lh(syncObject);

                    if (queueSize.load(std::memory_order_acquire) == 0) {
                        // Empty so break and run again. This may be the case
                        // if the consumer ran after we pushed but before we
                        // grabbed the lock on the syncObject.
                        EXPECT_TRUE(queue.empty());
                        break;
                    }

                    // Go to sleep until the consumer has drained the queue.
                    syncObject.wait(lh, [this, &state]() {
                        return queueSize.load(std::memory_order_acquire) == 0;
                    });
                    EXPECT_TRUE(queue.empty());
                    EXPECT_EQ(0, queueSize.load(std::memory_order_acquire));
                }
            }
        }

        state.counters["PushTime"] = pushTime / state.iterations();
        return pushedCount;
    }

    void stopConsumer() {
        // Set the queueSize to 1025 to signal the consumer to stop. We need to
        // set the queueSize to a non-zero value or the single element sanity
        // test can get stuck.
        queueSize.store(1025, std::memory_order_release);
    }

    bool shouldConsumerStop() {
        // We use value 1025 on queueSize to indicate that the consumer should
        // stop.
        return queueSize.load(std::memory_order_acquire) == 1025;
    }

    /**
     * Thread in which we run the Consumer. Needs to be a separate thread
     * or the use of condition variables in the GoogleBenchmark threads means we
     * will end up deadlocked if we attempt the level of synchronization
     * required in these tests.
     */
    std::thread consumerThread;

    /**
     * Cond var and mutex to only pop when something exists in the queue.
     */
    SyncObject syncObject;

    /**
     * QueueSize atomic which is required in our Sanity test to provide the
     * level of orchestration required to make our sanity checks without
     * using the queue's size function which is sequentially consistent which
     * would prevent us from encountering any memory ordering issues.
     */
    std::atomic<size_t> queueSize;

    /**
     * Our queue to bench
     */
    VBReadyQueue queue{1024};
};

// Uncontended pushing test when element did not previously exist
BENCHMARK_DEFINE_F(VBReadyQueueBench, PushEmpty)(benchmark::State& state) {
    while (state.KeepRunning()) {
        /**
         * Manual timing for this benchmark as timing will be inaccurate if we
         * pause and resume for the bits we care about.
         */
        auto start = std::chrono::steady_clock::now();
        queue.pushUnique(Vbid(0));
        auto end = std::chrono::steady_clock::now();
        state.SetIterationTime(
                std::chrono::duration<double>(end - start).count());

        Vbid vbid;
        queue.popFront(vbid);
    }
}

// Uncontended pushing test when element did previously exist
BENCHMARK_DEFINE_F(VBReadyQueueBench, PushNotEmpty)(benchmark::State& state) {
    while (state.KeepRunning()) {
        /**
         * Manual timing for this benchmark as timing will be inaccurate if we
         * pause and resume for the bits we care about.
         */
        queue.pushUnique(Vbid(0));
        auto start = std::chrono::steady_clock::now();
        queue.pushUnique(Vbid(0));
        auto end = std::chrono::steady_clock::now();
        state.SetIterationTime(
                std::chrono::duration<double>(end - start).count());

        Vbid vbid;
        queue.popFront(vbid);
    }
}

// Uncontended popping test
BENCHMARK_DEFINE_F(VBReadyQueueBench, PopFront)(benchmark::State& state) {
    queue.pushUnique(Vbid(0));

    while (state.KeepRunning()) {
        /**
         * Manual timing for this benchmark as timing will be inaccurate if we
         * pause and resume for the bits we care about.
         */
        auto start = std::chrono::steady_clock::now();
        Vbid vbid;
        queue.popFront(vbid);
        auto end = std::chrono::steady_clock::now();
        state.SetIterationTime(
                std::chrono::duration<double>(end - start).count());

        queue.pushUnique(Vbid(0));
    }
}

// Push an entire set of Vbids and pop them all. Tests sanity of queue size and
// set.
BENCHMARK_DEFINE_F(VBReadyQueueBench, PopAllSanity)(benchmark::State& state) {
    for (int i = 0; i < 1024; i++) {
        queue.pushUnique(Vbid(i));
    }

    while (state.KeepRunning()) {
        /**
         * Manual timing for this benchmark as timing will be inaccurate if we
         * pause and resume for the bits we care about.
         */
        auto start = std::chrono::steady_clock::now();
        Vbid vbid;
        for (int i = 0; i < 1024; i++) {
            auto popped = queue.popFront(vbid);
            EXPECT_TRUE(popped);
        }
        auto end = std::chrono::steady_clock::now();
        state.SetIterationTime(
                std::chrono::duration<double>(end - start).count());

        EXPECT_TRUE(queue.empty());
        for (int i = 0; i < 1024; i++) {
            EXPECT_FALSE(queue.exists(Vbid(i)));
        }

        while (queue.popFront(vbid)) {
        }

        for (int i = 0; i < 1024; i++) {
            queue.pushUnique(Vbid(i));
        }
    }
}

// Our general use case is as a unique MPSC queue. We measure our times manually
// here because we have two metrics that we care about (Push and Pop time). The
// goal of this benchmark is to measure push and pop time when heavily
// contended. Pushes are not guaranteed to be unique
BENCHMARK_DEFINE_F(VBReadyQueueBench, MPSCRandom)(benchmark::State& state) {
    if (state.thread_index() == 0) {
        // Lets run our consumer in a separate (not GoogleBenchmark thread)
        // because our use of condition variables to drain the queue when it is
        // not empty just does not work with the thread gating GoogleBenchmark
        // does in state.KeepRunning(). Lambda because std::thread doesn't play
        // nice with member functions without binding and this is simpler.
        consumerThread = std::thread([this, &state]() { runConsumer(state); });
    }

    runProducer(state,
                [&state]() { return Vbid(folly::Random::rand32() % 1024); });

    if (state.thread_index() == 0) {
        stopConsumer();
        syncObject.notify_all();
        consumerThread.join();
    }
}

// Sanity test that continually pushes when the queue is empty then pops
// immediately in a concurrent thread.
BENCHMARK_DEFINE_F(VBReadyQueueBench, SanityOneElement)
(benchmark::State& state) {
    // Lets run our consumer in a separate (not GoogleBenchmark thread)
    // because our use of condition variables to drain the queue when it is
    // not empty just does not work with the thread gating GoogleBenchmark
    // does in state.KeepRunning(). Lambda because std::thread doesn't play
    // nice with member functions without binding and this is simpler.
    consumerThread = std::thread(
            [this, &state]() { runConsumerSanityOneElement(state); });

    runProducerSanityOneElement(
            state, [&state]() { return Vbid(folly::Random::rand32() % 1024); });

    // Signal the consumer thread to stop
    stopConsumer();

    {
        // Notify the thread in case it was asleep
        std::unique_lock<std::mutex> lh(syncObject);
        syncObject.notify_all();
    }

    consumerThread.join();

    EXPECT_GT(2, queue.size());
}

// Sanity test that continually pushes when the queue is empty then pops
// immediately in a concurrent thread.
BENCHMARK_DEFINE_F(VBReadyQueueBench, SanityManyElements)
(benchmark::State& state) {
    // Lets run our consumer in a separate (not GoogleBenchmark thread)
    // because our use of condition variables to drain the queue when it is
    // not empty just does not work with the thread gating GoogleBenchmark
    // does in state.KeepRunning(). Lambda and returning value via promise
    // because std::thread doesn't play nice with member functions without
    // binding and this is simpler.
    std::promise<size_t> promise;
    auto future = promise.get_future();
    consumerThread = std::thread([this, &state, &promise]() {
        runConsumerSanityManyElements(state, std::move(promise));
    });

    // Run the producer in this thread
    auto pushedCount = runProducerSanityManyElements(state);

    // Signal the consumer thread to stop
    stopConsumer();

    {
        // Notify the thread in case it was asleep
        std::unique_lock<std::mutex> lh(syncObject);
        syncObject.notify_all();
    }

    consumerThread.join();

    auto poppedCount = future.get();
    EXPECT_EQ(pushedCount, poppedCount);
    EXPECT_TRUE(queue.empty());
}

BENCHMARK_REGISTER_F(VBReadyQueueBench, PushEmpty);
BENCHMARK_REGISTER_F(VBReadyQueueBench, PushNotEmpty);
BENCHMARK_REGISTER_F(VBReadyQueueBench, PopFront);
BENCHMARK_REGISTER_F(VBReadyQueueBench, PopAllSanity);

// Iterations is kinda arbitrary but stops this set of benchmarks from running
// for over 1 minute on my 2017 Macbook Pro.
BENCHMARK_REGISTER_F(VBReadyQueueBench, SanityOneElement)
        ->Threads(1)
        ->Iterations(1000000);

BENCHMARK_REGISTER_F(VBReadyQueueBench, SanityManyElements)
        ->Threads(1)
        ->Iterations(5000);

BENCHMARK_REGISTER_F(VBReadyQueueBench, MPSCRandom)
        ->Threads(cb::get_cpu_count())
        ->Iterations(1000000);
BENCHMARK_REGISTER_F(VBReadyQueueBench, MPSCRandom)
        ->Threads(cb::get_cpu_count() * 2)
        ->Iterations(1000000);
BENCHMARK_REGISTER_F(VBReadyQueueBench, MPSCRandom)
        ->Threads(cb::get_cpu_count() * 4)
        ->Iterations(1000000);
BENCHMARK_REGISTER_F(VBReadyQueueBench, MPSCRandom)
        ->Threads(cb::get_cpu_count() * 8)
        ->Iterations(1000000);
