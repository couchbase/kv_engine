#include "atomic.hh"
#include "locks.hh"
#include <pthread.h>
#include "assert.h"
#define NUM_THREADS 90
#define NUM_ITEMS 1000000

struct thread_args {
    SyncObject mutex;
    SyncObject gate;
    AtomicQueue<int> queue;
    int counter;
};

void *launch_consumer_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    int count;
    std::queue<int> outQueue;
    LockHolder lh(args->mutex);
    args->gate.acquire();
    args->counter++;
    args->gate.release();
    args->gate.notify();
    args->mutex.wait();
    lh.unlock();

    while(count < NUM_THREADS * NUM_ITEMS) {
        int size = args->queue.size();
        if (size != 0) {
            std::cerr << "Popping " << size << " items from queue" << std::endl;
        }
        args->queue.getAll(outQueue);
        while (!outQueue.empty()) {
            count++;
            outQueue.pop();
        }
    }
    sleep(1);
    assert(args->queue.empty());
    assert(outQueue.empty());
    return static_cast<void *>(0);
}

void *launch_test_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    int i(0);
    LockHolder lh(args->mutex);
    args->gate.acquire();
    args->counter++;
    args->gate.release();
    args->gate.notify();
    args->mutex.wait();
    lh.unlock();
    for (i = 0; i < NUM_ITEMS; ++i) {
        args->queue.push(i);
    }

    return static_cast<void *>(0);
}

int main() {
    pthread_t threads[NUM_THREADS];
    pthread_t consumer;
    int i(0), rc(0), result(-1);
    struct thread_args args;

    args.counter = 0;

    rc = pthread_create(&consumer, NULL, launch_consumer_thread, &args);
    assert(rc == 0);

    std::cerr << "Started consumer thread" << std::endl;

    for (i = 0; i < NUM_THREADS; ++i) {
        rc = pthread_create(&threads[i], NULL, launch_test_thread, &args);
        std::cerr << "Starting producer thread " << i << std::endl;
        assert(rc == 0);
    }

    // Wait for all threads to reach the starting gate
    int counter;
    while (true) {
        LockHolder lh(args.gate);
        counter = args.counter;
        std::cerr << counter << " threads waiting" << std::endl;
        if (counter == NUM_THREADS + 1) {
            break;
        }
        args.gate.wait();
    }
    std::cerr << "GO!" << std::endl;
    args.mutex.notify();

    for (i = 0; i < NUM_THREADS; ++i) {
        std::cerr << "Waiting for producer thread " << i << std::endl;
        rc = pthread_join(threads[i], reinterpret_cast<void **>(&result));
        assert(rc == 0);
        assert(result == 0);
    }

    std::cerr << "Waiting for consumer thread" << std::endl;
    rc = pthread_join(consumer, reinterpret_cast<void **>(&result));
    assert(rc == 0);
    assert(result == 0);
    assert(args.queue.empty());
}
