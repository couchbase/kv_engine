/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#ifndef SRC_ATOMICQUEUE_H_
#define SRC_ATOMICQUEUE_H_ 1

#include "atomic.h"

#ifdef _MSC_VER

#include <queue>
#include <thread>
#include <mutex>

/**
 * Create a simple version of the AtomicQueue for windows right now to
 * avoid the threadlocal usage which is currently using pthreads
 */
template <typename T>
class AtomicQueue {
public:
    void push(T &value) {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push(value);
    }

    void getAll(std::queue<T> &outQueue) {
        std::lock_guard<std::mutex> lock(mutex);
        while (!queue.empty()) {
            outQueue.push(queue.front());
            queue.pop();
        }
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(mutex);
        return queue.empty();
    }

    /**
     * Return the number of queued items.
     */
    size_t size() {
        std::lock_guard<std::mutex> lock(mutex);
        return queue.size();
    }
private:
    std::queue<T> queue;
    std::mutex mutex;
};

#else

#include "threadlocal.h"

template <typename T>
class CouchbaseAtomicPtr : public CouchbaseAtomic<T*> {
public:
    CouchbaseAtomicPtr(T *initial = NULL) : CouchbaseAtomic<T*>(initial) {}

    ~CouchbaseAtomicPtr() {}

    T *operator ->() {
        return CouchbaseAtomic<T*>::load();
    }

    T &operator *() {
        return *CouchbaseAtomic<T*>::load();
    }

    operator bool() const {
        return CouchbaseAtomic<T*>::load() != NULL;
    }

    bool operator !() const {
        return CouchbaseAtomic<T*>::load() == NULL;
    }
};



/**
 * Efficient approximate-FIFO queue optimize for concurrent writers.
 */
template <typename T>
class AtomicQueue {
public:
    AtomicQueue() : counter(0), numItems(0) {}

    ~AtomicQueue() {
        size_t i;
        for (i = 0; i < counter; ++i) {
            delete queues[i];
        }
    }

    /**
     * Place an item in the queue.
     */
    void push(T &value) {
        std::queue<T> *q = swapQueue(); // steal our queue
        q->push(value);
        ++numItems;
        q = swapQueue(q);
    }

    /**
     * Grab all items from this queue an place them into the provided
     * output queue.
     *
     * @param outQueue a destination queue to fill
     */
    void getAll(std::queue<T> &outQueue) {
        std::queue<T> *q(swapQueue()); // Grab my own queue
        std::queue<T> *newQueue(NULL);
        int count(0);

        // Will start empty unless this thread is adding stuff
        while (!q->empty()) {
            outQueue.push(q->front());
            q->pop();
            ++count;
        }

        size_t c(counter);
        for (size_t i = 0; i < c; ++i) {
            // Swap with another thread
            newQueue = queues[i].swapIfNot(NULL, q);
            // Empty the queue
            if (newQueue != NULL) {
                q = newQueue;
                while (!q->empty()) {
                    outQueue.push(q->front());
                    q->pop();
                    ++count;
                }
            }
        }

        q = swapQueue(q);
        numItems.fetch_sub(count);
    }

    /**
     * True if this queue is empty.
     */
    bool empty() const {
        return size() == 0;
    }

    /**
     * Return the number of queued items.
     */
    size_t size() const {
        return numItems;
    }
private:
    CouchbaseAtomicPtr<std::queue<T> > *initialize() {
        std::queue<T> *q = new std::queue<T>;
        size_t i(counter++);
        cb_assert(counter <= MAX_THREADS);
        queues[i] = q;
        threadQueue = &queues[i];
        return &queues[i];
    }

    std::queue<T> *swapQueue(std::queue<T> *newQueue = NULL) {
        CouchbaseAtomicPtr<std::queue<T> > *qPtr(threadQueue);
        if (qPtr == NULL) {
            qPtr = initialize();
        }
        return qPtr->exchange(newQueue);
    }

    ThreadLocalPtr<CouchbaseAtomicPtr<std::queue<T> > > threadQueue;
    CouchbaseAtomicPtr<std::queue<T> > queues[MAX_THREADS];
    AtomicValue<size_t> counter;
    AtomicValue<size_t> numItems;
    DISALLOW_COPY_AND_ASSIGN(AtomicQueue);
};
#endif


#endif  // SRC_ATOMICQUEUE_H_
