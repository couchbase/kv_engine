/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * The FutureQueue provides a std::priority_queue style interface
 * onto a queue of ExTask objects that are sorted by the tasks wakeTime.
 * The lowest wakeTime (soonest) will be the top() task.
 *
 * FutureQueue provides methods that allow a task's wakeTime to be mutated
 * whilst maintaining the priority ordering.
 */

#pragma once

#include <algorithm>
#include <chrono>
#include <mutex>
#include <queue>

#include "globaltask.h"

template <class C = std::deque<ExTask>,
          class Compare = CompareByDueDate>
class FutureQueue {
public:

    void push(ExTask task) {
        std::lock_guard<std::mutex> lock(queueMutex);
        queue.push(task);
    }

    void pop() {
        std::lock_guard<std::mutex> lock(queueMutex);
        queue.pop();
    }

    ExTask top() {
        std::lock_guard<std::mutex> lock(queueMutex);
        return queue.top();
    }

    size_t size() {
        std::lock_guard<std::mutex> lock(queueMutex);
        return queue.size();
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(queueMutex);
        return queue.empty();
    }

    /*
     * Update the wakeTime of task and ensure the heap property is
     * maintained.
     * @returns true if 'task' is in the FutureQueue.
     */
    bool updateWaketime(const ExTask& task,
                        std::chrono::steady_clock::time_point newTime) {
        std::lock_guard<std::mutex> lock(queueMutex);
        task->updateWaketime(newTime);
        // After modifiying the task's wakeTime, rebuild the heap
        return queue.heapify(task);
    }

    /*
     * snooze the task (by altering its wakeTime) and ensure the
     * heap property is maintained.
     * @returns true if 'task' is in the FutureQueue.
     */
    bool snooze(const ExTask& task, const double secs) {
        std::lock_guard<std::mutex> lock(queueMutex);
        task->snooze(secs);
        // After modifiying the task's wakeTime, rebuild the heap
        return queue.heapify(task);
    }

    /**
     * Checks that the invariants of the future queue are valid.
     * If not then throws std::logic_error.
     */
    void assertInvariants() {
        return queue.verifyHeapProperty();
    }

protected:

    /*
     * HeapifiableQueue exposes a method to maintain the heap ordering
     * of the underlying queue.
     *
     * This class is deliberately hidden inside FutureQueue so that any
     * extensions made to priority_queue can't be accessed without work.
     * I.e. correct locking and any need to 'heapify'.
     */
    class HeapifiableQueue : public std::priority_queue<ExTask, C, Compare> {
    public:
        /*
         * Ensure the heap property is maintained
         * @returns true if 'task' is in the queue and heapify() did something.
         */
        bool heapify(const ExTask& task) {
            // if the task exists, rebuild
            if (exists(task)) {
                if (this->c.back()->getId() == task->getId()) {
                    std::push_heap(this->c.begin(),
                                   this->c.end(),
                                   this->comp);
                } else {
                    std::make_heap(this->c.begin(),
                                   this->c.end(),
                                   this->comp);
                }
                return true;
            } else {
                return false;
            }
        }

        void verifyHeapProperty() {
            auto heap_end = std::is_heap_until(
                    this->c.begin(), this->c.end(), this->comp);
            if (heap_end != this->c.end()) {
                std::string msg;
                msg += "FutureQueue::verifyHeapProperty() - heap invariant "
                       "broken. First non-heap is task:" +
                       (*heap_end)->getDescription() + " wake:" +
                       std::to_string(
                               to_ns_since_epoch((*heap_end)->getWaketime())
                                       .count()) +
                       "\nAll items:\n";

                for (auto& task : this->c) {
                    msg += "\t task:" + task->getDescription() + " wake:" +
                           std::to_string(to_ns_since_epoch(task->getWaketime())
                                                  .count()) +
                           "\n";
                }
                throw std::logic_error(msg);
            }
        }

    protected:
        bool exists(const ExTask& task) {
            return std::find_if(this->c.begin(),
                                this->c.end(),
                                [&task](const ExTask& qTask) {
                                    return task->getId() == qTask->getId();
                                }) != this->c.end();
        }

    } queue;

    // All access to queue must be done with the queueMutex
    std::mutex queueMutex;
};
