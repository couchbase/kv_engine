/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

/*
 * FakeExecutorPool / FakeExecutorThread
 *
 * A pair of classes which act as a fake ExecutorPool for testing purposes.
 * Only executes tasks when explicitly told, and only on the main thread.
 *
 * See SingleThreadedEPStoreTest for basic usage.
 *
 * TODO: Improve usage documentation.
 */

#pragma once

#include "executorpool.h"
#include "executorthread.h"

#include <gtest/gtest.h>

class SingleThreadedExecutorPool : public ExecutorPool {
public:

    /* Registers an instance of this class as "the" executorpool (i.e. what
     * you get when you call ExecutorPool::get()).
     *
     * This *must* be called before the normal ExecutorPool is created.
     */
    static void replaceExecutorPoolWithFake() {
        LockHolder lh(initGuard);
        ExecutorPool* tmp = ExecutorPool::instance;
        if (tmp != NULL) {
            throw std::runtime_error("replaceExecutorPoolWithFake: "
                    "ExecutorPool instance already created - cowardly refusing to continue!");
        }

        EventuallyPersistentEngine *epe =
                ObjectRegistry::onSwitchThread(NULL, true);
        tmp = new SingleThreadedExecutorPool(NUM_TASK_GROUPS);
        ObjectRegistry::onSwitchThread(epe);
        instance = tmp;
    }

    SingleThreadedExecutorPool(size_t nTaskSets)
        : ExecutorPool(/*threads*/0, nTaskSets, 0, 0, 0, 0) {
    }

    bool _startWorkers() {
        // Don't actually start any worker threads (all work will be done
        // synchronously in the same thread) - but we do need to set
        // maxWorkers to at least 1 otherwise ExecutorPool::tryNewWork() will
        // never return any work.

        maxWorkers[WRITER_TASK_IDX] = 1;
        maxWorkers[READER_TASK_IDX] = 1;
        maxWorkers[AUXIO_TASK_IDX]  = 1;
        maxWorkers[NONIO_TASK_IDX]  = 1;

        return true;
    }

    // Helper methods to access normally protected state of ExecutorPool

    TaskQ& getLpTaskQ() {
        return lpTaskQ;
    }
};

/* A fake execution 'thread', to be used with the FakeExecutorPool Allows
 * execution of tasks synchronously in the current thrad.
 */
class FakeExecutorThread : public ExecutorThread {
public:
    FakeExecutorThread(ExecutorPool* manager_, int startingQueue)
        : ExecutorThread(manager_, startingQueue, "mock_executor") {
    }

    void runCurrentTask() {
        // Only supports one-shot tasks
        EXPECT_FALSE(currentTask->run());
        manager->doneWork(curTaskType);
        manager->cancel(currentTask->getId(), true);
    }

    ExTask& getCurrentTask() {
        return currentTask;
    }

    void updateCurrentTime() {
        now = gethrtime();
    }
};
