/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "executorpool.h"
#include "task.h"

#include <cstdlib>
#include <iostream>
#include <string>

ExecutorPool::ExecutorPool(size_t sz) {
    roundRobin.store(0);
    executors.reserve(sz);
    for (size_t ii = 0; ii < sz; ++ii) {
        executors.emplace_back(createWorker());
    }
}

void ExecutorPool::schedule(std::shared_ptr<Task>& task, bool runnable) {
    if (task->getMutex().try_lock()) {
        task->getMutex().unlock();
        throw std::logic_error(
            "The mutex should be held when trying to schedule a event");
    }

    executors[++roundRobin % executors.size()]->schedule(task, runnable);
}
