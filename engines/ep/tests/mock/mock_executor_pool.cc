/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include "mock_executor_pool.h"
#include "objectregistry.h"
#include "taskqueue.h"

void MockExecutorPool::replaceExecutorPoolWithMock() {
    LockHolder lh(initGuard);
    auto* executor = instance.load();
    if (executor) {
        executor->shutdown();
    }
    auto* epEngine = ObjectRegistry::onSwitchThread(nullptr, true);
    executor = new MockExecutorPool();
    ObjectRegistry::onSwitchThread(epEngine);
    instance.store(executor);
}

bool MockExecutorPool::isTaskScheduled(const task_type_t queueType,
                                       const std::string& taskName) {
    LockHolder lh(tMutex);
    for (const auto& it : taskLocator) {
        const auto& taskQueuePair = it.second;
        if (taskName == taskQueuePair.first->getDescription() &&
            taskQueuePair.second->getQueueType() == queueType) {
            return true;
        }
    }
    return false;
}
