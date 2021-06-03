/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_executor_pool.h"

#include <engines/ep/src/objectregistry.h>
#include <executor/taskqueue.h>
#include <memory>

void MockExecutorPool::replaceExecutorPoolWithMock() {
    std::lock_guard<std::mutex> lh(initGuard);
    auto& executor = getInstance();
    if (executor) {
        executor->shutdown();
    }
    auto* epEngine = ObjectRegistry::onSwitchThread(nullptr, true);
    executor = std::make_unique<MockExecutorPool>();
    ObjectRegistry::onSwitchThread(epEngine);
}

bool MockExecutorPool::isTaskScheduled(const task_type_t queueType,
                                       const std::string& taskName) {
    std::lock_guard<std::mutex> lh(tMutex);
    for (const auto& it : taskLocator) {
        const auto& taskQueuePair = it.second;
        if (taskName == taskQueuePair.first->getDescription() &&
            taskQueuePair.second->getQueueType() == queueType) {
            return true;
        }
    }
    return false;
}
