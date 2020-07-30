/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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
#include "cb3_executorpool.h"
#include "configuration.h"
#include "ep_engine.h"
#include "objectregistry.h"

std::mutex ExecutorPool::initGuard;
std::atomic<ExecutorPool*> ExecutorPool::instance;

ExecutorPool *ExecutorPool::get() {
    auto* tmp = instance.load();
    if (tmp == nullptr) {
        LockHolder lh(initGuard);
        tmp = instance.load();
        if (tmp == nullptr) {
            // Double-checked locking if instance is null - ensure two threads
            // don't both create an instance.

            Configuration &config =
                ObjectRegistry::getCurrentEngine()->getConfiguration();
            NonBucketAllocationGuard guard;
            tmp = new CB3ExecutorPool(
                    config.getMaxThreads(),
                    ThreadPoolConfig::ThreadCount(config.getNumReaderThreads()),
                    ThreadPoolConfig::ThreadCount(config.getNumWriterThreads()),
                    config.getNumAuxioThreads(),
                    config.getNumNonioThreads());
            instance.store(tmp);
        }
    }
    return tmp;
}

void ExecutorPool::shutdown() {
    std::lock_guard<std::mutex> lock(initGuard);
    auto* tmp = instance.load();
    if (tmp != nullptr) {
        NonBucketAllocationGuard guard;
        delete tmp;
        instance = nullptr;
    }
}
