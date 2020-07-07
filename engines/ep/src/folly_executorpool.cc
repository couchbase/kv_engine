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

#include "folly_executorpool.h"

#include <folly/executors/IOThreadPoolExecutor.h>

FollyExecutorPool::FollyExecutorPool(size_t maxThreads,
                                     ThreadPoolConfig::ThreadCount maxReaders,
                                     ThreadPoolConfig::ThreadCount maxWriters,
                                     size_t maxAuxIO,
                                     size_t maxNonIO)
    : ExecutorPool(maxThreads),
      ioPool(std::make_unique<folly::IOThreadPoolExecutor>(1)) {
}

size_t FollyExecutorPool::getNumWorkersStat() {
    std::abort();
    return 0;
}

size_t FollyExecutorPool::getNumReaders() {
    std::abort();
    return 0;
}

size_t FollyExecutorPool::getNumWriters() {
    std::abort();
    return 0;
}

size_t FollyExecutorPool::getNumAuxIO() {
    std::abort();
    return 0;
}

size_t FollyExecutorPool::getNumNonIO() {
    std::abort();
    return 0;
}

void FollyExecutorPool::setNumReaders(ThreadPoolConfig::ThreadCount v) {
    std::abort();
}

void FollyExecutorPool::setNumWriters(ThreadPoolConfig::ThreadCount v) {
    std::abort();
}

void FollyExecutorPool::setNumAuxIO(uint16_t v) {
    std::abort();
}

void FollyExecutorPool::setNumNonIO(uint16_t v) {
    std::abort();
}

size_t FollyExecutorPool::getNumSleepers() {
    std::abort();
    return 0;
}

size_t FollyExecutorPool::getNumReadyTasks() {
    std::abort();
    return 0;
}

void FollyExecutorPool::registerTaskable(Taskable& taskable) {
    std::abort();
}

std::vector<ExTask> FollyExecutorPool::unregisterTaskable(Taskable& taskable,
                                                          bool force) {
    std::abort();
    return std::vector<ExTask>();
}

size_t FollyExecutorPool::getNumTaskables() const {
    std::abort();
    return 0;
}

size_t FollyExecutorPool::schedule(ExTask task) {
    std::abort();
    return 0;
}

bool FollyExecutorPool::cancel(size_t taskId, bool eraseTask) {
    std::abort();
    return false;
}

bool FollyExecutorPool::wake(size_t taskId) {
    std::abort();
    return false;
}

bool FollyExecutorPool::snooze(size_t taskId, double tosleep) {
    std::abort();
    return false;
}

void FollyExecutorPool::doWorkerStat(Taskable& taskable,
                                     const void* cookie,
                                     const AddStatFn& add_stat) {
    std::abort();
}

void FollyExecutorPool::doTasksStat(Taskable& taskable,
                                    const void* cookie,
                                    const AddStatFn& add_stat) {
    std::abort();
}

void FollyExecutorPool::doTaskQStat(Taskable& taskable,
                                    const void* cookie,
                                    const AddStatFn& add_stat) {
    std::abort();
}
