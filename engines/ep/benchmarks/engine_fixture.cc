/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "engine_fixture.h"

#include <fakes/fake_executorpool.h>
#include <mock/mock_synchronous_ep_engine.h>
#include <programs/engine_testapp/mock_server.h>

#include "benchmark_memory_tracker.h"
#include "ep_time.h"

void EngineFixture::SetUp(const benchmark::State& state) {
    SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
    executorPool =
            reinterpret_cast<SingleThreadedExecutorPool*>(ExecutorPool::get());
    memoryTracker = BenchmarkMemoryTracker::getInstance(
            *get_mock_server_api()->alloc_hooks);
    memoryTracker->reset();
    std::string config = "dbname=benchmarks-test;ht_locks=47;" + varConfig;

    engine.reset(new SynchronousEPEngine(config));
    ObjectRegistry::onSwitchThread(engine.get());

    engine->setKVBucket(engine->public_makeBucket(engine->getConfiguration()));

    engine->public_initializeEngineCallbacks();
    initialize_time_functions(get_mock_server_api()->core);
    cookie = create_mock_cookie();
}

void EngineFixture::TearDown(const benchmark::State& state) {
    executorPool->cancelAndClearAll();
    destroy_mock_cookie(cookie);
    destroy_mock_event_callbacks();
    engine->getDcpConnMap().manageConnections();
    engine.reset();
    ObjectRegistry::onSwitchThread(nullptr);
    ExecutorPool::shutdown();
    memoryTracker->destroyInstance();
}

Item EngineFixture::make_item(uint16_t vbid,
                              const std::string& key,
                              const std::string& value) {
    Item item({key, DocNamespace::DefaultCollection},
              /*flags*/ 0,
              /*exp*/ 0,
              value.c_str(),
              value.size(),
              PROTOCOL_BINARY_DATATYPE_JSON);
    item.setVBucketId(vbid);
    return item;
}
