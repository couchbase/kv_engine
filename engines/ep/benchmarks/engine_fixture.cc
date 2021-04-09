/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "engine_fixture.h"

#include <fakes/fake_executorpool.h>
#include <mock/mock_synchronous_ep_engine.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <thread>

#include "benchmark_memory_tracker.h"
#include "ep_time.h"
#include "item.h"

void EngineFixture::SetUp(const benchmark::State& state) {
    if (state.thread_index == 0) {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        executorPool = reinterpret_cast<SingleThreadedExecutorPool*>(
                ExecutorPool::get());
        std::string config = "dbname=benchmarks-test;ht_locks=47;" + varConfig;

        engine = SynchronousEPEngine::build(config);

        initialize_time_functions(get_mock_server_api()->core);
        cookie = create_mock_cookie(engine.get());
    } else {
        // 'engine' setup by thread:0; wait until it has completed.
        while (!engine.get()) {
            std::this_thread::yield();
        }
        ObjectRegistry::onSwitchThread(engine.get());
    }
}

void EngineFixture::TearDown(const benchmark::State& state) {
    if (state.thread_index == 0) {
        executorPool->cancelAndClearAll();
        destroy_mock_cookie(cookie);
        engine->getDcpConnMap().manageConnections();
        engine.reset();
        ExecutorPool::shutdown();
        cb::io::rmrf("benchmarks-test");
    }
    ObjectRegistry::onSwitchThread(nullptr);
}

Item EngineFixture::make_item(Vbid vbid,
                              const std::string& key,
                              const std::string& value) {
    Item item({key, DocKeyEncodesCollectionId::No},
              /*flags*/ 0,
              /*exp*/ 0,
              value.c_str(),
              value.size(),
              PROTOCOL_BINARY_DATATYPE_JSON);
    item.setVBucketId(vbid);
    return item;
}
