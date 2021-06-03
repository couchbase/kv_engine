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

#pragma once

#include "tests/mock/mock_synchronous_ep_engine.h"
#include <benchmark/benchmark.h>
#include <memcached/vbucket.h>

class BenchmarkMemoryTracker;
class CookieIface;
class Item;
class SingleThreadedExecutorPool;

/**
 * A fixture for benchmarking EpEngine and related classes.
 */
class EngineFixture : public benchmark::Fixture {
protected:
    void SetUp(const benchmark::State& state) override;

    void TearDown(const benchmark::State& state) override;

    Item make_item(Vbid vbid, const std::string& key, const std::string& value);

    SynchronousEPEngineUniquePtr engine;
    CookieIface* cookie = nullptr;
    const Vbid vbid = Vbid(0);

    // Allows subclasses to add stuff to the config
    std::string varConfig;
    SingleThreadedExecutorPool* executorPool;
};
