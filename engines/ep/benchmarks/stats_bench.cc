/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <benchmark/benchmark.h>

#include "engine_fixture.h"

#include "../tests/mock/mock_ep_bucket.h"

#include <engines/ep/src/kv_bucket.h>
#include <executor/fake_executorpool.h>
#include <folly/portability/GTest.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>

#include <collections/manager.h>
#include <collections/vbucket_manifest.h>
#include <memcached/server_cookie_iface.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

class FakeManifest : public Collections::VB::Manifest {
public:
    using Collections::VB::Manifest::addCollectionStats;
    using Collections::VB::Manifest::addNewCollectionEntry;
    using Collections::VB::Manifest::Manifest;
};

void dummyCallback(std::string_view, std::string_view, const void*) {
}
/**
 * Fixture for stat collection benchmarks
 */
class StatsBench : public benchmark::Fixture {
public:
    void SetUp(benchmark::State& state) override {
        cookie = create_mock_cookie();

        vbManifest = std::make_unique<FakeManifest>(
                std::make_shared<Collections::Manager>());

        const auto firstCid = 10;
        const auto colCount = state.range(0);
        for (auto cid = firstCid; cid < colCount + firstCid; cid++) {
            vbManifest->addNewCollectionEntry(
                    {0x0, cid},
                    "collection-" + std::to_string(cid),
                    cb::NoExpiryLimit,
                    0);
        }
    }

    void TearDown(benchmark::State& state) override {
        vbManifest.reset();
        destroy_mock_cookie(cookie);
    }

protected:
    CookieIface* cookie;
    // unique ptr to allow easy reset
    std::unique_ptr<FakeManifest> vbManifest;
};

BENCHMARK_DEFINE_F(StatsBench, CollectionStats)(benchmark::State& state) {
    // Benchmark - measure how long it takes to add stats for N collections

    CBStatCollector collector(dummyCallback, cookie);
    while (state.KeepRunning()) {
        vbManifest->addCollectionStats(Vbid(0), collector);
    }
}

BENCHMARK_REGISTER_F(StatsBench, CollectionStats)->Range(1, 1000);

/**
 * Fixture for engine stat benchmarks
 */
class EngineStatsBench : public EngineFixture {
public:
    void SetUp(const benchmark::State& state) override {
        EngineFixture::SetUp(state);
        if (state.thread_index() == 0) {
            // stats check if expiry pager is initialised.
            dynamic_cast<MockEPBucket&>(*engine->getKVBucket())
                    .initializeExpiryPager(engine->getConfiguration());
        }
    }
};

/**
 * Fixture for benchmarking engine stat which aggregate across all vbuckets.
 */
class MultiVBEngineStatsBench : public EngineStatsBench {
public:
    void SetUp(const benchmark::State& state) override {
        if (!varConfig.empty()) {
            varConfig += ";";
        }
        varConfig += "max_vbuckets=1024";
        EngineStatsBench::SetUp(state);
        if (state.thread_index() == 0) {
            auto numVBs = state.range(0);
            for (auto vbid = 0; vbid < numVBs; vbid++) {
                ASSERT_EQ(cb::engine_errc::success,
                          engine->getKVBucket()->setVBucketState(
                                  Vbid(vbid), vbucket_state_active));
            }
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index() == 0) {
            auto numVBs = state.range(0);
            for (auto vbid = 0; vbid < numVBs; vbid++) {
                ASSERT_EQ(cb::engine_errc::success,
                          engine->getKVBucket()->deleteVBucket(Vbid(vbid),
                                                               nullptr));
                executorPool->runNextTask(AUXIO_TASK_IDX,
                                          "Removing (dead) " +
                                                  to_string(Vbid(vbid)) +
                                                  " from memory and disk");
            }
        }
        EngineFixture::TearDown(state);
    }
};

BENCHMARK_DEFINE_F(EngineStatsBench, EngineStats)(benchmark::State& state) {
    CBStatCollector collector(dummyCallback, cookie);
    auto bucketCollector = collector.forBucket("foobar");
    while (state.KeepRunning()) {
        engine->doEngineStats(bucketCollector);
    }
}

BENCHMARK_DEFINE_F(EngineStatsBench, Uuid)(benchmark::State& state) {
    // UUID stat group generates a single response. This is a reasonable
    // example of a "small" stat group.
    while (state.KeepRunning()) {
        engine->getStats(cookie, "uuid", "", dummyCallback);
    }
}

BENCHMARK_DEFINE_F(MultiVBEngineStatsBench, VBucketDetailsStats)
(benchmark::State& state) {
    CBStatCollector collector(dummyCallback, cookie);
    auto bucketCollector = collector.forBucket("foobar");
    while (state.KeepRunning()) {
        engine->doEngineStats(bucketCollector);
    }
}

BENCHMARK_REGISTER_F(EngineStatsBench, EngineStats);
BENCHMARK_REGISTER_F(EngineStatsBench, Uuid);

BENCHMARK_REGISTER_F(MultiVBEngineStatsBench, VBucketDetailsStats)
        ->Range(1, 1024);