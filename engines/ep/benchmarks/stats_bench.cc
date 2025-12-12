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

#include "../tests/mock/mock_dcp_producer.h"
#include "../tests/mock/mock_ep_bucket.h"
#include "../tests/mock/mock_stream.h"

#include <engines/ep/src/dcp/dcp-types.h>
#include <engines/ep/src/kv_bucket.h>
#include <engines/ep/src/vbucket.h>
#include <executor/fake_executorpool.h>
#include <folly/portability/GTest.h>
#include <memcached/protocol_binary.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>

#include <collections/manager.h>
#include <collections/vbucket_manifest.h>
#include <nlohmann/json.hpp>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>
#include <memory>
#include <string>
#include <unordered_map>

class FakeManifest : public Collections::VB::Manifest {
public:
    using Collections::VB::Manifest::addCollectionStats;
    using Collections::VB::Manifest::addNewCollectionEntry;
    using Collections::VB::Manifest::Manifest;
};

void dummyCallback(std::string_view, std::string_view, CookieIface&) {
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
                    Collections::Metered::Yes,
                    CanDeduplicate::Yes,
                    Collections::ManifestUid{0},
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

    CBStatCollector collector(dummyCallback, *cookie);
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
            for (Vbid::id_type vbid = 0; vbid < numVBs; vbid++) {
                ASSERT_EQ(cb::engine_errc::success,
                          engine->getKVBucket()->setVBucketState(
                                  Vbid(vbid), vbucket_state_active));
            }
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index() == 0) {
            auto numVBs = state.range(0);
            for (Vbid::id_type vbid = 0; vbid < numVBs; vbid++) {
                ASSERT_EQ(cb::engine_errc::success,
                          engine->getKVBucket()->deleteVBucket(Vbid(vbid),
                                                               nullptr));
                executorPool->runNextTask(TaskType::AuxIO,
                                          "Removing (dead) " +
                                                  to_string(Vbid(vbid)) +
                                                  " from memory and disk");
            }
        }
        EngineFixture::TearDown(state);
    }
};

class DcpStatsBench : public EngineStatsBench {
public:
    void SetUp(const benchmark::State& state) override {
        EngineStatsBench::SetUp(state);
        producer =
                std::make_shared<MockDcpProducer>(*engine,
                                                  cookie,
                                                  "test_producer",
                                                  cb::mcbp::DcpOpenFlag::None,
                                                  /*startTask*/ false);
        producer->createCheckpointProcessorTask();

        ASSERT_EQ(cb::engine_errc::success,
                  engine->getKVBucket()->setVBucketState(Vbid(0),
                                                         vbucket_state_active));

        auto vb0 = engine->getVBucket(Vbid(0));
        activeStream = std::make_shared<MockActiveStream>(
                engine.get(),
                producer,
                cb::mcbp::DcpAddStreamFlag::None,
                /*opaque*/ 0,
                *vb0,
                /*st_seqno*/ 0,
                /*en_seqno*/ ~0,
                /*vb_uuid*/ 0xabcd,
                /*snap_start_seqno*/ 0,
                /*snap_end_seqno*/ ~0,
                IncludeValue::No,
                IncludeXattrs::No);

        activeStream->public_registerCursor(
                *vb0->checkpointManager, producer->getName(), 0);
        activeStream->setActive();
        auto streamPtr = std::static_pointer_cast<ActiveStream>(activeStream);
        // Pretend we have the requested number of streams.
        producer->enableMultipleStreamRequests();
        for (uint16_t i = 0; i < state.range(1); i++) {
            producer->updateStreamsMap(Vbid(0),
                                       cb::mcbp::DcpStreamId(i),
                                       streamPtr,
                                       DcpProducer::AllowSwapInStreamMap::No);
        }
    }

    void TearDown(const benchmark::State& state) override {
        activeStream.reset();
        producer.reset();
        EngineStatsBench::TearDown(state);
    }

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> activeStream;
};

BENCHMARK_DEFINE_F(EngineStatsBench, EngineStats)(benchmark::State& state) {
    CBStatCollector collector(dummyCallback, *cookie);
    auto bucketCollector = collector.forBucket("foobar");
    while (state.KeepRunning()) {
        engine->doEngineStats(bucketCollector, nullptr);
    }
}

BENCHMARK_DEFINE_F(EngineStatsBench, Uuid)(benchmark::State& state) {
    // UUID stat group generates a single response. This is a reasonable
    // example of a "small" stat group.
    while (state.KeepRunning()) {
        engine->getStats(*cookie, "uuid", "", dummyCallback);
    }
}

BENCHMARK_DEFINE_F(MultiVBEngineStatsBench, VBucketDetailsStats)
(benchmark::State& state) {
    CBStatCollector collector(dummyCallback, *cookie);
    auto bucketCollector = collector.forBucket("foobar");
    while (state.KeepRunning()) {
        engine->doEngineStats(bucketCollector, nullptr);
    }
}

BENCHMARK_DEFINE_F(DcpStatsBench, ProducerStreamStats)
(benchmark::State& state) {
    // Check expected number of times this stream exists in the producer (+1 for
    // the pointer we have).
    ASSERT_EQ(state.range(1) + 1, activeStream.use_count());

    const auto format = state.range(0) ? ConnHandler::StreamStatsFormat::Json
                                       : ConnHandler::StreamStatsFormat::Legacy;

    while (state.KeepRunning()) {
        producer->addStreamStats(dummyCallback, *cookie, format);
    }
}

BENCHMARK_REGISTER_F(DcpStatsBench, ProducerStreamStats)
        ->ArgNames({"Json", "Streams"})
        ->ArgsProduct({
                {0, 1},
                {1, 10, 100, 1000},
        });

class VBucketDetailsBench : public EngineFixture {
public:
    void SetUp(const benchmark::State& state) override {
        EngineFixture::SetUp(state);
        // Warmup the Engine
        auto& epBucket = *static_cast<EPBucket*>(engine->getKVBucket());
        epBucket.initializeWarmupTask();
        epBucket.startWarmupTask();
        auto& readerQueue = *executorPool->getLpTaskQ(TaskType::Reader);
        while (!epBucket.isPrimaryWarmupComplete()) {
            CheckedExecutor executor(executorPool, readerQueue);
            // Run the tasks
            executor.runCurrentTask();
            executor.completeCurrentTask();
        }
        // Set a vbucket with a topology that we can read
        nlohmann::json top;
        top["topology"] = nlohmann::json::array(
                {{"ns_1@lqhhotvgbkkamggk.mbungiljt-jrqoce.nonprod-project-"
                  "active.com",
                  "ns_1@lqhhotvgbkkamggk.mbungiljt-jrqoce.nonprod-project-"
                  "replica1.com",
                  "ns_1@lqhhotvgbkkamggk.mbungiljt-jrqoce.nonprod-project-"
                  "replica2.com"}});
        engine->getKVBucket()->setVBucketState(
                vbid, vbucket_state_active, &top);
    }
};

BENCHMARK_DEFINE_F(VBucketDetailsBench, VBucketDurabilityState)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        engine->getStats(
                *cookie, "vbucket-durability-state 0", "", dummyCallback);
    }
}

BENCHMARK_REGISTER_F(EngineStatsBench, EngineStats);
BENCHMARK_REGISTER_F(EngineStatsBench, Uuid);

BENCHMARK_REGISTER_F(MultiVBEngineStatsBench, VBucketDetailsStats)
        ->Range(1, 1024);

BENCHMARK_REGISTER_F(VBucketDetailsBench, VBucketDurabilityState);
