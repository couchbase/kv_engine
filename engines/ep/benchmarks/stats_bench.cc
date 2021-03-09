/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#include <benchmark/benchmark.h>
#include <folly/portability/GTest.h>
#include <statistics/cbstat_collector.h>

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

void dummyCallback(std::string_view,
                   std::string_view,
                   gsl::not_null<const void*>) {
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
    cb::tracing::Traceable* cookie;
    // unique ptr to allow easy reset
    std::unique_ptr<FakeManifest> vbManifest;
};

BENCHMARK_DEFINE_F(StatsBench, CollectionStats)(benchmark::State& state) {
    // Benchmark - measure how long it takes to add stats for N collections

    CBStatCollector collector(dummyCallback, cookie, get_mock_server_api());
    while (state.KeepRunning()) {
        vbManifest->addCollectionStats(Vbid(0), collector);
    }
}

BENCHMARK_REGISTER_F(StatsBench, CollectionStats)->Range(1, 1000);
