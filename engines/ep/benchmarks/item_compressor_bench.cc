/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "item.h"
#include "item_compressor_visitor.h"
#include "tests/module_tests/item_compressor_test.h"
#include "tests/module_tests/test_helpers.h"

#include <benchmark/benchmark.h>
#include <engines/ep/src/item_compressor.h>
#include <folly/portability/GTest.h>

#include <memory>

class ItemCompressorBench : public benchmark::Fixture {
public:
    void SetUp(::benchmark::State& state) override {
        // The first parameter specifies the eviction mode:
        EvictionPolicy evictionPolicy;
        switch (state.range(0)) {
        case 0:
            state.SetLabel("ValueOnly");
            evictionPolicy = EvictionPolicy::Value;
            break;
        case 1:
            state.SetLabel("FullEviction");
            evictionPolicy = EvictionPolicy::Full;
            break;
        default:
            FAIL() << "Invalid input param(0) value:" << state.range(0);
        }

        checkpointConfig = std::make_unique<CheckpointConfig>(config);

        vbucket = std::make_unique<EPVBucket>(
                Vbid(0),
                vbucket_state_active,
                globalStats,
                *checkpointConfig,
                /*kvshard*/ nullptr,
                /*lastSeqno*/ 1000,
                /*lastSnapStart*/ 0,
                /*lastSnapEnd*/ 0,
                /*table*/ nullptr,
                std::make_shared<DummyCB>(),
                /*newSeqnoCb*/ nullptr,
                [](Vbid) { return; },
                NoopSyncWriteCompleteCb,
                NoopSyncWriteTimeoutFactory,
                NoopSeqnoAckCb,
                ImmediateCkptDisposer,
                config,
                evictionPolicy,
                std::make_unique<Collections::VB::Manifest>(
                        std::make_shared<Collections::Manager>()));

        populateVbucket();
    }

    void TearDown(const ::benchmark::State& state) override {
        vbucket.reset();
    }

protected:
    /* Fill the bucket with the given number of docs.
     */
    void populateVbucket() {
        // How many items to create in the VBucket
        const size_t ndocs = 50000;

        /* Set the hashTable to a sensible size */
        vbucket->ht.resize(ndocs);

        /* Store items */
        std::string valueData(1024, 'a');
        for (size_t i = 0; i < ndocs; i++) {
            std::string key = "key" + std::to_string(i);

            // Create a compressible item but with value not compressed
            auto item = makeCompressibleItem(vbucket->getId(),
                                             makeStoredDocKey(key.c_str()),
                                             valueData,
                                             PROTOCOL_BINARY_RAW_BYTES,
                                             false);
            ASSERT_EQ(MutationStatus::WasClean, vbucket->ht.set(*item));
        }

        ASSERT_EQ(ndocs, vbucket->ht.getNumItems());
    }

    std::unique_ptr<VBucket> vbucket;
    EPStats globalStats;
    std::unique_ptr<CheckpointConfig> checkpointConfig;
    Configuration config;
};

BENCHMARK_DEFINE_F(ItemCompressorBench, Visit)(benchmark::State& state) {
    ItemCompressorVisitor visitor;
    while (state.KeepRunning()) {
        HashTable::Position pos;
        while (pos != vbucket->ht.endPosition()) {
            state.PauseTiming();
            visitor.setDeadline(std::chrono::steady_clock::now() +
                                std::chrono::milliseconds(20));
            state.ResumeTiming();
            pos = vbucket->ht.pauseResumeVisit(visitor, pos);
        }
    }
    state.SetItemsProcessed(visitor.getVisitedCount());
}

BENCHMARK_REGISTER_F(ItemCompressorBench, Visit)->Range(0, 1);
