/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Benchmarks ActiveStream::next() under varying values of
 * dcp_active_stream_inline_checkpoint_item_limit. Below the limit the stream
 * runs checkpoint-item extraction inline; at/above the limit it schedules the
 * ActiveStreamCheckpointProcessorTask. Setting the limit to 0 disables the
 * inline path entirely (always async).
 */

#include "checkpoint_manager.h"
#include "dcp/active_stream.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "engine_fixture.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include "../tests/mock/mock_dcp_producer.h"
#include "../tests/mock/mock_stream.h"

#include <benchmark/benchmark.h>
#include <folly/portability/GTest.h>

class ActiveStreamNextBench : public EngineFixture {
protected:
    void SetUp(const benchmark::State& state) override {
        // Use ephemeral so we don't pay flushing cost - we want to benchmark
        // the in-memory next() codepath. Allow lots of room in the
        // CheckpointManager so a single open checkpoint can hold the entire
        // benchmark batch.
        varConfig =
                "max_size=2000000000;"
                "max_checkpoints=100000000;"
                "checkpoint_max_size=100000000;"
                "bucket_type=ephemeral;";
        EngineFixture::SetUp(state);
        if (state.thread_index() == 0) {
            engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);
            engine->getKVBucket()->createAndScheduleCheckpointDestroyerTasks();
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index() == 0) {
            stream.reset();
            producer.reset();
            engine->getKVBucket()->deleteVBucket(vbid, nullptr);
        }
        EngineFixture::TearDown(state);
    }

    /// Queue numItems mutations split as evenly as possible across
    /// numCheckpoints checkpoints (a new checkpoint is forced between each
    /// batch). Verifies the expected number of items were appended.
    void queueItems(size_t numItems, size_t numCheckpoints) {
        ASSERT_GE(numCheckpoints, 1u);
        ASSERT_GE(numItems, numCheckpoints);
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        auto& manager = *vb->checkpointManager;
        const auto highSeqnoBefore = manager.getHighSeqno();
        const auto numCheckpointsBefore = manager.getNumCheckpoints();
        const std::string value(16, 'x');

        size_t remaining = numItems;
        size_t keyIndex = 0;
        for (size_t cp = 0; cp < numCheckpoints; ++cp) {
            // Distribute remaining items evenly over the remaining
            // checkpoints (ceil division handles non-divisible cases).
            const size_t checkpointsLeft = numCheckpoints - cp;
            const size_t batch =
                    (remaining + checkpointsLeft - 1) / checkpointsLeft;
            for (size_t i = 0; i < batch; ++i, ++keyIndex) {
                queued_item item{
                        new Item(StoredDocKey("key" + std::to_string(keyIndex),
                                              CollectionID::Default),
                                 0,
                                 0,
                                 value.c_str(),
                                 value.size(),
                                 PROTOCOL_BINARY_RAW_BYTES)};
                item->setVBucketId(vbid);
                item->setQueuedTime();
                ASSERT_TRUE(manager.queueDirty(
                        item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr));
            }
            remaining -= batch;
            if (cp + 1 < numCheckpoints) {
                manager.createNewCheckpoint();
            }
        }
        ASSERT_EQ(0, remaining);
        ASSERT_EQ(highSeqnoBefore + static_cast<int64_t>(numItems),
                  manager.getHighSeqno());
        ASSERT_EQ(numCheckpointsBefore + numCheckpoints - 1,
                  manager.getNumCheckpoints());
    }

    /// Reset the open checkpoint - all queued items are dropped.
    void clearCheckpoint() {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        vb->checkpointManager->clear(0);
    }

    /**
     * Create a fresh DcpProducer and MockActiveStream registered against the
     * vbucket. Must be called AFTER any modification of
     * dcp_active_stream_inline_checkpoint_item_limit, since ActiveStream
     * reads that config value at construction.
     */
    void createProducerAndStream() {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        producer =
                std::make_shared<MockDcpProducer>(*engine,
                                                  cookie,
                                                  "active_stream_bench",
                                                  cb::mcbp::DcpOpenFlag::None,
                                                  /*startTask*/ true);
        producer->createCheckpointProcessorTask();

        stream = std::make_shared<MockActiveStream>(
                engine.get(),
                producer,
                cb::mcbp::DcpAddStreamFlag::None,
                /*opaque*/ 0,
                *vb,
                /*st_seqno*/ 0,
                /*en_seqno*/ ~0ULL,
                /*vb_uuid*/ 0xabcd,
                /*snap_start_seqno*/ 0,
                /*snap_end_seqno*/ ~0ULL);
        stream->public_registerCursor(
                *vb->checkpointManager, producer->getName(), 0);
        stream->setActive();
    }

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;
};

/**
 * Time ActiveStream::next() for the case when it populates from checkpoints
 */
BENCHMARK_DEFINE_F(ActiveStreamNextBench, NextInline)
(benchmark::State& state) {
    const auto numItems = static_cast<size_t>(state.range(0));
    const auto numCheckpoints = static_cast<size_t>(state.range(1));
    // Set a massive limit, always want to run inline for the benchmark.
    engine->getConfiguration().setDcpActiveStreamInlineCheckpointItemLimit(
            std::numeric_limits<size_t>::max());

    size_t totalMutations = 0;
    while (state.KeepRunning()) {
        state.PauseTiming();
        // Create the stream first so its cursor pins the checkpoints we are
        // about to populate - otherwise closed checkpoints with no cursor
        // can be removed before next() ever sees them.
        createProducerAndStream();
        queueItems(numItems, numCheckpoints);
        ASSERT_EQ(0, stream->public_readyQSize());

        // Drain the stream. The first next() runs populateReadyQ inline
        // (because numItems < limit); subsequent calls just pop from
        // readyQ. The drain ends when next() returns nullptr.
        size_t mutations = 0;
        std::unique_ptr<DcpResponse> resp;
        bool keepGoing = true;
        while (keepGoing) {
            const auto readyQSize = stream->public_readyQSize();
            if (readyQSize == 0) {
                // Time only the next call which will populate the readyQ.
                state.ResumeTiming();
                resp = stream->next(*producer);
                state.PauseTiming();
            } else {
                resp = stream->next(*producer);
            }
            if (resp) {
                if (resp->getEvent() == DcpResponse::Event::Mutation) {
                    ++mutations;
                }
                continue;
            }
            // resp is null, drained everything.
            keepGoing = false;
        }
        ASSERT_EQ(numItems, mutations);
        totalMutations += mutations;

        state.PauseTiming();
        stream.reset();
        producer.reset();
        clearCheckpoint();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(totalMutations);
}

static void NextInlineArgs(benchmark::internal::Benchmark* b) {
    b->ArgNames({"items", "checkpoints"});
    for (int64_t items : {1, 5, 10, 20, 50, 75, 100}) {
        for (int64_t checkpoints : {1, 2, 4, 8}) {
            // queueItems requires at least one item per checkpoint.
            if (checkpoints > items) {
                continue;
            }
            b->Args({items, checkpoints});
        }
    }
}

BENCHMARK_REGISTER_F(ActiveStreamNextBench, NextInline)->Apply(NextInlineArgs);
