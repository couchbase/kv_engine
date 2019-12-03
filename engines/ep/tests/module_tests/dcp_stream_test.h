/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#pragma once

#include "dcp_test.h"
#include "hash_table.h"
#include "vbucket_queue_item_ctx.h"

class MockDcpConsumer;
class MockPassiveStream;

/**
 * Test fixture for DCP stream tests.
 * The std::string template param specifies the type of bucket to test -
 * "persistent" or "ephemeral".
 */
class StreamTest : public DCPTest,
                   public ::testing::WithParamInterface<std::string> {
protected:
    void SetUp() override;

    void TearDown() override;
};

/**
 * Fixture for DCP stream tests which are specific to Ephemeral.
 */
class EphemeralStreamTest : public StreamTest {};

/*
 * Test fixture for single-threaded ActiveStream tests.
 *
 * Instantiated for both Persistent and Ephemeral buckets.
 */
class SingleThreadedActiveStreamTest
    : virtual public STParameterizedBucketTest {
protected:
    void SetUp() override;
    void TearDown() override;

    void startCheckpointTask();

    void setupProducer(const std::vector<std::pair<std::string, std::string>>&
                               controls = {},
                       bool startCheckpointProcessorTask = false);

    MutationStatus public_processSet(VBucket& vb,
                                     Item& item,
                                     const VBQueueItemCtx& ctx = {});

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;
};

/*
 * Test fixture for single-threaded PassiveStream tests
 *
 * Instantiated for both Persistent and Ephemeral buckets.
 */
class SingleThreadedPassiveStreamTest
    : virtual public STParameterizedBucketTest {
protected:
    void SetUp() override;
    void TearDown() override;

    enum class mb_33773Mode {
        closeStreamOnTask,
        closeStreamBeforeTask,
        noMemory,
        noMemoryAndClosed
    };
    void mb_33773(mb_33773Mode mode);

protected:
    std::shared_ptr<MockDcpConsumer> consumer;
    // Owned by the engine
    MockPassiveStream* stream;
};
