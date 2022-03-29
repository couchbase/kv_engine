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

#pragma once

#include "dcp_test.h"
#include "hash_table.h"
#include "vbucket_queue_item_ctx.h"

class MockDcpConsumer;
class MockPassiveStream;

/**
 * Test fixture for DCP stream tests.
 */
class StreamTest : public DCPTest, virtual public EPEngineParamTest {
protected:
    void SetUp() override;

    void TearDown() override;

    void setupProducerCompression();
};

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
    /**
     * Reset the current producer and stream and recreate with the given params.
     *
     * @param vb
     * @param dcpOpenflags The flags used at producer creation
     * @param jsonFilter JSON representing the collections filter for the stream
     */
    void recreateProducerAndStream(
            VBucket& vb,
            uint32_t flags,
            std::optional<std::string_view> jsonFilter = {});

    /**
     * @param vb
     * @param enforceProducerFlags Whether we should simulate the real StreamReq
     *  where Stream features are enabled depending on the flags passed from
     *  Producer.
     * @param jsonFilter JSON representing the collections filter for the stream
     *
     *  @todo: Currently we use some arg-defaults which would be nice to remove,
     *  but that needs a general refactor and touching many tests unrelated from
     *  the current work.
     */
    void recreateStream(VBucket& vb,
                        bool enforceProducerFlags = false,
                        std::optional<std::string_view> jsonFilter = {});

    /**
     * Verify that a DCP Producer sends user-xattrs in Normal (DCP_DELETE) and
     * Sync (DCP_PREPARE) Deletes.
     *
     * @param durReqs
     */
    void testProducerIncludesUserXattrsInDelete(
            const std::optional<cb::durability::Requirements>& durReqs);

    /**
     * Verifies scenarios where a Producer has IncludeDeletedUserXattrs::No, so
     * user-xattr (if any) must be pruned from the delete value before streaming
     * The test covers Normal and Sync Deletes.
     *
     * @param flags The DcpOpen flags under test
     */
    void testProducerPrunesUserXattrsForDelete(
            uint32_t flags,
            const std::optional<cb::durability::Requirements>& durReqs);

    enum class Xattrs : uint8_t { None, User, UserAndSys };

    /**
     * Verify that at Expiration we remove everything from the value but System
     * Xattrs.
     *
     * @param flags DcpOpen flags for simulating a pre-6.6 and 6.6 connections
     * @param xattrs Whether the value contain only the body or user/sys xattrs
     *  too
     */
    void testExpirationRemovesBody(uint32_t flags, Xattrs xattrs);

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

    /**
     * Helper method to construct the consumer and stream objects.
     * Postcondition: Both objects are created. stream has accepted the DCP
     * Stream and is in state AwaitingFirstSnapshotMarker.
     */
    void setupConsumerAndPassiveStream();
    void setupConsumer();
    void setupPassiveStream();
    void consumePassiveStreamStreamReq();
    void consumePassiveStreamAddStream();
    void maybeConsumePassiveStreamSeqnoAck();

    enum class mb_33773Mode {
        closeStreamOnTask,
        closeStreamBeforeTask,
        noMemory,
        noMemoryAndClosed
    };
    void mb_33773(mb_33773Mode mode);

    /**
     * Test that when we transition state to active we clear the
     * initialDiskSnapshot flag to ensure that we can stream from this vBucket.
     */
    void testInitialDiskSnapshotFlagClearedOnTransitionToActive(
            vbucket_state_t initialState);

    /**
     * Test that the Consumer rejects body in deletion's value abd fails.
     * That is the behaviour when (allowSanitizeValueInDeletion = false).
     *
     * @param durReqs (optional) The Dur Reqs, if we are testing SyncDelete
     */
    void testConsumerRejectsBodyInDeletion(
            const std::optional<cb::durability::Requirements>& durReqs);

    /**
     * Test that the Consumer removes the body (if any) in deletion's value.
     * That is the behaviour when (allowSanitizeValueInDeletion = true).
     *
     * @param durReqs (optional) The Dur Reqs, if we are testing SyncDelete
     */
    void testConsumerSanitizesBodyInDeletion(
            const std::optional<cb::durability::Requirements>& durReqs);

    /**
     * Test that the Consumer accepts user-xattrs in deletion for enabled
     * connections.
     *
     * @param sysXattrs Does the tested payload contain sys-xattrs?
     * @param durReqs (optional) The Dur Reqs, if we are testing SyncDelete
     * @param compressed Is the tested payload compressed?
     */
    void testConsumerReceivesUserXattrsInDelete(
            bool sysXattrs,
            const std::optional<cb::durability::Requirements>& durReqs,
            bool compressed = false);

protected:
    // Should the DcpConsumer have SyncReplication enabled when created in
    // SetUp()?
    bool enableSyncReplication = false;

    std::shared_ptr<MockDcpConsumer> consumer;
    // Owned by the engine
    MockPassiveStream* stream;
};

/**
 * The persistent-only version of SingleThreadedPassiveStreamTest.
 */
class STPassiveStreamPersistentTest : public SingleThreadedPassiveStreamTest {
public:
    void SetUp() override;

    void checkVBState(uint64_t lastSnapStart,
                      uint64_t lastSnapEnd,
                      CheckpointType type,
                      uint64_t hps,
                      uint64_t hcs,
                      uint64_t maxDelRevSeqno);
};

class STPassiveStreamMagmaTest : public STPassiveStreamPersistentTest {};
