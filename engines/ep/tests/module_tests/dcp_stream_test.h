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

#include "dcp/response.h"
#include "dcp_test.h"
#include <utilities/test_manifest.h>

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

    /**
     * Used in some tests for preventing checkpoint removal
     */
    void registerCursorAtCMStart();
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

    void setupProducer(const std::vector<std::pair<std::string, std::string>>&
                               controls = {});

    /**
     * Reset the current producer and stream and recreate with the given params.
     *
     * @param vb
     * @param dcpOpenflags The flags used at producer creation
     * @param jsonFilter JSON representing the collections filter for the stream
     */
    void recreateProducerAndStream(
            VBucket& vb,
            cb::mcbp::DcpOpenFlag flags,
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
            cb::mcbp::DcpOpenFlag flags,
            const std::optional<cb::durability::Requirements>& durReqs);

    enum class Xattrs : uint8_t { None, User, UserAndSys };

    enum class ExpiryPath : uint8_t { Access, Deletion };

    /**
     * Verify that at Expiration we remove everything from the value but System
     * Xattrs.
     *
     * @param flags DcpOpen flags for simulating pre-6.6 and 6.6+ connections
     * @param xattrs Whether the value contain only the body or user/sys xattrs
     *  too
     * @param ExpiryPath The path to execute for triggering the expiration
     */
    void testExpirationRemovesBody(cb::mcbp::DcpOpenFlag flags,
                                   Xattrs xattrs,
                                   ExpiryPath path);

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

    enum class OOMLevel { Bucket, Checkpoint };

    /**
     * Verify that PassiveStream is capable of forcing inbound DCP traffic
     * regardless of any OOM condition
     *
     * @param event The DCP Event under test
     * @param hasValue Used for verifying empty/non-empty tombstones
     */
    void testProcessMessageBypassMemCheck(DcpResponse::Event event,
                                          bool hasValue,
                                          OOMLevel oomLevel);

protected:
    // Should the DcpConsumer have SyncReplication enabled when created in
    // SetUp()?
    bool enableSyncReplication = false;

    /**
     * Should the SetUp() code set the vbucket to replica and create
     * consumer + PassiveStream.
     */
    bool startAsReplica = true;

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

/**
 * The persistent-only version of SingleThreadedActiveStreamTest.
 */
class STActiveStreamPersistentTest : public SingleThreadedActiveStreamTest {};

/**
 * Test fixture that verifies the behaviour of ActiveStream for CDC-enabled
 * connections.
 */
class CDCActiveStreamTest : public STActiveStreamPersistentTest {
protected:
    void SetUp() override;

    /**
     * Create a new open checkpoint, move the cursors (persistence and DCP) onto
     * it and removes all the closed checkpoint.
     * Also persistence and replication are cleared, ie all items persisted and
     * stream drained.
     * Useful helper for when we need to verify something from a clean state in
     * the middle of a test.
     */
    void clearCMAndPersistenceAndReplication();

    enum class HistoryRetentionMetric : uint8_t { BYTES, SECONDS };

    void testResilientToRetentionConfigChanges(HistoryRetentionMetric metric);

    CollectionsManifest manifest;
};

/**
 * Test fixture that verifies the behaviour of PassiveStream for CDC-enabled
 * connections.
 */
class CDCPassiveStreamTest : public STPassiveStreamPersistentTest {
protected:
    void SetUp() override;

    /**
     * Create an historical collection at replica.
     * SysEvent received in a snapshot with range defined by the user.
     *
     * @param snapType Disk or Memory
     * @param snapStart Start seqno of the snap range
     * @parma snapEnd End seqno of the snap range
     */
    void createHistoricalCollection(CheckpointType snapType,
                                    uint64_t snapStart,
                                    uint64_t snapEnd);
};