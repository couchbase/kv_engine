/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "dcp_durability_stream_test.h"

#include "checkpoint_utils.h"
#include "dcp_utils.h"
#include "test_helpers.h"

#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"

void DurabilityActiveStreamTest::SetUp() {
    SingleThreadedActiveStreamTest::SetUp();
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    setupProducer({{"enable_synchronous_replication", "true"}});
    ASSERT_TRUE(stream->public_supportSyncReplication());
}

void DurabilityActiveStreamTest::TearDown() {
    SingleThreadedActiveStreamTest::TearDown();
}

void DurabilityActiveStreamTest::testSendDcpPrepare() {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(*vb, 0 /*seqno*/);

    const auto key = makeStoredDocKey("key");
    const auto& value = "value";
    auto item = makePendingItem(
            key,
            value,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         1 /*timeout*/));
    VBQueueItemCtx ctx;
    ctx.durability =
            DurabilityItemCtx{item->getDurabilityReqs(), nullptr /*cookie*/};

    EXPECT_EQ(MutationStatus::WasClean, public_processSet(*vb, *item, ctx));

    // We don't account Prepares in VB stats
    EXPECT_EQ(0, vb->getNumItems());
    // We do in HT stats
    EXPECT_EQ(1, vb->ht.getNumItems());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    // 1 checkpoint
    ASSERT_EQ(1, ckptList.size());
    const auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is pending and contains the expected value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    EXPECT_EQ(key, (*it)->getKey());
    EXPECT_EQ(value, (*it)->getValue()->to_s());

    // We must have ckpt-start + Prepare
    auto outItems = stream->public_getOutstandingItems(*vb);
    ASSERT_EQ(2, outItems.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.at(0)->getOperation());
    ASSERT_EQ(queue_op::pending_sync_write, outItems.at(1)->getOperation());
    // Stream::readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());
    // Push items into the Stream::readyQ
    stream->public_processItems(outItems);
    // Stream::readyQ must contain SnapshotMarker + DCP_PREPARE
    ASSERT_EQ(2, stream->public_readyQSize());
    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Prepare, resp->getEvent());
    const uint64_t prepareSeqno = 1;
    EXPECT_EQ(prepareSeqno, *resp->getBySeqno());
    auto& prepare = static_cast<MutationResponse&>(*resp);
    EXPECT_EQ(key, prepare.getItem()->getKey());
    EXPECT_EQ(value, prepare.getItem()->getValue()->to_s());
    ASSERT_EQ(0, stream->public_readyQSize());
    resp = stream->public_popFromReadyQ();
    ASSERT_FALSE(resp);
}

TEST_F(DurabilityActiveStreamTest, SendDcpPrepare) {
    testSendDcpPrepare();
}

/*
 * This test checks that the ActiveStream::readyQ contains the right DCP
 * messages during the journey of an Aborted sync-write.
 */
TEST_F(DurabilityActiveStreamTest, SendDcpAbort) {
    // First, we need to enqueue a Prepare.
    testSendDcpPrepare();
    auto vb = engine->getVBucket(vbid);
    const auto key = makeStoredDocKey("key");
    const uint64_t prepareSeqno = 1;
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
    }

    // Now we proceed with testing the Abort of that Prepare
    auto& ckptMgr = *vb->checkpointManager;

    // Simulate timeout, indirectly calls VBucket::abort
    vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(1000));

    // We don't account Abort in VB stats
    EXPECT_EQ(0, vb->getNumItems());
    // We must have removed the Prepare from the HashTable and we don't have
    // any "abort" StoredValue
    EXPECT_EQ(0, vb->ht.getNumItems());
    // Note: We don't de-duplicate Prepare and Abort, so we have closed the open
    //     ckpt (the one containing the Prepare), created a new open ckpt and
    //     queued the Abort in the latter. So we must have 2 checkpoints now.
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED,
              ckptList.front()->getState());
    const auto* ckpt = ckptList.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is Abort and doesn't carry any value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::abort_sync_write, (*it)->getOperation());
    EXPECT_FALSE((*it)->getValue());

    // We must have ckpt-start + Abort
    auto outItems = stream->public_getOutstandingItems(*vb);
    ASSERT_EQ(2, outItems.size());
    ASSERT_EQ(queue_op::checkpoint_start, outItems.at(0)->getOperation());
    ASSERT_EQ(queue_op::abort_sync_write, outItems.at(1)->getOperation());
    // Stream::readyQ still empty
    ASSERT_EQ(0, stream->public_readyQSize());
    // Push items into the Stream::readyQ
    stream->public_processItems(outItems);
    // Stream::readyQ must contain SnapshotMarker + DCP_ABORT
    ASSERT_EQ(2, stream->public_readyQSize());
    auto resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    resp = stream->public_popFromReadyQ();
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Abort, resp->getEvent());
    const auto& abort = static_cast<AbortSyncWrite&>(*resp);
    EXPECT_EQ(key, abort.getKey());
    EXPECT_EQ(prepareSeqno, abort.getPreparedSeqno());
    EXPECT_EQ(2, abort.getAbortSeqno());
    ASSERT_EQ(0, stream->public_readyQSize());
    resp = stream->public_popFromReadyQ();
    ASSERT_FALSE(resp);
}

void DurabilityPassiveStreamTest::SetUp() {
    SingleThreadedPassiveStreamTest::SetUp();
    consumer->enableSyncReplication();
    ASSERT_TRUE(consumer->isSyncReplicationEnabled());
}

void DurabilityPassiveStreamTest::TearDown() {
    SingleThreadedPassiveStreamTest::TearDown();
}

TEST_F(DurabilityPassiveStreamTest, MemorySeqnoAckAtSyncWriteReceived) {
    // The consumer receives the snapshot-marker
    uint32_t opaque = 0;
    const uint64_t snapEnd = 3;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  snapEnd,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                  {});
    stream->processMarker(&snapshotMarker);
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(0, readyQ.size());

    // The consumer receives mutations {s:1, s:2, s:3}, with only s:2
    // durable. We have to check that we send a SeqnoAck as soon as the
    // replica receives a SyncWrite and that no further SeqnoAck is sent at
    // receiving the snapshot-end mutation.

    const std::string value("value");

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    const uint64_t syncWriteSeqno = 2;

    auto checkReadyQ = [&readyQ, syncWriteSeqno]() -> void {
        EXPECT_EQ(1, readyQ.size());
        ASSERT_EQ(1, readyQ.size());
        ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement,
                  readyQ.front()->getEvent());
        // @todo-durability: Disabled until DurabilityMonitor updated to have
        // a single prepared seqno (currently we only prepare when on-disk).
#if 0
        const auto* seqnoAck =
                static_cast<const SeqnoAcknowledgement*>(readyQ.front().get());
        EXPECT_EQ(ntohll(syncWriteSeqno), seqnoAck->getPreparedSeqno());
#endif
    };

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      syncWriteSeqno,
                      vbid,
                      value,
                      opaque,
                      cb::durability::Requirements())));
    // Verify that we have 1 SeqnoAck with memSeqno=syncWriteSeqno
    checkReadyQ();

    // snapshot-end
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      snapEnd, vbid, value, opaque)));
    // Verify that we still have only 1 SeqnoAck with memSeqno=syncWriteSeqno
    checkReadyQ();
}

TEST_F(DurabilityPassiveStreamTest, DiskSeqnoAckAtPersistedSeqno) {
    // The consumer receives the snapshot-marker [1, 3]
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  4 /*snapEnd*/,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                  {});
    stream->processMarker(&snapshotMarker);
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(0, readyQ.size());

    // The consumer receives mutations {s:1, s:2, s:3}, with only s:2
    // durable. We have to check that we do send a SeqnoAck as soon as the
    // FlushBatch is persisted, even if we have received/persisted a partial
    // snapshot (note that we never receive s:4 here).

    const std::string value("value");

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      2 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      cb::durability::Requirements())));
    EXPECT_EQ(1, readyQ.size());
    // SeqnoAck carrying mem-seqno in readyQ (mem-seqno acked at Prepare
    // receive)
    ASSERT_EQ(1, readyQ.size());
    ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement,
              readyQ.front()->getEvent());
    const auto* seqnoAck =
            static_cast<const SeqnoAcknowledgement*>(readyQ.front().get());
    EXPECT_EQ(ntohll(0), seqnoAck->getPreparedSeqno());
    ASSERT_TRUE(stream->public_popFromReadyQ());

    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));
    EXPECT_EQ(0, readyQ.size());

    // Flush
    EXPECT_EQ(
            std::make_pair(false /*more_to_flush*/, size_t(3) /*num_flushed*/),
            getEPBucket().flushVBucket(vbid));

    // We must have a correct SeqnoAck in readyQ
    ASSERT_EQ(1, readyQ.size());
    ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement,
              readyQ.front()->getEvent());
    seqnoAck = static_cast<const SeqnoAcknowledgement*>(readyQ.front().get());
    EXPECT_EQ(ntohll(3), seqnoAck->getPreparedSeqno());
}

void DurabilityPassiveStreamTest::testReceiveDcpPrepare() {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(*vb, 0 /*seqno*/);

    // The consumer receives snapshot-marker [1, 2]
    uint32_t opaque = 0;
    SnapshotMarker marker(opaque,
                          vbid,
                          1 /*snapStart*/,
                          2 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                          {} /*streamId*/);
    stream->processMarker(&marker);

    // The consumer receives s:1 durable
    const std::string value("value");
    const uint64_t prepareSeqno = 1;
    ASSERT_EQ(ENGINE_SUCCESS,
              stream->messageReceived(makeMutationConsumerMessage(
                      prepareSeqno,
                      vbid,
                      value,
                      opaque,
                      cb::durability::Requirements())));

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(1, vb->ht.getNumItems());
    const auto key = makeStoredDocKey("key_" + std::to_string(prepareSeqno));
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        EXPECT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        EXPECT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
    }
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    // 1 checkpoint
    ASSERT_EQ(1, ckptList.size());
    const auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is pending and contains the expected value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::pending_sync_write, (*it)->getOperation());
    EXPECT_EQ(key, (*it)->getKey());
    EXPECT_TRUE((*it)->getValue());
    EXPECT_EQ(value, (*it)->getValue()->to_s());
    EXPECT_EQ(1, (*it)->getBySeqno());
}

TEST_F(DurabilityPassiveStreamTest, ReceiveDcpPrepare) {
    testReceiveDcpPrepare();
}

/*
 * This test checks that a DCP Consumer receives and processes correctly a
 * DCP_ABORT message.
 */
TEST_F(DurabilityPassiveStreamTest, ReceiveDcpAbort) {
    // First, simulate the Consumer receiving a Prepare
    testReceiveDcpPrepare();
    auto vb = engine->getVBucket(vbid);
    const uint64_t prepareSeqno = 1;
    const auto key = makeStoredDocKey("key_" + std::to_string(prepareSeqno));
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_TRUE(sv.storedValue);
        ASSERT_EQ(CommittedState::Pending, sv.storedValue->getCommitted());
        ASSERT_EQ(prepareSeqno, sv.storedValue->getBySeqno());
    }

    // Now simulate the Consumer receiving Abort for that Prepare
    uint32_t opaque = 0;
    auto abortReceived = [this, opaque, &key, prepareSeqno](
                                 uint64_t abortSeqno) -> ENGINE_ERROR_CODE {
        return stream->messageReceived(std::make_unique<AbortSyncWrite>(
                opaque, vbid, key, prepareSeqno, abortSeqno));
    };

    // Check a negative first: at Replica we don't expect multiple Durable
    // items within the same checkpoint. That is to avoid Durable items de-dup
    // at Producer.
    uint64_t abortSeqno = prepareSeqno + 1;
    auto thrown{false};
    try {
        abortReceived(abortSeqno);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("duplicate item") !=
                    std::string::npos);
        thrown = true;
    }
    if (!thrown) {
        FAIL();
    }

    // So, we need to simulate a Producer sending another SnapshotMarker with
    // the MARKER_FLAG_CHK set before the Consumer receives the Abort. That
    // will force the Consumer closing the open checkpoint (which Contains the
    // Prepare) and cretaing a new open one for queueing the Abort.
    SnapshotMarker marker(
            opaque,
            vbid,
            3 /*snapStart*/,
            4 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*streamId*/);
    stream->processMarker(&marker);

    // 2 checkpoints
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);
    ASSERT_EQ(2, ckptList.size());
    auto* ckpt = ckptList.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_CLOSED, ckpt->getState());
    ckpt = ckptList.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(0, ckpt->getNumItems());

    // The consumer receives an Abort for the previous Prepare.
    // Note: The call to abortReceived() above throws /after/
    //     PassiveStream::last_seqno has been incremented, so we need to
    //     abortSeqno to bypass ENGINE_ERANGE checks.
    abortSeqno++;
    ASSERT_EQ(ENGINE_SUCCESS, abortReceived(abortSeqno));

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(0, vb->ht.getNumItems());
    {
        const auto sv = vb->ht.findForWrite(key);
        ASSERT_FALSE(sv.storedValue);
    }

    // empty-item
    auto it = ckpt->begin();
    ASSERT_EQ(queue_op::empty, (*it)->getOperation());
    // 1 metaitem (checkpoint-start)
    it++;
    ASSERT_EQ(1, ckpt->getNumMetaItems());
    EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
    // 1 non-metaitem is Abort and carries no value
    it++;
    ASSERT_EQ(1, ckpt->getNumItems());
    EXPECT_EQ(queue_op::abort_sync_write, (*it)->getOperation());
    EXPECT_EQ(key, (*it)->getKey());
    EXPECT_FALSE((*it)->getValue());
    EXPECT_EQ(abortSeqno, (*it)->getBySeqno());
}
