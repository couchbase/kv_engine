/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "checkpoint_manager.h"
#include "dcp/response.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_stream.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/dcp_stream_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <memcached/dcp_stream_id.h>
#include <utilities/test_manifest.h>
#include <array>
#include <deque>
#include <utility>

// These enums control the test input
enum class InputType { Mutation, Prepare, CPEndStart };

enum class ForStream {
    Yes,
    No,
};

/**
 * Test that a filtered DCP stream generates a correct set of responses, with
 * a seqno-advance when required. The test uses a MockActiveStream so that it
 * has finer control over the sequences of Items that are processed into
 * DcpResponse objects (which ultimately control what messages would be sent).
 *
 * The tests generate various sequences of input Items, maybe alternating
 * between the 'target' collection and one other, maybe a prepare is last or
 * not and so on.
 *
 * The TearDown of the test actually runs and validates that the DcpReponses
 * obtained from the ActiveStream match a sequence of DcpRespones that the test
 * generates (based on what we think the correct rules are).
 */
class CollectionsSeqnoAdvanced
    : public SingleThreadedKVBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<InputType, ForStream, int>> {
public:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        auto meta = nlohmann::json{
                {"topology", nlohmann::json::array({{"active", "replica"}})}};
        ASSERT_EQ(cb::engine_errc::success,
                  engine->getKVBucket()->setVBucketState(
                          vbid, vbucket_state_active, &meta));

        // The producer in this test does not support sync-writes so it will
        // always aim to replace with SeqnoAdvanced when required
        producer =
                std::make_shared<MockDcpProducer>(*engine,
                                                  cookie,
                                                  "CollectionsSeqnoAdvanced",
                                                  cb::mcbp::DcpOpenFlag::None,
                                                  false /*startTask*/);

        auto vb = engine->getVBucket(vbid);

        // Create two custom collections, but the test only cares about fruit
        CollectionsManifest cm;
        cm.add(CollectionEntry::vegetable);
        cm.add(CollectionEntry::fruit);
        vb->updateFromManifest(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                makeManifest(cm));
        stream = std::make_shared<MockActiveStream>(
                engine.get(),
                producer,
                cb::mcbp::DcpAddStreamFlag::None,
                0 /*opaque*/,
                *vb,
                0,
                ~0,
                0,
                0,
                0,
                IncludeValue::Yes,
                IncludeXattrs::Yes,
                IncludeDeletedUserXattrs::No,
                R"({"collections":["9"]})");

        // Always queue an initial checkpoint_start item for making the final
        // SnapshotMarker's flags easily predictable.
        // The test-suites stresses extra operations at the end of each test
        // (see ::TearDown()), so this step doesn't invalidate any test.
        queueCPStart();
    }

    void TearDown() override {
        // Now generate the final input Item as per the config
        setupOneOperation(std::get<0>(GetParam()), std::get<1>(GetParam()));

        // Now generate the expected DcpResponses and see what the stream
        // produces.
        generateExpectedResponses();

        stream->public_processItems(input);

        for (const auto& e : expected.responses) {
            auto rsp = stream->public_nextQueuedItem(*producer);
            if (rsp) {
                EXPECT_EQ(*e, *rsp);
            }
            EXPECT_TRUE(rsp) << "DCP response expected:" << e->to_string();
        }
        auto rsp = stream->public_nextQueuedItem(*producer);
        EXPECT_FALSE(rsp) << "Unexpected DcpResponse:" << rsp->to_string();

        stream.reset();
        producer.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    /**
     * Generate a sequence of DcpResponses that we expect the ActiveStream to
     * produce
     */
    void generateExpectedResponses();

    int getInputSize() const {
        return std::get<2>(GetParam());
    }

    void setupOneOperation(InputType type, ForStream fs) {
        switch (type) {
        case InputType::Mutation:
        case InputType::Prepare:
            ++currentSeqno;
            break;
        case InputType::CPEndStart:
            break;
        }

        switch (fs) {
        case ForStream::Yes: {
            queueOperation(type, myCollection);
            break;
        }
        case ForStream::No: {
            queueOperation(type, CollectionEntry::vegetable);
            break;
        }
        }
    }

    void queueOperation(InputType type, CollectionID cid) {
        switch (type) {
        case InputType::Mutation: {
            queueMutation(cid);
            break;
        }
        case InputType::Prepare: {
            queuePrepare(cid);
            break;
        }
        case InputType::CPEndStart: {
            queueCPEnd();
            queueCPStart();
            break;
        }
        }
    }

    void queueMutation(CollectionID cid) {
        auto item = makeCommittedItem(
                makeStoredDocKey(std::to_string(currentSeqno), cid), "value");
        item->setBySeqno(currentSeqno);
        input.items.emplace_back(item);
    }

    void queuePrepare(CollectionID cid) {
        auto item = makePendingItem(
                makeStoredDocKey(std::to_string(currentSeqno), cid), "value");
        item->setBySeqno(currentSeqno);
        input.items.emplace_back(item);
    }

    void queueCPStart() {
        queue_op checkpoint_op = queue_op::checkpoint_start;
        StoredDocKey key(to_string(checkpoint_op), CollectionID::SystemEvent);
        queued_item qi(new Item(key, vbid, checkpoint_op, 1, currentSeqno + 1));
        input.items.emplace_back(qi);
        input.ranges.push_back({{currentSeqno + 1, currentSeqno + 1}, {}, {}});
    }

    void queueCPEnd() {
        auto endSeqno = currentSeqno;
        if (!input.items.empty()) {
            endSeqno = static_cast<uint64_t>(input.items.back()->getBySeqno());
        }
        if (input.ranges.empty()) {
            input.ranges.push_back({{1, endSeqno}, {}, {}});
        } else {
            input.ranges.back().range.setEnd(endSeqno);
        }

        queue_op checkpoint_op = queue_op::checkpoint_end;
        StoredDocKey key(to_string(checkpoint_op), CollectionID::SystemEvent);
        queued_item qi(new Item(key, vbid, checkpoint_op, 1, currentSeqno + 1));
        input.items.emplace_back(qi);
    }

    // Starting seqno, each operation will increment this
    uint64_t currentSeqno{1};

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;

    // The collection that the stream is interested in.
    CollectionID myCollection = CollectionEntry::fruit.getId();

    ActiveStream::OutstandingItemsResult input;

    class ExpectedResponses {
    public:
        ExpectedResponses(Vbid vbid, CollectionID myCollection)
            : vbid(vbid), myCollection(myCollection) {
        }

        void snapshot(uint64_t start,
                      uint64_t end,
                      DcpSnapshotMarkerFlag flags) {
            responses.push_front(
                    std::make_unique<SnapshotMarker>(0 /*opaque*/,
                                                     vbid,
                                                     start,
                                                     end,
                                                     flags,
                                                     std::nullopt,
                                                     std::nullopt,
                                                     std::nullopt,
                                                     cb::mcbp::DcpStreamId{}));
        }

        void seqnoAdvanced(uint64_t seqno) {
            responses.push_back(std::make_unique<SeqnoAdvanced>(
                    0, vbid, cb::mcbp::DcpStreamId{}, seqno));
        }

        std::optional<uint64_t> generateResponse(queued_item& item) {
            if (item->getKey().getCollectionID() == myCollection) {
                if (item->shouldReplicate(false)) {
                    mutation(item);
                }
                return item->getBySeqno();
            }
            return std::nullopt;
        }

        void clear() {
            responses.clear();
        }

        void mutation(queued_item& item) {
            responses.push_back(std::make_unique<MutationResponse>(
                    item,
                    0 /*opaque*/,
                    IncludeValue::Yes,
                    IncludeXattrs::Yes,
                    IncludeDeleteTime::No,
                    IncludeDeletedUserXattrs::No,
                    DocKeyEncodesCollectionId::Yes,
                    EnableExpiryOutput::No,
                    cb::mcbp::DcpStreamId{}));
        }

        std::deque<std::unique_ptr<DcpResponse>> responses;
        Vbid vbid;
        CollectionID myCollection;
    } expected{vbid, myCollection};
};

// Generate a sequence of DcpResponses that we expect the ActiveStream to return
// 1) This test cover a DCP filtered stream, so we only except responses for
//    the fruit collection.
// 2) This test does not use a sync-replication enabled stream, thus any prepare
//    or abort which occurs is not sent - but because prepare/abort updates the
//    collection high-seqno, SeqnoAdvance is expected to bring the stream to the
//    collection high-seqno in the case of 'skipped' abort/prepare (a collection
//     has no concept of max-visible seqno as it can use SeqnoAdvance).
void CollectionsSeqnoAdvanced::generateExpectedResponses() {
    // Generate the expected output from input
    ASSERT_FALSE(input.items.empty());

    // To generate the sequence of DcpResponses we will iterate through the
    // input items and examine the Item 'type' (shouldReplicate) and the item
    // key. As we iterate we are tracking the highest sequence number of our
    // collection... irrespective of what that seqno represents.
    std::optional<uint64_t> myHighCollectionSeqno;
    auto itr = input.items.begin();

    // Secondly we will track the highest myCollection Item
    auto highestMyCollectionItem = input.items.end();

    for (; itr != input.items.end(); itr++) {
        auto seq = expected.generateResponse(*itr);
        if (seq) {
            myHighCollectionSeqno = seq;
            // And track the highest my-collection item
            highestMyCollectionItem = itr;
        }
    }

    // If the highest Item doesn't replicate - it is expected to be 'replaced'
    // with a seqno-advance
    if (highestMyCollectionItem != input.items.end() &&
        !(*highestMyCollectionItem)->shouldReplicate(false)) {
        expected.seqnoAdvanced((*highestMyCollectionItem)->getBySeqno());
    }

    // Finally if nothing in this sequence actually affected the collection,
    // then nothing is expected.
    if (!myHighCollectionSeqno) {
        expected.clear();
    } else {
        // else a snapshot will be seen (this is pushed to the front of the
        // expected responses)
        // Note: At setup we always queue an initial checkpoint_start item, so
        // the CHK flag is set too in the marker
        expected.snapshot(0,
                          myHighCollectionSeqno.value(),
                          DcpSnapshotMarkerFlag::Memory |
                                  DcpSnapshotMarkerFlag::Checkpoint);
    }
}

//
// Generate an input sequence which has 2x the config Items and then alternates
// between the test input and a mutation for a different collection.
// For example input size of 2 we get the following sequences to test
// m1 = mutation for the stream, m2 mutation for other collection. p = prepare
// m1 m2 m1 m2
// m2 m2 m2 m2
// p1 m2 p1 m2
// p2 m2 p2 m2
//
TEST_P(CollectionsSeqnoAdvanced, mixed) {
    // Generate alternating inputs of mutations
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(std::get<0>(GetParam()), std::get<1>(GetParam()));

        setupOneOperation(InputType::Mutation, ForStream::No);
    }
}

// Generate a sequence which is prepare or mutations all for this stream
TEST_P(CollectionsSeqnoAdvanced, allForStream) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(std::get<0>(GetParam()), ForStream::Yes);
    }
}

// Generate a sequence which is prepare/mutations, none for this stream
TEST_P(CollectionsSeqnoAdvanced, noneForStream) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(std::get<0>(GetParam()), ForStream::No);
    }
}

TEST_P(CollectionsSeqnoAdvanced, prepareForMeMutationForOther) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(InputType::Prepare, ForStream::Yes);
        setupOneOperation(InputType::Mutation, ForStream::No);
    }
}

// Test a sequence which always begins with 1 mutations we should receive
TEST_P(CollectionsSeqnoAdvanced, oneForMe) {
    // One for Me
    setupOneOperation(InputType::Mutation, ForStream::Yes);

    // N for you
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(std::get<0>(GetParam()), ForStream::No);
    }
}

// If this test ends with CPEndStart it would trigger MB-49453
TEST_P(CollectionsSeqnoAdvanced, prepareForMeMutationForMe) {
    for (int i = 0; i < getInputSize(); i++) {
        setupOneOperation(InputType::Prepare, std::get<1>(GetParam()));
        setupOneOperation(InputType::Mutation, ForStream::Yes);
    }
}

const std::array<InputType, 3> inputs1 = {
        {InputType::Mutation, InputType::Prepare, InputType::CPEndStart}};
const std::array<ForStream, 2> inputs2 = {{ForStream::Yes, ForStream::No}};

std::string to_string(InputType type) {
    switch (type) {
    case InputType::Prepare: {
        return "Prepare";
    }
    case InputType::Mutation: {
        return "Mutation";
    }
    case InputType::CPEndStart: {
        return "CPEndStart";
    }
    }
    throw std::invalid_argument("to_string(InputType) invalid input");
}

std::string to_string(ForStream fs) {
    switch (fs) {
    case ForStream::Yes: {
        return "for_stream";
    }
    case ForStream::No: {
        return "not_for_stream";
    }
    }
    throw std::invalid_argument("to_string(ForStream) invalid input");
}

std::string printTestName(
        const testing::TestParamInfo<CollectionsSeqnoAdvanced::ParamType>&
                info) {
    return "snapshot_size_" + std::to_string(std::get<2>(info.param)) +
           "_with_an_extra_" + to_string(std::get<0>(info.param)) + "_" +
           to_string(std::get<1>(info.param));
}

INSTANTIATE_TEST_SUITE_P(CollectionsSeqnoAdvanced,
                         CollectionsSeqnoAdvanced,
                         ::testing::Combine(::testing::ValuesIn(inputs1),
                                            ::testing::ValuesIn(inputs2),
                                            ::testing::Values(1, 2, 3)),
                         printTestName);
