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

#include "dcp/response.h"
#include "test_helpers.h"

#include <folly/portability/GTest.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dockey.h>

TEST(DcpResponseTest, DcpCommit_getMessageSize) {
    std::string key("key"); // tests will see 'key\0'
    auto sdkey = makeStoredDocKey(key, CollectionID{0x5555});
    cb::mcbp::unsigned_leb128<CollectionIDType> lebEncoded(0x5555);
    auto collectionAwareSize = sizeof(protocol_binary_request_header) +
                               sizeof(cb::mcbp::request::DcpCommitPayload) +
                               key.size() + lebEncoded.size();

    auto collectionUnAwareSize = sizeof(protocol_binary_request_header) +
                                 sizeof(cb::mcbp::request::DcpCommitPayload) +
                                 key.size();

    auto rsp1 = std::make_unique<CommitSyncWriteConsumer>(
            0, Vbid(99), 1 /*prepare*/, 2 /*commit*/, sdkey);

    EXPECT_EQ(collectionAwareSize, rsp1->getMessageSize());

    auto rsp2 = std::make_unique<CommitSyncWriteConsumer>(
            0,
            Vbid(99),
            1 /*prepare*/,
            2 /*commit*/,
            sdkey.makeDocKeyWithoutCollectionID());

    EXPECT_EQ(collectionUnAwareSize, rsp2->getMessageSize());

    auto rsp3 =
            std::make_unique<CommitSyncWrite>(0,
                                              Vbid(99),
                                              1 /*prepare*/,
                                              2 /*commit*/,
                                              sdkey,
                                              DocKeyEncodesCollectionId::Yes);

    EXPECT_EQ(collectionAwareSize, rsp3->getMessageSize());

    auto rsp4 = std::make_unique<CommitSyncWrite>(
            0,
            Vbid(99),
            1 /*prepare*/,
            2 /*commit*/,
            sdkey.makeDocKeyWithoutCollectionID(),
            DocKeyEncodesCollectionId::No);

    EXPECT_EQ(collectionUnAwareSize, rsp4->getMessageSize());
}

TEST(DcpResponseTest, DcpAbort_getMessageSize) {
    std::string key("key"); // tests will see 'key\0'
    auto sdkey = makeStoredDocKey(key, CollectionID{0x5555});
    cb::mcbp::unsigned_leb128<CollectionIDType> lebEncoded(0x5555);
    auto collectionAwareSize = sizeof(protocol_binary_request_header) +
                               sizeof(cb::mcbp::request::DcpAbortPayload) +
                               key.size() + lebEncoded.size();

    auto collectionUnAwareSize = sizeof(protocol_binary_request_header) +
                                 sizeof(cb::mcbp::request::DcpAbortPayload) +
                                 key.size();

    auto rsp1 = std::make_unique<AbortSyncWriteConsumer>(
            0, Vbid(99), sdkey, 1 /*prepare*/, 2 /*abort*/
    );

    EXPECT_EQ(collectionAwareSize, rsp1->getMessageSize());

    auto rsp2 = std::make_unique<AbortSyncWriteConsumer>(
            0,
            Vbid(99),
            sdkey.makeDocKeyWithoutCollectionID(),
            1 /*prepare*/,
            2 /*abort*/);

    EXPECT_EQ(collectionUnAwareSize, rsp2->getMessageSize());

    auto rsp3 =
            std::make_unique<AbortSyncWrite>(0,
                                             Vbid(99),
                                             sdkey,
                                             1 /*prepare*/,
                                             2 /*abort*/,
                                             DocKeyEncodesCollectionId::Yes);

    EXPECT_EQ(collectionAwareSize, rsp3->getMessageSize());

    auto rsp4 = std::make_unique<AbortSyncWrite>(
            0,
            Vbid(99),

            sdkey.makeDocKeyWithoutCollectionID(),
            1 /*prepare*/,
            2 /*abort*/,
            DocKeyEncodesCollectionId::No);

    EXPECT_EQ(collectionUnAwareSize, rsp4->getMessageSize());
}