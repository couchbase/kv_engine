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