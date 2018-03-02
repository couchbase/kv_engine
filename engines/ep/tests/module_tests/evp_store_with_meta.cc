/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil
 * -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "bgfetcher.h"
#include "ep_time.h"
#include "evp_store_single_threaded_test.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/test_helpers.h"
#include "utilities/protocol2text.h"

#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

class WithMetaTest : public SingleThreadedEPBucketTest {
public:
    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
        store->setVBucketState(vbid, vbucket_state_active, false);
        expiry = ep_real_time() + 31557600; // +1 year in seconds
    }

    void enableLww() {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "conflict_resolution_type=lww";
    }

    /**
     * Build a *_with_meta packet, defaulting a number of arguments (keeping
     * some of the test bodies smaller)
     */
    std::vector<char> buildWithMeta(protocol_binary_command op,
                                    ItemMetaData itemMeta,
                                    const std::string& key,
                                    const std::string& value) const {
        return buildWithMetaPacket(op,
                                   0 /*datatype*/,
                                   vbid,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   key,
                                   value,
                                   {},
                                   0);
    }

    /**
     * Given a buffer of data representing a with_meta packet, update the meta
     * Allows test to avoid lots of allocation/copying when creating inputs.
     */
    static void updateMeta(std::vector<char>& wm,
                           uint64_t cas,
                           uint64_t revSeq,
                           uint32_t flags,
                           uint32_t exp) {
        auto packet = reinterpret_cast<protocol_binary_request_set_with_meta*>(
                wm.data());
        packet->message.body.cas = htonll(cas);
        packet->message.body.seqno = htonll(revSeq);
        packet->message.body.expiration = htonl(exp);
        packet->message.body.flags = flags;
    }

    /**
     * Given a buffer of data representing a with_meta packet, update the meta
     * Allows test to avoid lots of allocation/copying when creating inputs.
     */
    static void updateMeta(std::vector<char>& wm,
                           const ItemMetaData& itemMeta) {
        updateMeta(wm,
                   itemMeta.cas,
                   itemMeta.revSeqno,
                   itemMeta.flags,
                   uint32_t(itemMeta.exptime));
    }

    /**
     * Call the correct engine function for the op (set vs delete)
     */
    ENGINE_ERROR_CODE callEngine(protocol_binary_command op,
                                 std::vector<char>& wm) {
        if (op == PROTOCOL_BINARY_CMD_DEL_WITH_META ||
            op == PROTOCOL_BINARY_CMD_DELQ_WITH_META) {
            return engine->deleteWithMeta(
                    cookie,
                    reinterpret_cast<protocol_binary_request_delete_with_meta*>(
                            wm.data()),
                    this->addResponse,
                    DocNamespace::DefaultCollection);
        } else {
            return engine->setWithMeta(
                    cookie,
                    reinterpret_cast<protocol_binary_request_set_with_meta*>(
                            wm.data()),
                    this->addResponse,
                    DocNamespace::DefaultCollection);
        }
    }

    /**
     * Get the item and check its value, if called for a delete, assuming
     * delete with value
     */
    void checkGetItem(
            const std::string& key,
            const std::string& expectedValue,
            ItemMetaData expectedMeta,
            ENGINE_ERROR_CODE expectedGetReturnValue = ENGINE_SUCCESS) {
        auto result = store->get({key, DocNamespace::DefaultCollection},
                                 vbid,
                                 nullptr,
                                 GET_DELETED_VALUE);

        ASSERT_EQ(expectedGetReturnValue, result.getStatus());

        if (expectedGetReturnValue == ENGINE_SUCCESS) {
            EXPECT_EQ(0,
                      strncmp(expectedValue.data(),
                              result.item->getData(),
                              result.item->getNBytes()));
            EXPECT_EQ(expectedMeta.cas, result.item->getCas());
            EXPECT_EQ(expectedMeta.revSeqno, result.item->getRevSeqno());
            EXPECT_EQ(expectedMeta.flags, result.item->getFlags());
            EXPECT_EQ(expectedMeta.exptime, result.item->getExptime());
        }
    }

    void oneOp(protocol_binary_command op,
               ItemMetaData itemMeta,
               int options,
               protocol_binary_response_status expectedResponseStatus,
               const std::string& key,
               const std::string& value) {
        auto swm = buildWithMetaPacket(op,
                                       0 /*datatype*/,
                                       vbid,
                                       0 /*opaque*/,
                                       0 /*cas*/,
                                       itemMeta,
                                       key,
                                       value,
                                       {},
                                       options);
        if (expectedResponseStatus == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) {
            EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, callEngine(op, swm));
        } else {
            EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, swm));
            EXPECT_EQ(expectedResponseStatus, getAddResponseStatus());
        }
    }

    /**
     * Run one op and check the result
     */
    void oneOpAndCheck(protocol_binary_command op,
                       ItemMetaData itemMeta,
                       int options,
                       bool withValue,
                       protocol_binary_response_status expectedResponseStatus,
                       ENGINE_ERROR_CODE expectedGetReturnValue) {
        std::string key = "mykey";
        std::string value;
        if (withValue) {
            value = createXattrValue("myvalue"); // xattr but stored as raw
        }
        oneOp(op,
              itemMeta,
              options,
              expectedResponseStatus,
              key,
              value);
        checkGetItem(key, value, itemMeta, expectedGetReturnValue);
    }

    // *_with_meta with winning mutations
    struct TestData {
        ItemMetaData meta;
        protocol_binary_response_status expectedStatus;
    };

    /**
     * The conflict_win test is reused by seqno/lww and is intended to
     * test each winning op/meta input
     */
    void conflict_win(protocol_binary_command op,
                      int options,
                      const std::array<TestData, 4>& testData,
                      const ItemMetaData& itemMeta);
    /**
     * The conflict_lose test is reused by seqno/lww and is intended to
     * test each winning op/meta input
     */
    void conflict_lose(protocol_binary_command op,
                       int options,
                       bool withValue,
                       const std::array<TestData, 4>& testData,
                       const ItemMetaData& itemMeta);

    /**
     * The conflict_del_lose_xattr test demonstrates how a delete never gets
     * to compare xattrs when in conflict.
     */
    void conflict_del_lose_xattr(protocol_binary_command op,
                                 int options,
                                 bool withValue);

    /**
     * The conflict_lose_xattr test demonstrates how a set gets
     * to compare xattrs when in conflict, and the server doc would win.
     */
    void conflict_lose_xattr(protocol_binary_command op,
                             int options,
                             bool withValue);
    /**
     * Initialise an expiry value which allows us to set/get items without them
     * expiring, i.e. a few years of expiry wiggle room
     */
    time_t expiry;
};

class WithMetaLwwTest : public WithMetaTest {
public:
    void SetUp() override {
        enableLww();
        WithMetaTest::SetUp();
    }
};

class DelWithMetaTest
        : public WithMetaTest,
          public ::testing::WithParamInterface<
                  ::testing::tuple<bool, protocol_binary_command>> {
public:
    void SetUp() override {
        withValue = ::testing::get<0>(GetParam());
        op = ::testing::get<1>(GetParam());
        WithMetaTest::SetUp();
    }

    protocol_binary_command op;
    bool withValue;
};

class DelWithMetaLwwTest
        : public WithMetaTest,
          public ::testing::WithParamInterface<
                  ::testing::tuple<bool, protocol_binary_command>> {
public:
    void SetUp() override {
        withValue = ::testing::get<0>(GetParam());
        op = ::testing::get<1>(GetParam());
        enableLww();
        WithMetaTest::SetUp();
    }

    protocol_binary_command op;
    bool withValue;
};

class AllWithMetaTest
        : public WithMetaTest,
          public ::testing::WithParamInterface<protocol_binary_command> {};

class AddSetWithMetaTest
        : public WithMetaTest,
          public ::testing::WithParamInterface<protocol_binary_command> {};

class AddSetWithMetaLwwTest
        : public WithMetaTest,
          public ::testing::WithParamInterface<protocol_binary_command> {
public:
    void SetUp() override {
        enableLww();
        WithMetaTest::SetUp();
    }
};

class XattrWithMetaTest
        : public WithMetaTest,
          public ::testing::WithParamInterface<
                  ::testing::tuple<bool, protocol_binary_command>> {};

class SnappyWithMetaTest : public WithMetaTest,
                           public ::testing::WithParamInterface<bool> {};

TEST_P(AddSetWithMetaTest, basic) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  ENGINE_SUCCESS);
}

TEST_F(WithMetaTest, basicAdd) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    oneOpAndCheck(PROTOCOL_BINARY_CMD_ADD_WITH_META,
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  ENGINE_SUCCESS);

    oneOpAndCheck(PROTOCOL_BINARY_CMD_ADD_WITH_META,
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, // can't do a second add
                  ENGINE_SUCCESS); // can still get the key
}

TEST_P(DelWithMetaTest, basic) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    // A delete_w_meta against an empty bucket queues a BGFetch (get = ewblock)
    // A delete_w_meta(with_value) sets the new value (get = success)
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);
}

TEST_P(AllWithMetaTest, invalidCas) {
    // 0 CAS in the item meta is invalid
    ItemMetaData itemMeta{0 /*cas*/, 0, 0, 0};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
                  ENGINE_KEY_ENOENT);

    // -1 CAS in the item meta is invalid
    itemMeta.cas = ~0ull;
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
                  ENGINE_KEY_ENOENT);
}

TEST_P(DelWithMetaTest, invalidCas) {
    // 0 CAS in the item meta is invalid
    ItemMetaData itemMeta{0 /*cas*/, 0, 0, 0};
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
                  ENGINE_KEY_ENOENT);

    // -1 CAS in the item meta is invalid
    itemMeta.cas = ~0ull;
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AllWithMetaTest, failForceAccept) {
    // FORCE_ACCEPT_WITH_META_OPS not allowed unless we're LWW
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_ACCEPT_WITH_META_OPS,
                  true /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_EINVAL,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AddSetWithMetaLwwTest, allowForceAccept) {
    // FORCE_ACCEPT_WITH_META_OPS ok on LWW
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_ACCEPT_WITH_META_OPS,
                  true /*set a value*/,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  ENGINE_SUCCESS);
}

TEST_P(DelWithMetaLwwTest, allowForceAccept) {
    // FORCE_ACCEPT_WITH_META_OPS ok on LWW
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(op,
                  itemMeta,
                  FORCE_ACCEPT_WITH_META_OPS,
                  withValue,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);
}

TEST_P(AllWithMetaTest, regenerateCASInvalid) {
    // REGENERATE_CAS cannot be by itself
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  REGENERATE_CAS,
                  true,
                  PROTOCOL_BINARY_RESPONSE_EINVAL,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AllWithMetaTest, forceFail) {
    store->setVBucketState(vbid, vbucket_state_replica, false);
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0 /*no options*/,
                  true,
                  PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AllWithMetaTest, forceSuccessReplica) {
    store->setVBucketState(vbid, vbucket_state_replica, false);
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_WITH_META_OP,
                  true,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  ENGINE_SUCCESS);
}

TEST_P(AllWithMetaTest, forceSuccessPending) {
    store->setVBucketState(vbid, vbucket_state_pending, false);
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_WITH_META_OP,
                  true,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  ENGINE_SUCCESS);
}

TEST_P(AllWithMetaTest, regenerateCAS) {
    // Test that
    uint64_t cas = 1;
    auto swm =
            buildWithMetaPacket(GetParam(),
                                0 /*datatype*/,
                                vbid /*vbucket*/,
                                0 /*opaque*/,
                                0 /*cas*/,
                                {cas, 0, 0, 0},
                                "mykey",
                                "myvalue",
                                {},
                                SKIP_CONFLICT_RESOLUTION_FLAG | REGENERATE_CAS);

    EXPECT_EQ(ENGINE_SUCCESS, callEngine(GetParam(), swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());
    auto result = store->get({"mykey", DocNamespace::DefaultCollection},
                             vbid,
                             nullptr,
                             GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
    EXPECT_NE(cas, result.item->getCas()) << "CAS didn't change";
}

TEST_P(AllWithMetaTest, invalid_extlen) {
    // extlen must be very specific values
    std::string key = "mykey";
    std::string value = "myvalue";
    ItemMetaData itemMeta{1, 0, 0, 0};

    auto swm = buildWithMetaPacket(GetParam(),
                                   0 /*datatype*/,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   key,
                                   value);

    auto packet = reinterpret_cast<protocol_binary_request_set_with_meta*>(
            swm.data());
    // futz the extlen (yes yes AFL fuzz would be ace)
    for (uint8_t e = 0; e < 0xff; e++) {
        if (e == 24 || e == 26 || e == 28 || e == 30) {
            // Skip the valid sizes
            continue;
        }
        packet->message.header.request.extlen = e;
        EXPECT_EQ(ENGINE_SUCCESS, callEngine(GetParam(), swm));
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, getAddResponseStatus());
        checkGetItem(key, value, itemMeta, ENGINE_KEY_ENOENT);
    }
}

// Test to verify that a set with meta will store the data
// as uncompressed in the hash table
TEST_F(WithMetaTest, storeUncompressedInOffMode) {
    std::string valueData{R"({"aaaaaaaaa":10000000000})"};
    auto item = makeCompressibleItem(vbid, makeStoredDocKey("key"), valueData,
                                     PROTOCOL_BINARY_RAW_BYTES, true);

    item->setCas();

    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY, item->getDataType());

    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(item->getData()),
                                item->getNBytes()};

    ItemMetaData itemMeta{item->getCas(), item->getRevSeqno(),
                          item->getFlags(), item->getExptime()};

    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);

    auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_SET_WITH_META,
                                   item->getDataType() /*datatype*/,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   std::string("key"),
                                   std::string(item->getData(), item->getNBytes()),
                                   {},
                                   SKIP_CONFLICT_RESOLUTION_FLAG);
    EXPECT_EQ(ENGINE_SUCCESS, callEngine(PROTOCOL_BINARY_CMD_SET_WITH_META, swm));

    VBucketPtr vb = store->getVBucket(vbid);
    StoredValue* v(vb->ht.find(makeStoredDocKey("key"), TrackReference::No,
                               WantsDeleted::No));
    ASSERT_NE(nullptr, v);
    EXPECT_EQ(valueData, v->getValue()->to_s());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, v->getDatatype());
}

TEST_P(AllWithMetaTest, nmvb) {
    std::string key = "mykey";
    std::string value = "myvalue";
    auto swm = buildWithMetaPacket(GetParam(),
                                   0 /*datatype*/,
                                   vbid + 1 /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   {1, 0, 0, 0},
                                   key,
                                   value);
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, callEngine(GetParam(), swm));

    // Set a dead VB
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid + 1, vbucket_state_dead, false));
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, callEngine(GetParam(), swm));

    // update the VB in the packet to the pending one
    auto packet = reinterpret_cast<protocol_binary_request_header*>(swm.data());
    packet->request.vbucket = htons(vbid + 2);
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid + 2, vbucket_state_pending, false));
    EXPECT_EQ(ENGINE_EWOULDBLOCK, callEngine(GetParam(), swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());

    // Re-run the op now active, else we have a memory leak
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid + 2, vbucket_state_active, false));
    EXPECT_EQ(ENGINE_SUCCESS, callEngine(GetParam(), swm));
}

TEST_P(AllWithMetaTest, takeoverBackedup) {
    ItemMetaData itemMeta{1, 0, 0, expiry};
    auto swm = buildWithMetaPacket(GetParam(),
                                   0 /*datatype*/,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   "mykey",
                                   "myvalue");

    store->getVBucket(vbid)->setTakeoverBackedUpState(true);
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0,
                  true,
                  PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AllWithMetaTest, degraded) {
    ItemMetaData itemMeta{1, 0, 0, expiry};
    auto swm = buildWithMetaPacket(GetParam(),
                                   0 /*datatype*/,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   "mykey",
                                   "myvalue");

    engine->public_enableTraffic(false);
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0,
                  true,
                  PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                  ENGINE_KEY_ENOENT);
}

void WithMetaTest::conflict_lose(protocol_binary_command op,
                                 int options,
                                 bool withValue,
                                 const std::array<TestData, 4>& testData,
                                 const ItemMetaData& itemMeta) {
    std::string value;
    if (withValue) {
        value = createXattrValue("myvalue");
    }
    std::string key = "mykey";
    // First add a document so we have something to conflict with
    auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_ADD_WITH_META,
                                   0,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   key,
                                   value,
                                   {},
                                   options);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_ADD_WITH_META, swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());

    for (const auto& td : testData) {
        oneOp(op, td.meta, options, td.expectedStatus, key, value);
    }
}

// store a document then <op>_with_meta with equal ItemMeta but xattr on
void WithMetaTest::conflict_del_lose_xattr(protocol_binary_command op,
                                           int options,
                                           bool withValue) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::string value;
    if (withValue) {
        value = createXattrValue("myvalue");
    }
    std::string key = "mykey";
    // First add a document so we have something to conflict with
    auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_ADD_WITH_META,
                                   0 /*xattr off*/,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   key,
                                   value,
                                   {},
                                   options);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_ADD_WITH_META, swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());

    // revSeqno/cas/exp/flags equal, xattr on, conflict (a set would win)
    swm = buildWithMetaPacket(op,
                              PROTOCOL_BINARY_DATATYPE_XATTR,
                              vbid /*vbucket*/,
                              0 /*opaque*/,
                              0 /*cas*/,
                              itemMeta,
                              key,
                              value,
                              {},
                              options);
    EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, getAddResponseStatus());
}

void WithMetaTest::conflict_lose_xattr(protocol_binary_command op,
                                       int options,
                                       bool withValue) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::string value;
    if (withValue) {
        value = createXattrValue("myvalue");
    }
    std::string key = "mykey";
    // First add a document so we have something to conflict with
    auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_ADD_WITH_META,
                                   PROTOCOL_BINARY_DATATYPE_XATTR,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   key,
                                   value,
                                   {},
                                   options);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_ADD_WITH_META, swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());

    // revSeqno/cas/exp/flags equal, xattr off, conflict (a set would win)
    swm = buildWithMetaPacket(op,
                              0,
                              vbid /*vbucket*/,
                              0 /*opaque*/,
                              0 /*cas*/,
                              itemMeta,
                              key,
                              value,
                              {},
                              options);
    EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, getAddResponseStatus());
}

TEST_P(DelWithMetaTest, conflict_lose) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) seqno 2) cas 3) expiry 4) flags 5) xattr
    // However deletes only check 1 and 2.

    std::array<TestData, 4> data;

    // 1) revSeqno is less and everything else larger. Expect conflict
    data[0] = {{101, 99, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 2. revSeqno is equal, cas is less, others are larger. Expect conflict
    data[1] = {{99, 100, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};

    conflict_lose(op, 0, withValue, data, itemMeta);
}

TEST_P(DelWithMetaLwwTest, conflict_lose) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    // However deletes only check 1 and 2.

    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 2. cas is equal, revSeqno is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};

    conflict_lose(op, FORCE_ACCEPT_WITH_META_OPS, withValue, data, itemMeta);
}

TEST_P(DelWithMetaTest, conflict_xattr_lose) {
    conflict_del_lose_xattr(op, 0, withValue);
}

TEST_P(DelWithMetaLwwTest, conflict_xattr_lose) {
    conflict_del_lose_xattr(op, FORCE_ACCEPT_WITH_META_OPS, withValue);
}

TEST_P(AddSetWithMetaTest, conflict_lose) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) seqno 2) cas 3) expiry 4) flags 5) xattr
    std::array<TestData, 4> data;
    // 1) revSeqno is less and everything else larger. Expect conflict
    data[0] = {{101, 99, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 2. revSeqno is equal, cas is less, others are larger. Expect conflict
    data[1] = {{99, 100, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};

    conflict_lose(
            GetParam(), 0 /*options*/, true /*withValue*/, data, itemMeta);
}

TEST_P(AddSetWithMetaLwwTest, conflict_lose) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 2. cas is equal, revSeq is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1},
               PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS};

    conflict_lose(GetParam(), FORCE_ACCEPT_WITH_META_OPS, true, data, itemMeta);
}

TEST_P(AddSetWithMetaTest, conflict_xattr_lose) {
    conflict_lose_xattr(GetParam(), 0 /*options*/, true /*withvalue*/);
}

TEST_P(AddSetWithMetaLwwTest, conflict_xattr_lose) {
    conflict_lose_xattr(
            GetParam(), FORCE_ACCEPT_WITH_META_OPS, true /*withvalue*/);
}

// This test will store an item with this meta data then store again
// using the testData entries
void WithMetaTest::conflict_win(protocol_binary_command op,
                                int options,
                                const std::array<TestData, 4>& testData,
                                const ItemMetaData& itemMeta) {
    EXPECT_NE(op, PROTOCOL_BINARY_CMD_ADD_WITH_META);
    EXPECT_NE(op, PROTOCOL_BINARY_CMD_ADDQ_WITH_META);
    bool isDelete = op == PROTOCOL_BINARY_CMD_DEL_WITH_META ||
                    op == PROTOCOL_BINARY_CMD_DELQ_WITH_META;
    bool isSet = op == PROTOCOL_BINARY_CMD_SET_WITH_META ||
                 op == PROTOCOL_BINARY_CMD_SETQ_WITH_META;

    int counter = 0;
    for (auto& td : testData) {
        // Set our "target" (new key each iteration)
        std::string key = "mykey" + std::to_string(counter);
        key.push_back(op); // and the op for test uniqueness
        std::string value = "newvalue" + std::to_string(counter);
        auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_SET_WITH_META,
                                       0 /*datatype*/,
                                       vbid /*vbucket*/,
                                       0 /*opaque*/,
                                       0 /*cas*/,
                                       itemMeta,
                                       key,
                                       "myvalue",
                                       {},
                                       options);

        EXPECT_EQ(ENGINE_SUCCESS,
                  callEngine(PROTOCOL_BINARY_CMD_SET_WITH_META, swm));
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus())
                << "Failed to set the target key:" << key;

        // Next the test packet (always with a value).
        auto wm = buildWithMetaPacket(op,
                                      0 /*datatype*/,
                                      vbid /*vbucket*/,
                                      0 /*opaque*/,
                                      0 /*cas*/,
                                      td.meta,
                                      key,
                                      value,
                                      {},
                                      options);

        // Now set/del against the item using the test iteration metadata
        EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, wm));

        auto status = getAddResponseStatus();
        if (isDelete) {
            EXPECT_EQ(td.expectedStatus, status)
                    << "Failed deleteWithMeta for iteration " << counter;
            if (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                checkGetItem(key, value, td.meta);
            }
        } else {
            EXPECT_TRUE(isSet);
            EXPECT_EQ(td.expectedStatus, status)
                    << "Failed setWithMeta for iteration " << counter;
            checkGetItem(key, value, td.meta);
        }
        counter++;
    }

    // ... Finally give an Item with a datatype (not xattr)
    std::string key = "mykey" + std::to_string(counter);
    key.push_back(op); // and the op for test uniqueness
    auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_ADD_WITH_META,
                                   PROTOCOL_BINARY_DATATYPE_JSON,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   key,
                                   "myvalue",
                                   {},
                                   options);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_ADD_WITH_META, swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());

    // And test same cas/seq/exp/flags but marked with xattr
    auto xattrValue = createXattrValue("xattr_value");
    swm = buildWithMetaPacket(op,
                              PROTOCOL_BINARY_DATATYPE_XATTR,
                              vbid /*vbucket*/,
                              0 /*opaque*/,
                              0 /*cas*/,
                              itemMeta,
                              key,
                              xattrValue,
                              {},
                              options);
    EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, swm));

    if (isSet) {
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());
        checkGetItem(key, xattrValue, itemMeta);
    } else {
        EXPECT_TRUE(isDelete);
        protocol_binary_response_status expected =
                (options & SKIP_CONFLICT_RESOLUTION_FLAG) != 0
                        ? PROTOCOL_BINARY_RESPONSE_SUCCESS
                        : PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        // del fails as conflict resolution won't get to the XATTR test
        EXPECT_EQ(expected, getAddResponseStatus());
    }
}

// Using test data that should cause a conflict, run the conflict_lose test
// but with SKIP_CONFLICT_RESOLUTION_FLAG
TEST_F(WithMetaLwwTest, mutate_conflict_resolve_skipped) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 2. cas is equal, revSeq is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, PROTOCOL_BINARY_RESPONSE_SUCCESS};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(PROTOCOL_BINARY_CMD_SET_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS | SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_SETQ_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS | SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
}

// Using test data that should cause a conflict, run the conflict_lose test
// but with SKIP_CONFLICT_RESOLUTION_FLAG
TEST_F(WithMetaTest, mutate_conflict_resolve_skipped) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 2. cas is equal, revSeq is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, PROTOCOL_BINARY_RESPONSE_SUCCESS};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(PROTOCOL_BINARY_CMD_SET_WITH_META,
                 SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_SETQ_WITH_META,
                 SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
}

// Using test data that should cause a conflict, run the conflict_lose test
// but with SKIP_CONFLICT_RESOLUTION_FLAG
TEST_F(WithMetaLwwTest, del_conflict_resolve_skipped) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    // However deletes only check 1 and 2.

    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 2. cas is equal, revSeqno is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, PROTOCOL_BINARY_RESPONSE_SUCCESS};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(PROTOCOL_BINARY_CMD_DEL_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS | SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_DELQ_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS | SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
}

// Using test data that should cause a conflict, run the conflict_lose test
// but with SKIP_CONFLICT_RESOLUTION_FLAG
TEST_F(WithMetaTest, del_conflict_resolve_skipped) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    // However deletes only check 1 and 2.

    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 2. cas is equal, revSeqno is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1}, PROTOCOL_BINARY_RESPONSE_SUCCESS};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, PROTOCOL_BINARY_RESPONSE_SUCCESS};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(PROTOCOL_BINARY_CMD_DEL_WITH_META,
                 SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_DELQ_WITH_META,
                 SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
}

TEST_F(WithMetaTest, set_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {
            {{{100, 101, 100, expiry}, // ... mutate with higher seq
              PROTOCOL_BINARY_RESPONSE_SUCCESS},
             {{101, 100, 100, expiry}, // ... mutate with same but higher cas
              PROTOCOL_BINARY_RESPONSE_SUCCESS},
             {{100,
               100,
               100,
               expiry + 1}, // ... mutate with same but higher exp
              PROTOCOL_BINARY_RESPONSE_SUCCESS},
             {{100, 100, 101, expiry}, // ... mutate with same but higher flags
              PROTOCOL_BINARY_RESPONSE_SUCCESS}}};

    conflict_win(PROTOCOL_BINARY_CMD_SET_WITH_META, 0, data, itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_SETQ_WITH_META, 0, data, itemMeta);
}

TEST_F(WithMetaTest, del_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {{
            {{100, 101, 100, expiry}, // ... mutate with higher seq
             PROTOCOL_BINARY_RESPONSE_SUCCESS},
            {{101, 100, 100, expiry}, // ... mutate with same but higher cas
             PROTOCOL_BINARY_RESPONSE_SUCCESS},
            {{100, 100, 100, expiry + 1}, // ... mutate with same but higher exp
             PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS}, // delete ignores expiry
            {{100, 100, 101, expiry}, // ... mutate with same but higher flags
             PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS} // delete ignores flags
    }};

    conflict_win(PROTOCOL_BINARY_CMD_DEL_WITH_META, 0, data, itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_DELQ_WITH_META, 0, data, itemMeta);
}

TEST_F(WithMetaLwwTest, set_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {
            {{{101, 100, 100, expiry}, // ... mutate with higher cas
              PROTOCOL_BINARY_RESPONSE_SUCCESS},
             {{100, 101, 100, expiry}, // ... mutate with same but higher seq
              PROTOCOL_BINARY_RESPONSE_SUCCESS},
             {{100,
               100,
               100,
               expiry + 1}, // ... mutate with same but higher exp
              PROTOCOL_BINARY_RESPONSE_SUCCESS},
             {{100, 100, 101, expiry}, // ... mutate with same but higher flags
              PROTOCOL_BINARY_RESPONSE_SUCCESS}}};

    conflict_win(PROTOCOL_BINARY_CMD_SET_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS,
                 data,
                 itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_SETQ_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS,
                 data,
                 itemMeta);
}

TEST_F(WithMetaLwwTest, del_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {{
            {{101, 100, 100, expiry}, // ... mutate with higher cas
             PROTOCOL_BINARY_RESPONSE_SUCCESS},
            {{100, 101, 100, expiry}, // ... mutate with same but higher seq
             PROTOCOL_BINARY_RESPONSE_SUCCESS},
            {{100, 100, 100, expiry + 1}, // ... mutate with same but higher exp
             PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS}, // delete ignores expiry
            {{100, 100, 101, expiry}, // ... mutate with same but higher flags
             PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS} // delete ignores flags
    }};

    conflict_win(PROTOCOL_BINARY_CMD_DEL_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS,
                 data,
                 itemMeta);
    conflict_win(PROTOCOL_BINARY_CMD_DELQ_WITH_META,
                 FORCE_ACCEPT_WITH_META_OPS,
                 data,
                 itemMeta);
}

TEST_P(AllWithMetaTest, markJSON) {
    // Write a XATTR doc with JSON body, expect the doc to be marked as JSON
    auto value = createXattrValue(
            R"({"json":"yesplease"})");
    auto swm = buildWithMetaPacket(GetParam(),
                                   PROTOCOL_BINARY_DATATYPE_XATTR,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   {100, 100, 100, expiry},
                                   "json",
                                   value);
    EXPECT_EQ(ENGINE_SUCCESS, callEngine(GetParam(), swm));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, getAddResponseStatus());

    auto result = store->get({"json", DocNamespace::DefaultCollection},
                             vbid,
                             nullptr,
                             GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
    EXPECT_EQ(0,
              strncmp(value.data(),
                      result.item->getData(),
                      result.item->getNBytes()));
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
              result.item->getDataType());
}

// Test uses an XATTR body that has 1 system key (see createXattrValue)
TEST_P(SnappyWithMetaTest, xattrPruneUserKeysOnDelete1) {
    auto value = createXattrValue(
            R"({"json":"yesplease"})", true, GetParam());
    uint8_t snappy = GetParam() ? PROTOCOL_BINARY_DATATYPE_SNAPPY : 0;
    ItemMetaData itemMeta{1, 1, 0, expiry};
    std::string mykey = "mykey";
    DocKey key{mykey, DocNamespace::DefaultCollection};
    auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_SET_WITH_META,
                                   PROTOCOL_BINARY_DATATYPE_XATTR | snappy,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   mykey,
                                   value);

    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_SET_WITH_META, swm));
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    itemMeta.revSeqno++; // make delete succeed
    auto dwm = buildWithMeta(
            PROTOCOL_BINARY_CMD_DEL_WITH_META, itemMeta, mykey, {});
    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_DEL_WITH_META, dwm));

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    auto options = get_options_t(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto result = store->get(key, vbid, nullptr, options);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());

    // Now reconstruct a XATTR Blob and validate the user keys are gone
    // These code relies on knowing what createXattrValue generates.
    auto sz = cb::xattr::get_body_offset(
            {result.item->getData(), result.item->getNBytes()});

    cb::xattr::Blob blob({const_cast<char*>(result.item->getData()), sz});

    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_STREQ("{\"cas\":\"0xdeadbeefcafefeed\"}",
                 reinterpret_cast<char*>(blob.get("_sync").data()));

    auto itm = result.item.get();
    // The meta-data should match the delete_with_meta
    EXPECT_EQ(itemMeta.cas, itm->getCas());
    EXPECT_EQ(itemMeta.flags, itm->getFlags());
    EXPECT_EQ(itemMeta.revSeqno, itm->getRevSeqno());
    EXPECT_EQ(itemMeta.exptime, itm->getExptime());

}

// Test uses an XATTR body that has no system keys
TEST_P(XattrWithMetaTest, xattrPruneUserKeysOnDelete2) {
    auto value = createXattrValue(
            R"({"json":"yesplease"})", false, ::testing::get<0>(GetParam()));
    uint8_t snappy =
            ::testing::get<0>(GetParam()) ? PROTOCOL_BINARY_DATATYPE_SNAPPY : 0;

    ItemMetaData itemMeta{1, 1, 0, expiry};
    std::string mykey = "mykey";
    DocKey key{mykey, DocNamespace::DefaultCollection};
    auto swm = buildWithMetaPacket(PROTOCOL_BINARY_CMD_SET_WITH_META,
                                   PROTOCOL_BINARY_DATATYPE_XATTR | snappy,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   mykey,
                                   value);

    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_SET_WITH_META, swm));
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    itemMeta.revSeqno++; // make delete succeed
    auto dwm = buildWithMeta(
            PROTOCOL_BINARY_CMD_DEL_WITH_META, itemMeta, mykey, {});
    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(PROTOCOL_BINARY_CMD_DEL_WITH_META, dwm));

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    auto options = get_options_t(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto result = store->get(key, vbid, nullptr, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, result.getStatus());

    // Run the BGFetcher task
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    options = get_options_t(options & (~GET_DELETED_VALUE));

    // K/V is gone
    result = store->get(key, vbid, nullptr, options);
    EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus());
}

TEST_P(AllWithMetaTest, skipConflicts) {
    ItemMetaData itemMeta{1, 0, 0, expiry};
    auto swm = buildWithMetaPacket(GetParam(),
                                   0 /*datatype*/,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   "mykey",
                                   "myvalue");

    engine->public_enableTraffic(false);
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0,
                  true,
                  PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                  ENGINE_KEY_ENOENT);
}

// Perform a DeleteWithMeta with a deleteTime of 1, verify that time comes back
// after a fetch and getMeta
TEST_P(DelWithMetaTest, setting_deleteTime) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, 1};
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue,
                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              store->getMetaData({"mykey", DocNamespace::DefaultCollection},
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getMetaData({"mykey", DocNamespace::DefaultCollection},
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    EXPECT_EQ(itemMeta.exptime, metadata.exptime);
}

auto opcodeValues = ::testing::Values(PROTOCOL_BINARY_CMD_SET_WITH_META,
                                      PROTOCOL_BINARY_CMD_SETQ_WITH_META,
                                      PROTOCOL_BINARY_CMD_ADD_WITH_META,
                                      PROTOCOL_BINARY_CMD_ADDQ_WITH_META,
                                      PROTOCOL_BINARY_CMD_DEL_WITH_META,
                                      PROTOCOL_BINARY_CMD_DELQ_WITH_META);

auto addSetOpcodeValues = ::testing::Values(PROTOCOL_BINARY_CMD_SET_WITH_META,
                                            PROTOCOL_BINARY_CMD_SETQ_WITH_META,
                                            PROTOCOL_BINARY_CMD_ADD_WITH_META,
                                            PROTOCOL_BINARY_CMD_ADDQ_WITH_META);

auto deleteOpcodeValues = ::testing::Values(PROTOCOL_BINARY_CMD_DEL_WITH_META,
                                            PROTOCOL_BINARY_CMD_DELQ_WITH_META);

struct PrintToStringCombinedName {
    std::string
    operator()(const ::testing::TestParamInfo<
               ::testing::tuple<bool, protocol_binary_command>>& info) const {
        std::string rv = memcached_opcode_2_text(::testing::get<1>(info.param));
        if (::testing::get<0>(info.param)) {
            rv += "_with_value";
        }
        return rv;
    }
};

struct PrintToStringCombinedNameSnappyOnOff {
    std::string
    operator()(const ::testing::TestParamInfo<
               ::testing::tuple<bool, protocol_binary_command>>& info) const {
        std::string rv = memcached_opcode_2_text(::testing::get<1>(info.param));
        if (::testing::get<0>(info.param)) {
            rv += "_snappy";
        }
        return rv;
    }
};

struct PrintOpcode {
    std::string operator()(
            const ::testing::TestParamInfo<protocol_binary_command>& info)
            const {
        return memcached_opcode_2_text(info.param);
    }
};

struct PrintSnappyOnOff {
    std::string operator()(const ::testing::TestParamInfo<bool>& info) const {
        if (info.param) {
            return "snappy";
        }
        return "no_snappy";
    }
};

INSTANTIATE_TEST_CASE_P(DelWithMeta,
                        DelWithMetaTest,
                        ::testing::Combine(::testing::Bool(),
                                           deleteOpcodeValues),
                        PrintToStringCombinedName());

INSTANTIATE_TEST_CASE_P(DelWithMetaLww,
                        DelWithMetaLwwTest,
                        ::testing::Combine(::testing::Bool(),
                                           deleteOpcodeValues),
                        PrintToStringCombinedName());

INSTANTIATE_TEST_CASE_P(AddSetWithMeta,
                        AddSetWithMetaTest,
                        addSetOpcodeValues,
                        PrintOpcode());

INSTANTIATE_TEST_CASE_P(AddSetWithMetaLww,
                        AddSetWithMetaLwwTest,
                        addSetOpcodeValues,
                        PrintOpcode());

INSTANTIATE_TEST_CASE_P(AddSetDelMeta,
                        AllWithMetaTest,
                        opcodeValues,
                        PrintOpcode());

INSTANTIATE_TEST_CASE_P(SnappyWithMetaTest,
                        SnappyWithMetaTest,
                        ::testing::Bool(),
                        PrintSnappyOnOff());

INSTANTIATE_TEST_CASE_P(AddSetDelXattrMeta,
                        XattrWithMetaTest,
                        // Bool for snappy on/off
                        ::testing::Combine(::testing::Bool(), opcodeValues),
                        PrintToStringCombinedNameSnappyOnOff());
