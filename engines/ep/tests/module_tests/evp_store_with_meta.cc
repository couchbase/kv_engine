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
#include "ep_bucket.h"
#include "ep_time.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "kv_bucket.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/test_helpers.h"

#include <daemon/protocol/mcbp/engine_errc_2_mcbp.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

class WithMetaTest : public SingleThreadedEPBucketTest {
public:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "allow_del_with_meta_prune_user_data=true";
        SingleThreadedEPBucketTest::SetUp();
        store->setVBucketState(vbid, vbucket_state_active);
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
    std::vector<char> buildWithMeta(cb::mcbp::ClientOpcode op,
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
        auto& request = *reinterpret_cast<cb::mcbp::Request*>(wm.data());
        auto ext = request.getExtdata();
        using Extras = cb::mcbp::request::SetWithMetaPayload;
        auto* extra = const_cast<Extras*>(
                reinterpret_cast<const Extras*>(ext.data()));
        extra->setCas(cas);
        extra->setSeqno(revSeq);
        extra->setExpiration(exp);
        extra->setFlagsInNetworkByteOrder(flags);
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
    ENGINE_ERROR_CODE callEngine(cb::mcbp::ClientOpcode op,
                                 std::vector<char>& wm) {
        auto* req = reinterpret_cast<cb::mcbp::Request*>(wm.data());
        if (op == cb::mcbp::ClientOpcode::DelWithMeta ||
            op == cb::mcbp::ClientOpcode::DelqWithMeta) {
            return engine->deleteWithMeta(cookie, *req, this->addResponse);
        } else {
            return engine->setWithMeta(cookie, *req, this->addResponse);
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
        auto result = store->get({key, DocKeyEncodesCollectionId::No},
                                 vbid,
                                 cookie,
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

    void oneOp(cb::mcbp::ClientOpcode op,
               uint8_t datatype,
               ItemMetaData itemMeta,
               int options,
               cb::mcbp::Status expectedResponseStatus,
               const std::string& key,
               const std::string& value,
               const std::vector<char>& emd) {
        auto swm = buildWithMetaPacket(op,
                                       datatype,
                                       vbid,
                                       0 /*opaque*/,
                                       0 /*cas*/,
                                       itemMeta,
                                       key,
                                       value,
                                       emd,
                                       options);
        if (expectedResponseStatus == cb::mcbp::Status::NotMyVbucket) {
            EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, callEngine(op, swm));
        } else if (expectedResponseStatus == cb::mcbp::Status::Etmpfail) {
            EXPECT_EQ(ENGINE_TMPFAIL, callEngine(op, swm));
        } else if (expectedResponseStatus == cb::mcbp::Status::Einval) {
            EXPECT_EQ(ENGINE_EINVAL, callEngine(op, swm));
        } else if (expectedResponseStatus == cb::mcbp::Status::KeyEexists) {
            EXPECT_EQ(ENGINE_KEY_EEXISTS, callEngine(op, swm));
        } else {
            EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, swm));
            EXPECT_EQ(expectedResponseStatus, getAddResponseStatus());
        }
    }

    /**
     * Run one op and check the result
     */
    void oneOpAndCheck(cb::mcbp::ClientOpcode op,
                       ItemMetaData itemMeta,
                       int options,
                       bool withValue,
                       cb::mcbp::Status expectedResponseStatus,
                       ENGINE_ERROR_CODE expectedGetReturnValue,
                       const std::vector<char>& emd = {}) {
        std::string key = "mykey";
        std::string value;
        if (withValue) {
            value = createXattrValue("myvalue"); // xattr but stored as raw
        }
        oneOp(op,
              withValue ? PROTOCOL_BINARY_DATATYPE_XATTR
                        : PROTOCOL_BINARY_RAW_BYTES,
              itemMeta,
              options,
              expectedResponseStatus,
              key,
              value,
              emd);
        checkGetItem(key, value, itemMeta, expectedGetReturnValue);
    }

    // *_with_meta with winning mutations
    struct TestData {
        ItemMetaData meta;
        cb::mcbp::Status expectedStatus;
    };

    /**
     * The conflict_win test is reused by seqno/lww and is intended to
     * test each winning op/meta input
     */
    void conflict_win(cb::mcbp::ClientOpcode op,
                      int options,
                      const std::array<TestData, 4>& testData,
                      const ItemMetaData& itemMeta);
    /**
     * The conflict_lose test is reused by seqno/lww and is intended to
     * test each winning op/meta input
     */
    void conflict_lose(cb::mcbp::ClientOpcode op,
                       int options,
                       bool withValue,
                       const std::array<TestData, 4>& testData,
                       const ItemMetaData& itemMeta);

    /**
     * The conflict_del_lose_xattr test demonstrates how a delete never gets
     * to compare xattrs when in conflict.
     */
    void conflict_del_lose_xattr(cb::mcbp::ClientOpcode op,
                                 int options,
                                 bool withValue);

    /**
     * The conflict_lose_xattr test demonstrates how a set gets
     * to compare xattrs when in conflict, and the server doc would win.
     */
    void conflict_lose_xattr(cb::mcbp::ClientOpcode op,
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
              ::testing::tuple<bool, cb::mcbp::ClientOpcode>> {
public:
    void SetUp() override {
        withValue = ::testing::get<0>(GetParam());
        op = ::testing::get<1>(GetParam());
        WithMetaTest::SetUp();
    }

    cb::mcbp::ClientOpcode op;
    bool withValue;
};

class DelWithMetaLwwTest
    : public WithMetaTest,
      public ::testing::WithParamInterface<
              ::testing::tuple<bool, cb::mcbp::ClientOpcode>> {
public:
    void SetUp() override {
        withValue = ::testing::get<0>(GetParam());
        op = ::testing::get<1>(GetParam());
        enableLww();
        WithMetaTest::SetUp();
    }

    cb::mcbp::ClientOpcode op;
    bool withValue;
};

class AllWithMetaTest
    : public WithMetaTest,
      public ::testing::WithParamInterface<cb::mcbp::ClientOpcode> {};

class AddSetWithMetaTest
    : public WithMetaTest,
      public ::testing::WithParamInterface<cb::mcbp::ClientOpcode> {};

class AddSetWithMetaLwwTest
    : public WithMetaTest,
      public ::testing::WithParamInterface<cb::mcbp::ClientOpcode> {
public:
    void SetUp() override {
        enableLww();
        WithMetaTest::SetUp();
    }
};

class XattrWithMetaTest
    : public WithMetaTest,
      public ::testing::WithParamInterface<
              ::testing::tuple<bool, cb::mcbp::ClientOpcode>> {};

class SnappyWithMetaTest : public WithMetaTest,
                           public ::testing::WithParamInterface<bool> {};

TEST_P(AddSetWithMetaTest, basic) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  cb::mcbp::Status::Success,
                  ENGINE_SUCCESS);
}

TEST_F(WithMetaTest, basicAdd) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    oneOpAndCheck(cb::mcbp::ClientOpcode::AddWithMeta,
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  cb::mcbp::Status::Success,
                  ENGINE_SUCCESS);

    oneOpAndCheck(cb::mcbp::ClientOpcode::AddWithMeta,
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  cb::mcbp::Status::KeyEexists, // can't do a second add
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
                  cb::mcbp::Status::Success,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);
}

TEST_P(AllWithMetaTest, invalidCas) {
    // 0 CAS in the item meta is invalid
    ItemMetaData itemMeta{0 /*cas*/, 0, 0, 0};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  cb::mcbp::Status::KeyEexists,
                  ENGINE_KEY_ENOENT);

    // -1 CAS in the item meta is invalid
    itemMeta.cas = ~0ull;
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0, // no-options
                  true /*set a value*/,
                  cb::mcbp::Status::KeyEexists,
                  ENGINE_KEY_ENOENT);
}

TEST_P(DelWithMetaTest, invalidCas) {
    // 0 CAS in the item meta is invalid
    ItemMetaData itemMeta{0 /*cas*/, 0, 0, 0};
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue /*set a value*/,
                  cb::mcbp::Status::KeyEexists,
                  ENGINE_KEY_ENOENT);

    // -1 CAS in the item meta is invalid
    itemMeta.cas = ~0ull;
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue /*set a value*/,
                  cb::mcbp::Status::KeyEexists,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AllWithMetaTest, failForceAccept) {
    // FORCE_ACCEPT_WITH_META_OPS not allowed unless we're LWW
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_ACCEPT_WITH_META_OPS,
                  true /*set a value*/,
                  cb::mcbp::Status::Einval,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AddSetWithMetaLwwTest, allowForceAccept) {
    // FORCE_ACCEPT_WITH_META_OPS ok on LWW
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_ACCEPT_WITH_META_OPS,
                  true /*set a value*/,
                  cb::mcbp::Status::Success,
                  ENGINE_SUCCESS);
}

TEST_P(DelWithMetaLwwTest, allowForceAccept) {
    // FORCE_ACCEPT_WITH_META_OPS ok on LWW
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(op,
                  itemMeta,
                  FORCE_ACCEPT_WITH_META_OPS,
                  withValue,
                  cb::mcbp::Status::Success,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);
}

TEST_P(AllWithMetaTest, regenerateCASInvalid) {
    // REGENERATE_CAS cannot be by itself
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  REGENERATE_CAS,
                  true,
                  cb::mcbp::Status::Einval,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AllWithMetaTest, forceFail) {
    store->setVBucketState(vbid, vbucket_state_replica);
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0 /*no options*/,
                  true,
                  cb::mcbp::Status::NotMyVbucket,
                  ENGINE_KEY_ENOENT);
}

TEST_P(AllWithMetaTest, forceSuccessReplica) {
    store->setVBucketState(vbid, vbucket_state_replica);
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_WITH_META_OP,
                  true,
                  cb::mcbp::Status::Success,
                  ENGINE_SUCCESS);
}

TEST_P(AllWithMetaTest, forceSuccessPending) {
    store->setVBucketState(vbid, vbucket_state_pending);
    ItemMetaData itemMeta{1, 0, 0, expiry};
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  FORCE_WITH_META_OP,
                  true,
                  cb::mcbp::Status::Success,
                  ENGINE_SUCCESS);
}

TEST_P(AllWithMetaTest, regenerateCAS) {
    // Test that
    uint64_t cas = 1;
    auto swm =
            buildWithMetaPacket(GetParam(),
                                PROTOCOL_BINARY_DATATYPE_XATTR,
                                vbid /*vbucket*/,
                                0 /*opaque*/,
                                0 /*cas*/,
                                {cas, 0, 0, 0},
                                "mykey",
                                createXattrValue("myvalue", true),
                                {},
                                SKIP_CONFLICT_RESOLUTION_FLAG | REGENERATE_CAS);

    EXPECT_EQ(ENGINE_SUCCESS, callEngine(GetParam(), swm));
    EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());
    auto result = store->get({"mykey", DocKeyEncodesCollectionId::No},
                             vbid,
                             cookie,
                             GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
    EXPECT_NE(cas, result.item->getCas()) << "CAS didn't change";
}

// Test to verify that a set with meta will store the data
// as uncompressed in the hash table
TEST_F(WithMetaTest, storeUncompressedInOffMode) {

    // Return if the bucket compression mode is not 'off'
    if (engine->getCompressionMode() != BucketCompressionMode::Off) {
         return;
    }
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

    auto swm =
            buildWithMetaPacket(cb::mcbp::ClientOpcode::SetWithMeta,
                                item->getDataType() /*datatype*/,
                                vbid /*vbucket*/,
                                0 /*opaque*/,
                                0 /*cas*/,
                                itemMeta,
                                std::string("key"),
                                std::string(item->getData(), item->getNBytes()),
                                {},
                                SKIP_CONFLICT_RESOLUTION_FLAG);
    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(cb::mcbp::ClientOpcode::SetWithMeta, swm));

    VBucketPtr vb = store->getVBucket(vbid);
    const auto* v(vb->ht.findForRead(makeStoredDocKey("key")).storedValue);
    ASSERT_NE(nullptr, v);
    EXPECT_EQ(valueData, v->getValue()->to_s());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, v->getDatatype());
}

TEST_P(AllWithMetaTest, nmvb) {
    std::string key = "mykey";
    std::string value = "myvalue";
    auto swm = buildWithMetaPacket(GetParam(),
                                   0 /*datatype*/,
                                   Vbid(vbid.get() + 1),
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   {1, 0, 0, 0},
                                   key,
                                   value);
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, callEngine(GetParam(), swm));

    // Set a dead VB
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(Vbid(vbid.get() + 1), vbucket_state_dead));
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, callEngine(GetParam(), swm));

    // update the VB in the packet to the pending one
    auto packet = reinterpret_cast<protocol_binary_request_header*>(swm.data());
    packet->request.setVBucket(Vbid(vbid.get() + 2));
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(Vbid(vbid.get() + 2),
                                     vbucket_state_pending));
    EXPECT_EQ(ENGINE_EWOULDBLOCK, callEngine(GetParam(), swm));
    EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());

    // Re-run the op now active, else we have a memory leak
    EXPECT_EQ(
            ENGINE_SUCCESS,
            store->setVBucketState(Vbid(vbid.get() + 2), vbucket_state_active));
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
                  cb::mcbp::Status::Etmpfail,
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
                  cb::mcbp::Status::Etmpfail,
                  ENGINE_KEY_ENOENT);
}

void WithMetaTest::conflict_lose(cb::mcbp::ClientOpcode op,
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
    auto swm =
            buildWithMetaPacket(cb::mcbp::ClientOpcode::AddWithMeta,
                                value.empty() ? PROTOCOL_BINARY_RAW_BYTES
                                              : PROTOCOL_BINARY_DATATYPE_XATTR,
                                vbid /*vbucket*/,
                                0 /*opaque*/,
                                0 /*cas*/,
                                itemMeta,
                                key,
                                value,
                                {},
                                options);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(cb::mcbp::ClientOpcode::AddWithMeta, swm));
    EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());

    for (const auto& td : testData) {
        oneOp(op,
              withValue ? PROTOCOL_BINARY_DATATYPE_XATTR
                        : PROTOCOL_BINARY_RAW_BYTES,
              td.meta,
              options,
              td.expectedStatus,
              key,
              value,
              {});
    }
}

// store a document then <op>_with_meta with equal ItemMeta but xattr on
void WithMetaTest::conflict_del_lose_xattr(cb::mcbp::ClientOpcode op,
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
    auto swm = buildWithMetaPacket(cb::mcbp::ClientOpcode::AddWithMeta,
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
              callEngine(cb::mcbp::ClientOpcode::AddWithMeta, swm));
    EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());

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
    EXPECT_EQ(ENGINE_KEY_EEXISTS, callEngine(op, swm));
}

void WithMetaTest::conflict_lose_xattr(cb::mcbp::ClientOpcode op,
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
    auto swm = buildWithMetaPacket(cb::mcbp::ClientOpcode::AddWithMeta,
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
              callEngine(cb::mcbp::ClientOpcode::AddWithMeta, swm));
    EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());

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
    EXPECT_EQ(ENGINE_KEY_EEXISTS, callEngine(op, swm));
}

TEST_P(DelWithMetaTest, conflict_lose) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) seqno 2) cas 3) expiry 4) flags 5) xattr
    // However deletes only check 1 and 2.

    std::array<TestData, 4> data;

    // 1) revSeqno is less and everything else larger. Expect conflict
    data[0] = {{101, 99, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 2. revSeqno is equal, cas is less, others are larger. Expect conflict
    data[1] = {{99, 100, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, cb::mcbp::Status::KeyEexists};

    conflict_lose(op, 0, withValue, data, itemMeta);
}

TEST_P(DelWithMetaLwwTest, conflict_lose) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    // However deletes only check 1 and 2.

    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 2. cas is equal, revSeqno is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, cb::mcbp::Status::KeyEexists};

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
    data[0] = {{101, 99, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 2. revSeqno is equal, cas is less, others are larger. Expect conflict
    data[1] = {{99, 100, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1}, cb::mcbp::Status::KeyEexists};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, cb::mcbp::Status::KeyEexists};

    conflict_lose(
            GetParam(), 0 /*options*/, true /*withValue*/, data, itemMeta);
}

TEST_P(AddSetWithMetaLwwTest, conflict_lose) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};

    // Conflict test order: 1) cas 2) seqno 3) expiry 4) flags 5) xattr
    std::array<TestData, 4> data;
    // 1) cas is less and everything else larger. Expect conflict
    data[0] = {{99, 101, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 2. cas is equal, revSeq is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, cb::mcbp::Status::KeyEexists};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1}, cb::mcbp::Status::KeyEexists};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, cb::mcbp::Status::KeyEexists};

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
void WithMetaTest::conflict_win(cb::mcbp::ClientOpcode op,
                                int options,
                                const std::array<TestData, 4>& testData,
                                const ItemMetaData& itemMeta) {
    EXPECT_NE(op, cb::mcbp::ClientOpcode::AddWithMeta);
    EXPECT_NE(op, cb::mcbp::ClientOpcode::AddqWithMeta);
    bool isDelete = op == cb::mcbp::ClientOpcode::DelWithMeta ||
                    op == cb::mcbp::ClientOpcode::DelqWithMeta;
    bool isSet = op == cb::mcbp::ClientOpcode::SetWithMeta ||
                 op == cb::mcbp::ClientOpcode::SetqWithMeta;

    int counter = 0;
    for (auto& td : testData) {
        // Set our "target" (new key each iteration) and the op for test
        // uniqueness
        std::string key = "mykey" + std::to_string(counter) + to_string(op);
        std::string value =
                createXattrValue("newvalue" + std::to_string(counter), true);
        auto swm = buildWithMetaPacket(cb::mcbp::ClientOpcode::SetWithMeta,
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
                  callEngine(cb::mcbp::ClientOpcode::SetWithMeta, swm));
        EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus())
                << "Failed to set the target key:" << key;

        // Next the test packet (always with a value).
        auto wm = buildWithMetaPacket(op,
                                      PROTOCOL_BINARY_DATATYPE_XATTR,
                                      vbid /*vbucket*/,
                                      0 /*opaque*/,
                                      0 /*cas*/,
                                      td.meta,
                                      key,
                                      value,
                                      {},
                                      options);

        // Now set/del against the item using the test iteration metadata
        cb::mcbp::Status status;
        if (td.expectedStatus == cb::mcbp::Status::Success) {
            EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, wm));
            status = getAddResponseStatus();
        } else {
            auto error = callEngine(op, wm);
            status = cb::mcbp::to_status(cb::engine_errc(error));
        }

        if (isDelete) {
            EXPECT_EQ(td.expectedStatus, status)
                    << "Failed deleteWithMeta for iteration " << counter;
            if (status == cb::mcbp::Status::Success) {
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

    // ... Finally give an Item with a datatype (not xattr) and the op for test
    // uniqueness
    std::string key = "mykey" + std::to_string(counter) + to_string(op);
    ;
    auto swm = buildWithMetaPacket(cb::mcbp::ClientOpcode::AddWithMeta,
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
              callEngine(cb::mcbp::ClientOpcode::AddWithMeta, swm));
    EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());

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

    if (isSet) {
        EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, swm));
        EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());
        checkGetItem(key, xattrValue, itemMeta);
    } else {
        EXPECT_TRUE(isDelete);
        if ((options & SKIP_CONFLICT_RESOLUTION_FLAG) != 0) {
            // del fails as conflict resolution won't get to the XATTR test
            EXPECT_EQ(ENGINE_SUCCESS, callEngine(op, swm));
            EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());
        } else {
            EXPECT_EQ(ENGINE_KEY_EEXISTS, callEngine(op, swm));
        }
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
    data[0] = {{99, 101, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 2. cas is equal, revSeq is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1}, cb::mcbp::Status::Success};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, cb::mcbp::Status::Success};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(cb::mcbp::ClientOpcode::SetWithMeta,
                 FORCE_ACCEPT_WITH_META_OPS | SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::SetqWithMeta,
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
    data[0] = {{99, 101, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 2. cas is equal, revSeq is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 3. revSeqno/cas equal, flags larger, exp less, conflict
    data[2] = {{100, 100, 101, expiry - 1}, cb::mcbp::Status::Success};
    // 4. revSeqno/cas/exp equal, flags less, conflict
    data[3] = {{100, 100, 99, expiry}, cb::mcbp::Status::Success};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(cb::mcbp::ClientOpcode::SetWithMeta,
                 SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::SetqWithMeta,
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
    data[0] = {{99, 101, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 2. cas is equal, revSeqno is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1}, cb::mcbp::Status::Success};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, cb::mcbp::Status::Success};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(cb::mcbp::ClientOpcode::DelWithMeta,
                 FORCE_ACCEPT_WITH_META_OPS | SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::DelqWithMeta,
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
    data[0] = {{99, 101, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 2. cas is equal, revSeqno is less, others are larger. Expect conflict
    data[1] = {{100, 99, 101, expiry + 1}, cb::mcbp::Status::Success};
    // 3. revSeqno/cas/flags equal, exp larger. Conflict as exp not checked
    data[2] = {{100, 100, 100, expiry + 1}, cb::mcbp::Status::Success};
    // 4. revSeqno/cas/exp equal, flags larger. Conflict as exp not checked
    data[3] = {{100, 100, 200, expiry}, cb::mcbp::Status::Success};

    // Run with SKIP_CONFLICT_RESOLUTION_FLAG
    conflict_win(cb::mcbp::ClientOpcode::DelWithMeta,
                 SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::DelqWithMeta,
                 SKIP_CONFLICT_RESOLUTION_FLAG,
                 data,
                 itemMeta);
}

TEST_F(WithMetaTest, set_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {
            {{{100, 101, 100, expiry}, // ... mutate with higher seq
              cb::mcbp::Status::Success},
             {{101, 100, 100, expiry}, // ... mutate with same but higher cas
              cb::mcbp::Status::Success},
             {{100, 100, 100, expiry + 1}, // ... mutate with same but higher
                                           // exp
              cb::mcbp::Status::Success},
             {{100, 100, 101, expiry}, // ... mutate with same but higher flags
              cb::mcbp::Status::Success}}};

    conflict_win(cb::mcbp::ClientOpcode::SetWithMeta, 0, data, itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::SetqWithMeta, 0, data, itemMeta);
}

TEST_F(WithMetaTest, del_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {{
            {{100, 101, 100, expiry}, // ... mutate with higher seq
             cb::mcbp::Status::Success},
            {{101, 100, 100, expiry}, // ... mutate with same but higher cas
             cb::mcbp::Status::Success},
            {{100, 100, 100, expiry + 1}, // ... mutate with same but higher exp
             cb::mcbp::Status::KeyEexists}, // delete ignores expiry
            {{100, 100, 101, expiry}, // ... mutate with same but higher flags
             cb::mcbp::Status::KeyEexists} // delete ignores flags
    }};

    conflict_win(cb::mcbp::ClientOpcode::DelWithMeta, 0, data, itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::DelqWithMeta, 0, data, itemMeta);
}

TEST_F(WithMetaLwwTest, set_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {
            {{{101, 100, 100, expiry}, // ... mutate with higher cas
              cb::mcbp::Status::Success},
             {{100, 101, 100, expiry}, // ... mutate with same but higher seq
              cb::mcbp::Status::Success},
             {{100, 100, 100, expiry + 1}, // ... mutate with same but higher
                                           // exp
              cb::mcbp::Status::Success},
             {{100, 100, 101, expiry}, // ... mutate with same but higher flags
              cb::mcbp::Status::Success}}};

    conflict_win(cb::mcbp::ClientOpcode::SetWithMeta,
                 FORCE_ACCEPT_WITH_META_OPS,
                 data,
                 itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::SetqWithMeta,
                 FORCE_ACCEPT_WITH_META_OPS,
                 data,
                 itemMeta);
}

TEST_F(WithMetaLwwTest, del_conflict_win) {
    ItemMetaData itemMeta{
            100 /*cas*/, 100 /*revSeq*/, 100 /*flags*/, expiry /*expiry*/};
    std::array<TestData, 4> data = {{
            {{101, 100, 100, expiry}, // ... mutate with higher cas
             cb::mcbp::Status::Success},
            {{100, 101, 100, expiry}, // ... mutate with same but higher seq
             cb::mcbp::Status::Success},
            {{100, 100, 100, expiry + 1}, // ... mutate with same but higher exp
             cb::mcbp::Status::KeyEexists}, // delete ignores expiry
            {{100, 100, 101, expiry}, // ... mutate with same but higher flags
             cb::mcbp::Status::KeyEexists} // delete ignores flags
    }};

    conflict_win(cb::mcbp::ClientOpcode::DelWithMeta,
                 FORCE_ACCEPT_WITH_META_OPS,
                 data,
                 itemMeta);
    conflict_win(cb::mcbp::ClientOpcode::DelqWithMeta,
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
    EXPECT_EQ(cb::mcbp::Status::Success, getAddResponseStatus());

    auto result = store->get({"json", DocKeyEncodesCollectionId::No},
                             vbid,
                             cookie,
                             GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
    EXPECT_EQ(0,
              strncmp(value.data(),
                      result.item->getData(),
                      result.item->getNBytes()));
    if (GetParam() == cb::mcbp::ClientOpcode::DelWithMeta ||
        GetParam() == cb::mcbp::ClientOpcode::DelqWithMeta) {
        // Delete strips off the user value and user xattrs, but leaves the
        // system XATTR behind
        EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, result.item->getDataType());
    } else {
        EXPECT_EQ(
                PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
                result.item->getDataType());
    }
}

// Test uses an XATTR body that has 1 system key (see createXattrValue)
TEST_P(SnappyWithMetaTest, xattrPruneUserKeysOnDelete1) {
    auto value = createXattrValue(
            R"({"json":"yesplease"})", true, GetParam());
    uint8_t snappy = GetParam() ? PROTOCOL_BINARY_DATATYPE_SNAPPY : 0;
    ItemMetaData itemMeta{1, 1, 0, expiry};
    std::string mykey = "mykey";
    DocKey key{mykey, DocKeyEncodesCollectionId::No};
    auto swm = buildWithMetaPacket(cb::mcbp::ClientOpcode::SetWithMeta,
                                   PROTOCOL_BINARY_DATATYPE_XATTR | snappy,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   mykey,
                                   value);

    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(cb::mcbp::ClientOpcode::SetWithMeta, swm));
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    itemMeta.revSeqno++; // make delete succeed
    auto dwm = buildWithMeta(
            cb::mcbp::ClientOpcode::DelWithMeta, itemMeta, mykey, {});
    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(cb::mcbp::ClientOpcode::DelWithMeta, dwm));

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    auto options = get_options_t(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto result = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, result.getStatus());
    runBGFetcherTask();

    result = store->get(key, vbid, cookie, options);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());

    // Now reconstruct a XATTR Blob and validate the user keys are gone
    // These code relies on knowing what createXattrValue generates.
    auto sz = cb::xattr::get_body_offset(
            {result.item->getData(), result.item->getNBytes()});

    cb::xattr::Blob blob({const_cast<char*>(result.item->getData()), sz},
                         false);

    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_STREQ("{\"cas\":\"0xdeadbeefcafefeed\"}",
                 reinterpret_cast<char*>(blob.get("_sync").data()));

    auto itm = result.item.get();
    EXPECT_TRUE(itm->isDeleted()) << "Not deleted " << *itm;
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
    DocKey key{mykey, DocKeyEncodesCollectionId::No};
    auto swm = buildWithMetaPacket(cb::mcbp::ClientOpcode::SetWithMeta,
                                   PROTOCOL_BINARY_DATATYPE_XATTR | snappy,
                                   vbid /*vbucket*/,
                                   0 /*opaque*/,
                                   0 /*cas*/,
                                   itemMeta,
                                   mykey,
                                   value);

    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);

    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(cb::mcbp::ClientOpcode::SetWithMeta, swm));
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    itemMeta.revSeqno++; // make delete succeed
    auto dwm = buildWithMeta(
            cb::mcbp::ClientOpcode::DelWithMeta, itemMeta, mykey, {});
    EXPECT_EQ(ENGINE_SUCCESS,
              callEngine(cb::mcbp::ClientOpcode::DelWithMeta, dwm));

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    auto options = get_options_t(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto result = store->get(key, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, result.getStatus());

    runBGFetcherTask();

    options = get_options_t(options & (~GET_DELETED_VALUE));

    // K/V is gone
    result = store->get(key, vbid, cookie, options);
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
                  cb::mcbp::Status::Etmpfail,
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
                  cb::mcbp::Status::Success,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              store->getMetaData({"mykey", DocKeyEncodesCollectionId::No},
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    runBGFetcherTask();
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getMetaData({"mykey", DocKeyEncodesCollectionId::No},
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    EXPECT_EQ(itemMeta.exptime, metadata.exptime);
}

// Perform a DeleteWithMeta with a deleteTime of 0 and verify that a time is
// generated.
TEST_P(DelWithMetaTest, setting_zero_deleteTime) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, 0};
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue,
                  cb::mcbp::Status::Success,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              store->getMetaData({"mykey", DocKeyEncodesCollectionId::No},
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getMetaData({"mykey", DocKeyEncodesCollectionId::No},
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    EXPECT_NE(0, metadata.exptime);
}

TEST_P(DelWithMetaTest, MB_31141) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    // Do a delete with valid extended meta
    // see - ep-engine/docs/protocol/del_with_meta.md
    oneOpAndCheck(op,
                  itemMeta,
                  0, // no-options
                  withValue,
                  cb::mcbp::Status::Success,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK,
                  {0x01, 0x01, 0x00, 0x01, 0x01});

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    auto options = get_options_t(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto result = store->get(
            {"mykey", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, result.getStatus());

    runBGFetcherTask();

    result = store->get(
            {"mykey", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());

    // Before the fix 5.0+ could of left the value as 5, the size of the
    // extended metadata. (The expected size is the size of the system
    // xattrs segment. The rest of the value (user defined xattrs and value)
    // was stripped off as part of delete)
    int expectedSize = withValue ? 43 : 0;
    EXPECT_EQ(expectedSize, result.item->getNBytes());
}

TEST_P(AddSetWithMetaTest, MB_31141) {
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    // Do a set/add with valid extended meta
    // see - ep-engine/docs/protocol/del_with_meta.md
    oneOpAndCheck(GetParam(),
                  itemMeta,
                  0, // no-options
                  true,
                  cb::mcbp::Status::Success,
                  ENGINE_SUCCESS,
                  {0x01, 0x01, 0x00, 0x01, 0x01});

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    auto options = get_options_t(QUEUE_BG_FETCH);
    auto result = store->get(
            {"mykey", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
    // Only the value should come back
    EXPECT_EQ(275, result.item->getNBytes());
}

TEST_P(DelWithMetaTest, isExpirationOption) {
    // Trigger the basic DelWithMetaTest with the IS_EXPIRATION option
    ItemMetaData itemMeta{0xdeadbeef, 0xf00dcafe, 0xfacefeed, expiry};
    // A delete_w_meta against an empty bucket queues a BGFetch (get = ewblock)
    // A delete_w_meta(with_value) sets the new value (get = success)
    oneOpAndCheck(op,
                  itemMeta,
                  IS_EXPIRATION,
                  withValue,
                  cb::mcbp::Status::Success,
                  withValue ? ENGINE_SUCCESS : ENGINE_EWOULDBLOCK);
}

auto opcodeValues = ::testing::Values(cb::mcbp::ClientOpcode::SetWithMeta,
                                      cb::mcbp::ClientOpcode::SetqWithMeta,
                                      cb::mcbp::ClientOpcode::AddWithMeta,
                                      cb::mcbp::ClientOpcode::AddqWithMeta,
                                      cb::mcbp::ClientOpcode::DelWithMeta,
                                      cb::mcbp::ClientOpcode::DelqWithMeta);

auto addSetOpcodeValues =
        ::testing::Values(cb::mcbp::ClientOpcode::SetWithMeta,
                          cb::mcbp::ClientOpcode::SetqWithMeta,
                          cb::mcbp::ClientOpcode::AddWithMeta,
                          cb::mcbp::ClientOpcode::AddqWithMeta);

auto deleteOpcodeValues =
        ::testing::Values(cb::mcbp::ClientOpcode::DelWithMeta,
                          cb::mcbp::ClientOpcode::DelqWithMeta);

struct PrintToStringCombinedName {
    std::string
    operator()(const ::testing::TestParamInfo<
               ::testing::tuple<bool, cb::mcbp::ClientOpcode>>& info) const {
        std::string rv = to_string(
                cb::mcbp::ClientOpcode(::testing::get<1>(info.param)));
        if (::testing::get<0>(info.param)) {
            rv += "_with_value";
        }
        return rv;
    }
};

struct PrintToStringCombinedNameSnappyOnOff {
    std::string
    operator()(const ::testing::TestParamInfo<
               ::testing::tuple<bool, cb::mcbp::ClientOpcode>>& info) const {
        std::string rv = to_string(
                cb::mcbp::ClientOpcode(::testing::get<1>(info.param)));
        if (::testing::get<0>(info.param)) {
            rv += "_snappy";
        }
        return rv;
    }
};

struct PrintOpcode {
    std::string operator()(
            const ::testing::TestParamInfo<cb::mcbp::ClientOpcode>& info)
            const {
        return to_string(cb::mcbp::ClientOpcode(info.param));
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

INSTANTIATE_TEST_SUITE_P(DelWithMeta,
                         DelWithMetaTest,
                         ::testing::Combine(::testing::Bool(),
                                            deleteOpcodeValues),
                         PrintToStringCombinedName());

INSTANTIATE_TEST_SUITE_P(DelWithMetaLww,
                         DelWithMetaLwwTest,
                         ::testing::Combine(::testing::Bool(),
                                            deleteOpcodeValues),
                         PrintToStringCombinedName());

INSTANTIATE_TEST_SUITE_P(AddSetWithMeta,
                         AddSetWithMetaTest,
                         addSetOpcodeValues,
                         PrintOpcode());

INSTANTIATE_TEST_SUITE_P(AddSetWithMetaLww,
                         AddSetWithMetaLwwTest,
                         addSetOpcodeValues,
                         PrintOpcode());

INSTANTIATE_TEST_SUITE_P(AddSetDelMeta,
                         AllWithMetaTest,
                         opcodeValues,
                         PrintOpcode());

INSTANTIATE_TEST_SUITE_P(SnappyWithMetaTest,
                         SnappyWithMetaTest,
                         ::testing::Bool(),
                         PrintSnappyOnOff());

INSTANTIATE_TEST_SUITE_P(AddSetDelXattrMeta,
                         XattrWithMetaTest,
                         // Bool for snappy on/off
                         ::testing::Combine(::testing::Bool(), opcodeValues),
                         PrintToStringCombinedNameSnappyOnOff());
