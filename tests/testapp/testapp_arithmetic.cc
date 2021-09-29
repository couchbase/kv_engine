/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"
#include <protocol/mcbp/ewb_encode.h>
#include <cctype>
#include <limits>
#include <thread>

class ArithmeticTest : public TestappXattrClientTest {};

/**
 * Test fixture for arithmetic tests which always need XATTR support on.
 */
class ArithmeticXattrOnTest : public TestappXattrClientTest {};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        ArithmeticTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

// Tests which always need XATTR support (hence only instantiated with
// XattrSupport::Yes).
INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        ArithmeticXattrOnTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

TEST_P(ArithmeticTest, TestArithmeticNoCreateOnNotFound) {
    try {
        userConnection->increment(name, 1, 0, 0xffffffff);
        FAIL() << "Document should not be created";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.getReason();
    }

    try {
        userConnection->decrement(name, 1, 0, 0xffffffff);
        FAIL() << "Document should not be created";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.getReason();
    }
}

TEST_P(ArithmeticTest, TestIncrementCreateOnNotFound) {
    EXPECT_EQ(0, userConnection->increment(name + "_incr", 1, 0));
    EXPECT_EQ(0, userConnection->increment(name + "_decr", 1, 0));
}

static void incr_decr_loop(MemcachedConnection& connection,
                           const std::string& key,
                           int delta) {

    uint64_t expected = 0;

    for (int ii = 0; ii < 101; ++ii) {
        EXPECT_EQ(expected, connection.arithmetic(key, delta));
        expected += delta;
    }
    // (remove the last delta, it was not added to the one on the server
    expected -= delta;

    for (int ii = 0; ii < 100; ++ii) {
        expected -= delta;
        EXPECT_EQ(expected, connection.arithmetic(key, delta * -1));
    }

    // Verify that we ended back at 0 by trying to add 0 ;-)
    EXPECT_EQ(0, connection.arithmetic(key, 0));
}

TEST_P(ArithmeticTest, TestBasicArithmetic_1) {
    incr_decr_loop(*userConnection, name, 1);
}

TEST_P(ArithmeticTest, TestBasicArithmetic_33) {
    incr_decr_loop(*userConnection, name, 33);
}

TEST_P(ArithmeticTest, TestDecrementDontWrap) {
    for (int ii = 0; ii < 10; ++ii) {
        EXPECT_EQ(0, userConnection->decrement(name, 1));
    }
}

TEST_P(ArithmeticTest, TestIncrementDoesWrap) {
    uint64_t initial = std::numeric_limits<uint64_t>::max();

    // Create the initial value so that we know where we should start off ;)
    EXPECT_EQ(0, userConnection->increment(name, 1));

    EXPECT_EQ(initial, userConnection->increment(name, initial));
    EXPECT_EQ(0, userConnection->increment(name, 1));
    EXPECT_EQ(initial, userConnection->increment(name, initial));
    EXPECT_EQ(0, userConnection->increment(name, 1));
}

TEST_P(ArithmeticTest, TestConcurrentAccess) {
    auto conn1 = userConnection->clone();
    auto conn2 = userConnection->clone();
    const int iterationCount = 100;
    const int incrDelta = 7;
    const int decrDelta = -3;

    conn1->authenticate("Luke", mcd_env->getPassword("Luke"));
    conn1->selectBucket(bucketName);
    conn2->authenticate("Luke", mcd_env->getPassword("Luke"));
    conn2->selectBucket(bucketName);

    // Create the starting point
    uint64_t expected = std::numeric_limits<uint32_t>::max();
    ASSERT_EQ(0, userConnection->increment(name, 0));
    ASSERT_EQ(expected, userConnection->increment(name, expected));

    std::string doc = name;

    std::thread t1 {
        [&conn1, &doc, &iterationCount, incrDelta]() {
            conn1->arithmetic(doc + "_t1", 0, 0);

            // wait for the other thread to start
            bool running = false;
            while (!running) {
                try {
                    conn1->arithmetic(doc + "_t2", 1, 0, 0xffffffff);
                    running = true;
                } catch (const ConnectionError& error) {
                    ASSERT_TRUE(error.isNotFound());
                }
            }

            for (int ii = 0; ii < iterationCount; ++ii) {
                conn1->arithmetic(doc, incrDelta);
            }
        }
    };

    std::thread t2 {
        [&conn2, &doc, &iterationCount, &decrDelta]() {
            conn2->arithmetic(doc + "_t2", 0, 0);
            bool running = false;
            while (!running) {
                try {
                    conn2->arithmetic(doc + "_t1", 1, 0, 0xffffffff);
                    running = true;
                } catch (const ConnectionError& error) {
                    ASSERT_TRUE(error.isNotFound());
                }
            }

            for (int ii = 0; ii < iterationCount; ++ii) {
                conn2->arithmetic(doc, decrDelta);
            }
        }
    };

    // Wait for them to complete
    t1.join();
    t2.join();

    expected += (iterationCount * incrDelta) + (iterationCount * decrDelta);
    EXPECT_EQ(expected, userConnection->increment(name, 0));
}

TEST_P(ArithmeticTest, TestMutationInfo) {
    MutationInfo info;
    ASSERT_EQ(0, userConnection->increment(name, 0, 0, 0, &info));

    // Not all the backends supports the vbucket seqno and uuid..
    // The cas should be filled out, so we should be able to do a CAS replace
    Document doc;
    doc.info.cas = info.cas;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();

    userConnection->mutate(doc, Vbid(0), MutationType::Replace);
}

TEST_P(ArithmeticTest, TestIllegalDatatype) {
    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();

    userConnection->mutate(doc, Vbid(0), MutationType::Add);

    try {
        userConnection->increment(name, 0);
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isDeltaBadval()) << error.getReason();
    }
}

/*
 * Unit test to test the fix for MB-33813
 * This test checks that we do not return NOT_STORED back to the client
 * if bucket_store returns it to ArithmeticCommandContext::storeNewItem().
 * Instead ArithmeticCommandContext::storeNewItem() should reset the
 * ArithmeticCommandContext state machine and try again.
 *
 * To check that cb::engine_errc::not_stored/NOT_STORED is not returned we use
 * eWouldBlockEngine to return cb::engine_errc::not_stored on the 3rd engine
 * request using the binary sequence 0b100.
 *
 * This works as the process we expect to see by the ArithmeticCommandContext
 * state machine is as follows:
 *  start: GetItem
 *      calls bucket_get() should return cb::engine_errc::success
 *  -> CreateNewItem
 *      calls bucket_allocate_ex should return cb::engine_errc::success
 *  -> StoreNewItem
 *      calls bucket_store return cb::engine_errc::not_stored
 *      cb::engine_errc::not_stored returned so reset and try again.
 *  -> Reset
 *  -> GetItem
 *  .... Do the work to increment the value again
 *  -> Done
 */
TEST_P(ArithmeticTest, MB33813) {
    std::string key(name + "_inc");

    // Make the 3rd request send to the engine return
    // cb::engine_errc::not_stored In this case this will be the store that
    // happens as a result of a call to ArithmeticCommandContext::storeNewItem()
    auto sequence = ewb::encodeSequence({
            ewb::Passthrough,
            ewb::Passthrough,
            cb::engine_errc::not_stored,
            ewb::Passthrough,
            ewb::Passthrough,
            ewb::Passthrough,
    });
    userConnection->configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                               /*unused*/ {},
                                               /*unused*/ {},
                                               sequence);

    EXPECT_EQ(1, userConnection->increment(key, 0, 1));

    userConnection->disableEwouldBlockEngine();

    Vbid vb(0);
    Document doc = userConnection->get(key, vb);
    EXPECT_EQ("1", doc.value);

    EXPECT_EQ(2, userConnection->increment(key, 1));

    doc = userConnection->get(key, vb);
    EXPECT_EQ("2", doc.value);

    // Sanity check do the same thing but with a decrement
    key = name + "_dec";
    userConnection->configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                               /*unused*/ {},
                                               /*unused*/ {},
                                               sequence);

    EXPECT_EQ(2, userConnection->decrement(key, 0, 2));

    userConnection->disableEwouldBlockEngine();

    doc = userConnection->get(key, vb);
    EXPECT_EQ("2", doc.value);

    EXPECT_EQ(1, userConnection->decrement(key, 1));

    doc = userConnection->get(key, vb);
    EXPECT_EQ("1", doc.value);
}

static void test_stored_doc(MemcachedConnection& conn,
                            const std::string& key,
                            const std::string& content,
                            bool badval) {

    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = key;
    doc.value = content;

    ASSERT_NO_THROW(conn.mutate(doc, Vbid(0), MutationType::Set));
    if (badval) {
        try {
            conn.increment(key, 1);
            FAIL() << "Did not catch: " << content;
        } catch (const ConnectionError& error) {
            EXPECT_TRUE(error.isDeltaBadval()) << error.getReason();
        }
    } else {
        EXPECT_EQ(1, conn.increment(key, 1));
    }
}

TEST_P(ArithmeticTest, TestOperateOnStoredDocument) {
#ifdef THREAD_SANITIZER
    // This test takes ~20 secs under Thread sanitizer, and it is
    // mostly here in order to validate that we correctly detect if
    // we can perform incr/decr on a document stored in the cache
    // (depending on the content being a legal value or not)
    // To speed up CV, just run the test for a single parameterization.
    if (GetParam() != std::make_tuple(TransportProtocols::McbpPlain,
                                      XattrSupport::Yes,
                                      ClientJSONSupport::Yes,
                                      ClientSnappySupport::Yes)) {
        return;
    }
#endif
    // It is "allowed" for the value to be padded with whitespace
    // before and after the numeric value.
    // but no other characters should be present!
    for (unsigned int ii = 0; ii < 256; ++ii) {
        if (std::isspace(ii)) {
            std::string content;
            content = std::string{"0"} + char(ii);
            test_stored_doc(*userConnection, name, content, false);
            content = char(ii) + std::string{"0"};
            test_stored_doc(*userConnection, name, content, false);
        } else if (!std::isdigit(ii)) {
            std::string content;
            if (ii != 0) {
                content = std::string{"0"} + char(ii);
                test_stored_doc(*userConnection, name, content, true);
            }

            if (ii != '-' && ii != '+') {
                content = char(ii) + std::string{"0"};
                test_stored_doc(*userConnection, name, content, true);
            }
        }
    }
}

TEST_P(ArithmeticXattrOnTest, TestDocWithXattr) {
    EXPECT_EQ(0, userConnection->increment(name, 1));

    // Add an xattr
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictAdd);
        cmd.setKey(name);
        cmd.setPath("meta.author");
        cmd.setValue("\"Trond Norbye\"");
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        const auto resp = userConnection->execute(cmd);
        ASSERT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
    }

    // Perform the normal operation
    EXPECT_EQ(1, userConnection->increment(name, 1));

    // The xattr should have been preserved!
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
        cmd.setKey(name);
        cmd.setPath("meta.author");
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
        userConnection->sendCommand(cmd);

        BinprotSubdocResponse resp;
        userConnection->recvResponse(resp);
        ASSERT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
        EXPECT_EQ("\"Trond Norbye\"", resp.getValue());
    }
}

// Increment and decrement should not update the expiry time on existing
// documents
TEST_P(ArithmeticXattrOnTest, MB25402) {
    // Start by creating the counter without expiry time
    userConnection->increment(name, 1, 0, 0, nullptr);
    // increment the counter (which should already exists causing the expiry
    // time to be ignored)
    userConnection->increment(name, 1, 0, 3600, nullptr);

    // Verify that the expiry time is still
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$document", SUBDOC_FLAG_XATTR_PATH);
    userConnection->sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    userConnection->recvResponse(multiResp);

    auto& results = multiResp.getResults();

    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);

    // Ensure that we found all we expected and they're of the correct type:
    auto meta = nlohmann::json::parse(results[0].value);
    EXPECT_EQ(0, meta["exptime"].get<int>());
}
