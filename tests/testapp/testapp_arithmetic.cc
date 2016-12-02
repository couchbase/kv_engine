/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <cctype>
#include <limits>
#include <thread>
#include "testapp_arithmetic.h"

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        ArithmeticTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(ArithmeticTest, TestArithmeticNoCreateOnNotFound) {
    auto& connection = getConnection();

    try {
        connection.increment(name, 1, 0, 0xffffffff);
        FAIL() << "Document should not be created";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.getReason();
    }

    try {
        connection.decrement(name, 1, 0, 0xffffffff);
        FAIL() << "Document should not be created";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.getReason();
    }
}

TEST_P(ArithmeticTest, TestIncrementCreateOnNotFound) {
    auto& connection = getConnection();
    EXPECT_EQ(0, connection.increment(name+"_incr", 1, 0));
    EXPECT_EQ(0, connection.increment(name+"_decr", 1, 0));
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
    incr_decr_loop(getConnection(), name, 1);
}

TEST_P(ArithmeticTest, TestBasicArithmetic_33) {
    incr_decr_loop(getConnection(), name, 33);
}

TEST_P(ArithmeticTest, TestDecrementDontWrap) {
    auto& connection = getConnection();
    for (int ii = 0; ii < 10; ++ii) {
        EXPECT_EQ(0, connection.decrement(name, 1));
    }
}

TEST_P(ArithmeticTest, TestIncrementDoesWrap) {
    auto& connection = getConnection();
    uint64_t initial = std::numeric_limits<uint64_t>::max();

    // Create the initial value so that we know where we should start off ;)
    EXPECT_EQ(0, connection.increment(name, 1));

    EXPECT_EQ(initial, connection.increment(name, initial));
    EXPECT_EQ(0, connection.increment(name, 1));
    EXPECT_EQ(initial, connection.increment(name, initial));
    EXPECT_EQ(0, connection.increment(name, 1));
}

TEST_P(ArithmeticTest, TestConcurrentAccess) {
    auto& conn = getConnection();
    auto conn1 = conn.clone();
    auto conn2 = conn.clone();
    const int iterationCount = 100;
    const int incrDelta = 7;
    const int decrDelta = -3;

    // Create the starting point
    uint64_t expected = std::numeric_limits<uint32_t>::max();
    ASSERT_EQ(0, conn.increment(name, 0));
    ASSERT_EQ(expected, conn.increment(name, expected));

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
    EXPECT_EQ(expected, conn.increment(name, 0));
}

TEST_P(ArithmeticTest, TestMutationInfo) {
    auto& conn = getConnection();
    MutationInfo info;
    ASSERT_EQ(0, conn.increment(name, 0, 0, 0, &info));

    // Not all the backends supports the vbucket seqno and uuid..
    // The cas should be filled out, so we should be able to do a CAS replace
    Document doc;
    doc.info.cas = info.cas;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Json;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    char* ptr = cJSON_Print(memcached_cfg.get());
    std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
    cJSON_Free(ptr);

    conn.mutate(doc, 0, Greenstack::MutationType::Replace);
}

TEST_P(ArithmeticTest, TestIllegalDatatype) {
    auto& conn = getConnection();

    Document doc;
    doc.info.cas = Greenstack::CAS::Wildcard;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Json;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    char* ptr = cJSON_Print(memcached_cfg.get());
    std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
    cJSON_Free(ptr);

    ASSERT_NO_THROW(conn.mutate(doc, 0, Greenstack::MutationType::Add));

    try {
        conn.increment(name, 0);
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isDeltaBadval()) << error.getReason();
    }
}

static void test_stored_doc(MemcachedConnection& conn,
                            const std::string& key,
                            const std::string& content,
                            bool badval) {

    Document doc;
    doc.info.cas = Greenstack::CAS::Wildcard;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Raw;
    doc.info.flags = 0xcaffee;
    doc.info.id = key;
    std::copy(content.begin(), content.end(), std::back_inserter(doc.value));

    ASSERT_NO_THROW(conn.mutate(doc, 0, Greenstack::MutationType::Set));
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
    auto& conn = getConnection();

    // It is "allowed" for the value to be padded with whitespace
    // before and after the numeric value.
    // but no other characters should be present!
    for (unsigned int ii = 0; ii < 256; ++ii) {
        if (std::isspace(ii)) {
            std::string content;
            content = std::string{"0"} + char(ii);
            test_stored_doc(conn, name, content, false);
            content = char(ii) + std::string{"0"};
            test_stored_doc(conn, name, content, false);
        } else if (!std::isdigit(ii)) {
            std::string content;
            if (ii != 0) {
                content = std::string{"0"} + char(ii);
                test_stored_doc(conn, name, content, true);
            }

            if (ii != '-' && ii != '+') {
                content = char(ii) + std::string{"0"};
                test_stored_doc(conn, name, content, true);
            }
        }
    }
}

TEST_P(ArithmeticTest, TestDocWithXattr) {
    auto& conn = getConnection();
    conn.increment(name, 1);

    // Add an xattr
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
        cmd.setKey(name);
        cmd.setPath("meta.author");
        cmd.setValue("\"Trond Norbye\"");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH);

        BinprotResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    // Perform the normal operation
    EXPECT_EQ(1, conn.increment(name, 1));

    // The xattr should have been preserved!
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET);
        cmd.setKey(name);
        cmd.setPath("meta.author");
        cmd.setFlags(SUBDOC_FLAG_XATTR_PATH);

        BinprotSubdocResponse resp;
        safe_do_command(cmd, resp, PROTOCOL_BINARY_RESPONSE_SUCCESS);

        EXPECT_EQ("\"Trond Norbye\"", resp.getValue());
    }
}
