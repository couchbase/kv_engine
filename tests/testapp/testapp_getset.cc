/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "testapp.h"
#include "testapp_client_test.h"
#include <protocol/connection/client_mcbp_connection.h>

#include <algorithm>
#include <platform/compress.h>

class GetSetTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
    }
};

INSTANTIATE_TEST_CASE_P(
        TransportProtocols,
        GetSetTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain,
                                             TransportProtocols::McbpIpv6Plain,
                                             TransportProtocols::McbpSsl,
                                             TransportProtocols::McbpIpv6Ssl),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No)),
        PrintToStringCombinedName());

TEST_P(GetSetTest, TestAdd) {
    MemcachedConnection& conn = getConnection();
    conn.mutate(document, 0, MutationType::Add);

    int eExistsCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    // Adding it one more time should fail
    try {
        conn.mutate(document, 0, MutationType::Add);
        FAIL() << "It should not be possible to add a document that exists";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eExistsCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS));
    }

    // Add with a cas should fail
    int invalCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL);
    try {
        document.info.cas = mcbp::cas::Wildcard + 1;
        conn.mutate(document, 0, MutationType::Add);
        FAIL() << "It should not be possible to add a document that exists";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(invalCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL));
    }
}

TEST_P(GetSetTest, TestReplace) {
    MemcachedConnection& conn = getConnection();

    // Replacing a nonexisting document should fail
    int eNoentCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    try {
        conn.mutate(document, 0, MutationType::Replace);
        FAIL() << "It's not possible to replace a nonexisting document";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eNoentCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT));
    }

    conn.mutate(document, 0, MutationType::Add);
    // Replace this time should be fine!
    MutationInfo info;
    info = conn.mutate(document, 0, MutationType::Replace);

    // Replace with invalid cas should fail
    document.info.cas = info.cas + 1;

    int eExistsCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    try {
        conn.mutate(document, 0, MutationType::Replace);
        FAIL() << "replace with CAS mismatch should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eExistsCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS));
    }

    // Trying to replace a deleted document should also fail
    conn.remove(name, 0, 0);
    document.info.cas = 0;
    try {
        conn.mutate(document, 0, MutationType::Replace);
        FAIL() << "It's not possible to replace a nonexisting document (deleted)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }

    // And CAS replace
    document.info.cas = 1;
    try {
        conn.mutate(document, 0, MutationType::Replace);
        FAIL() << "It's not possible to replace a nonexisting document (deleted)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }

}

TEST_P(GetSetTest, TestReplaceWithXattr) {
    // The current code does not preserve XATTRs yet
    getConnection().mutate(document, 0, MutationType::Add);

    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getValue());
    getConnection().mutate(document, 0, MutationType::Replace);
    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));
}

TEST_P(GetSetTest, TestSet) {
    MemcachedConnection& conn = getConnection();
    // Set should fail if the key doesn't exists and we're using CAS
    document.info.cas = 1;

    int eNoentCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    try {
        conn.mutate(document, 0, MutationType::Set);
        FAIL() << "Set with CAS and no such doc should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
        EXPECT_EQ(eNoentCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT));
    }

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    // set should work even if a nonexisting document should fail
    document.info.cas = mcbp::cas::Wildcard;
    conn.mutate(document, 0, MutationType::Set);

    // And it should be possible to set it once more
    auto info = conn.mutate(document, 0, MutationType::Set);

    // And it should be possible to set it with a CAS
    document.info.cas = info.cas;
    info = conn.mutate(document, 0, MutationType::Set);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    // Replace with invalid cas should fail
    document.info.cas = info.cas + 1;

    int eExistsCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    try {
        conn.mutate(document, 0, MutationType::Replace);
        FAIL() << "set with CAS mismatch should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eExistsCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS));
    }
}

TEST_P(GetSetTest, TestGetMiss) {
    MemcachedConnection& conn = getConnection();
    int eNoentCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    try {
        conn.get("TestGetMiss", 0);
        FAIL() << "Expected TestGetMiss to throw an exception";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eNoentCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT));
    }
}

TEST_P(GetSetTest, TestGetSuccess) {
    MemcachedConnection& conn = getConnection();
    conn.mutate(document, 0, MutationType::Set);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    const auto stored = conn.get(name, 0);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 1,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(cb::mcbp::Datatype::JSON, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppend) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');
    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value[0] = 'b';
    conn.mutate(document, 0, MutationType::Append);

    const auto stored = conn.get(name, 0);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    document.value.push_back('b');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppendWithXattr) {
    // The current code does not preserve XATTRs
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');
    int sucCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    getConnection().mutate(document, 0, MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getValue());

    document.value[0] = 'b';
    getConnection().mutate(document, 0, MutationType::Append);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));

    const auto stored = getConnection().get(name, 0);

    // Check that we correctly increment the status counter stat.
    // * We expect 7 * helloResps because of
    //   a) 3x getConnection() above
    //   b) 3x getConnection() in the 1xcreateXattr and 2xgetXattr
    //   c) 1x for getResponseCount below
    // * We expect testSuccessCount successes for each command we ran
    // * Plus 1 more success to account for the stat call in the first
    //   getResponseCount
    int testSuccessCount = 6;
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        // We had 3x xattr operations fail (1x createXattr 2x getXattr)
        testSuccessCount = 3;
    }
    EXPECT_EQ(sucCount + (helloResps() * 7) + testSuccessCount + 1,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    // And the rest of the doc should look the same
    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    document.value.push_back('b');
    EXPECT_EQ(document.value, stored.value);
}


TEST_P(GetSetTest, TestAppendCasSuccess) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas;
    conn.mutate(document, 0, MutationType::Append);

    const auto stored = conn.get(name, 0);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(info.cas, stored.info.cas);
    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    document.value.push_back('b');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppendCasMismatch) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, MutationType::Append);
        FAIL() << "Append with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }

    // verify it didn't change..
    const auto stored = conn.get(name, 0);

    EXPECT_EQ(info.cas, stored.info.cas);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPrepend) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value[0] = 'b';
    conn.mutate(document, 0, MutationType::Prepend);

    const auto stored = conn.get(name, 0);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value.push_back('a');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPrependWithXattr) {
    // The current code does not preserve XATTRs
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    int sucCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    getConnection().mutate(document, 0, MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getValue());

    document.value[0] = 'b';
    getConnection().mutate(document, 0, MutationType::Prepend);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));

    const auto stored = getConnection().get(name, 0);

    // Check that we correctly increment the status counter stat.
    // * We expect 7 * helloResps because of
    //   a) 3x getConnection() above
    //   b) 3x getConnection() in the 1xcreateXattr and 2xgetXattr
    //   c) 1x for getResponseCount below
    // * We expect testSuccessCount successes for each command we ran
    // * Plus 1 more success to account for the stat call in the first
    //   getResponseCount
    int testSuccessCount = 6;
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        // We had xattr operations fail (1x createXattr 2x getXattr)
        testSuccessCount = 3;
    }
    EXPECT_EQ(sucCount + (helloResps() * 7) + testSuccessCount + 1,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    // And the rest of the doc should look the same
    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value.push_back('a');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPrependCasSuccess) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas;
    conn.mutate(document, 0, MutationType::Prepend);

    const auto stored = conn.get(name, 0);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value.push_back('a');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPerpendCasMismatch) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, MutationType::Prepend);
        FAIL() << "Prepend with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }
    const auto stored = conn.get(name, 0);

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestIllegalVbucket) {
    auto& conn = getConnection();
    conn.authenticate("@admin", "password", "PLAIN");
    // A memcached bucket only use vbucket 0
    conn.createBucket("bucket", "", BucketType::Memcached);
    conn.selectBucket("bucket");

    try {
        conn.get("TestGetInvalidVbucket", 1);
        FAIL() << "Expected fetch of item in illegal vbucket to throw an exception";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotMyVbucket()) << error.what();
    }

    conn.deleteBucket("bucket");
    conn.reconnect();
}

static void compress_vector(const std::vector<char>& input,
                            std::vector<uint8_t>& output) {
    cb::compression::Buffer compressed;
    EXPECT_TRUE(cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                         input.data(), input.size(),
                                         compressed));
    EXPECT_GT(input.size(), compressed.len);
    output.resize(compressed.len);
    memcpy(output.data(), compressed.data.get(), compressed.len);
}

TEST_P(GetSetTest, TestAppendCompressedSource) {
    TESTAPP_SKIP_IF_NO_COMPRESSION();
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value.resize(input.size());
    std::fill(document.value.begin(), document.value.end(), 'b');
    document.info.datatype = cb::mcbp::Datatype::Raw;

    conn.mutate(document, 0, MutationType::Append);
    const auto stored = conn.get(name, 0);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'a', input.size());
    memset(expected.data() + input.size(), 'b', input.size());
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestAppendCompressedData) {
    TESTAPP_SKIP_IF_NO_COMPRESSION();
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.resize(1024);
    std::fill(document.value.begin(), document.value.end(), 'a');

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    compress_vector(input, document.value);
    document.info.datatype = cb::mcbp::Datatype::Snappy;
    conn.mutate(document, 0, MutationType::Append);

    const auto stored = conn.get(name, 0);

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'a', input.size());
    memset(expected.data() + input.size(), 'b', input.size());

    ASSERT_EQ(2048, stored.value.size());

    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestAppendCompressedSourceAndData) {
    TESTAPP_SKIP_IF_NO_COMPRESSION();
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> append(1024);
    std::fill(append.begin(), append.end(), 'b');
    compress_vector(append, document.value);
    conn.mutate(document, 0, MutationType::Append);
    const auto stored = conn.get(name, 0);

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() + append.size());
    memset(expected.data(), 'a', input.size());
    memset(expected.data() + input.size(), 'b', append.size());
    EXPECT_EQ(expected, stored.value);
}


TEST_P(GetSetTest, TestPrependCompressedSource) {
    TESTAPP_SKIP_IF_NO_COMPRESSION();
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value.resize(input.size());
    std::fill(document.value.begin(), document.value.end(), 'b');
    document.info.datatype = cb::mcbp::Datatype::Raw;

    conn.mutate(document, 0, MutationType::Prepend);
    const auto stored = conn.get(name, 0);

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'b', input.size());
    memset(expected.data() + input.size(), 'a', input.size());
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestPrependCompressedData) {
    TESTAPP_SKIP_IF_NO_COMPRESSION();
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.resize(1024);
    std::fill(document.value.begin(), document.value.end(), 'a');

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    compress_vector(input, document.value);
    document.info.datatype = cb::mcbp::Datatype::Snappy;
    conn.mutate(document, 0, MutationType::Prepend);

    const auto stored = conn.get(name, 0);

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'b', input.size());
    memset(expected.data() + input.size(), 'a', input.size());
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestPrepepndCompressedSourceAndData) {
    TESTAPP_SKIP_IF_NO_COMPRESSION();
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> append(1024);
    std::fill(append.begin(), append.end(), 'b');
    compress_vector(append, document.value);
    conn.mutate(document, 0, MutationType::Prepend);
    const auto stored = conn.get(name, 0);

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() + append.size());
    memset(expected.data(), 'b', input.size());
    memset(expected.data() + input.size(), 'a', append.size());
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestGetMeta) {
    getConnection().mutate(document, 0, MutationType::Add);
    auto meta = getConnection().getMeta(document.info.id, 0, 0);
    EXPECT_EQ(0, meta.deleted);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, meta.datatype);
    EXPECT_EQ(0, meta.expiry);

    document.info.datatype = cb::mcbp::Datatype::Raw;
    getConnection().mutate(document, 0, MutationType::Replace);
    meta = getConnection().getMeta(document.info.id, 0, 0);
    EXPECT_EQ(0, meta.deleted);
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, meta.datatype);
}
