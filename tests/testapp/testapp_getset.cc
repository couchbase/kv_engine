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

#include <algorithm>
#include <platform/compress.h>

class GetSetTest : public TestappXattrClientTest {
protected:
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
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
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
    EXPECT_TRUE(hasCorrectDatatype(stored, expectedJSONDatatype()));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 1,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppend) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";
    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value = "b";
    conn.mutate(document, 0, MutationType::Append);

    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ab"), stored.value);
}

TEST_P(GetSetTest, TestAppendWithXattr) {
    // The current code does not preserve XATTRs
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";
    int sucCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    getConnection().mutate(document, 0, MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getValue());

    document.value = "b";
    getConnection().mutate(document, 0, MutationType::Append);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));

    const auto stored = getConnection().get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

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
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ab"), stored.value);
}


TEST_P(GetSetTest, TestAppendCasSuccess) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas;
    conn.mutate(document, 0, MutationType::Append);

    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(info.cas, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ab"), stored.value);
}

TEST_P(GetSetTest, TestAppendCasMismatch) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, MutationType::Append);
        FAIL() << "Append with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }

    // verify it didn't change..
    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    EXPECT_EQ(info.cas, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("a"), stored.value);
}

TEST_P(GetSetTest, TestPrepend) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value = "b";
    conn.mutate(document, 0, MutationType::Prepend);

    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ba"), stored.value);
}

TEST_P(GetSetTest, TestPrependWithXattr) {
    // The current code does not preserve XATTRs
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int sucCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    getConnection().mutate(document, 0, MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getValue());

    document.value = "b";
    getConnection().mutate(document, 0, MutationType::Prepend);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));

    const auto stored = getConnection().get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

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
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ba"), stored.value);
}

TEST_P(GetSetTest, TestPrependCasSuccess) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas;
    conn.mutate(document, 0, MutationType::Prepend);

    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ba"), stored.value);
}

TEST_P(GetSetTest, TestPrependCasMismatch) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    const auto info = conn.mutate(document, 0, MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, MutationType::Prepend);
        FAIL() << "Prepend with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }
    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    EXPECT_NE(mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("a"), stored.value);
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
                            std::string& output) {
    cb::compression::Buffer compressed;
    EXPECT_TRUE(cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                         input, compressed));
    EXPECT_GT(input.size(), compressed.size());
    output.assign(compressed.data(), compressed.size());
}

TEST_P(GetSetTest, TestCompressedData) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    EXPECT_EQ(successCount + statResps() + 1,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));
}

TEST_P(GetSetTest, TestInvalidCompressedData) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');

    // Replacing a nonexisting document should fail
    int einvalCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL);
    try {
        conn.mutate(document, 0, MutationType::Set);
        FAIL() << "It's not possible to set uncompressed documents if the "
                  "datatype is set as SNAPPY";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(einvalCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL));
    }
}

TEST_P(GetSetTest, TestAppendCompressedSource) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value.assign(input.size(), 'b');
    document.info.datatype = cb::mcbp::Datatype::Raw;

    conn.mutate(document, 0, MutationType::Append);
    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(input.size(), 'a');
    expected.append(input.size(), 'b');
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestAppendCompressedData) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.assign(1024, 'a');

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    compress_vector(input, document.value);
    document.info.datatype = cb::mcbp::Datatype::Snappy;
    conn.mutate(document, 0, MutationType::Append);

    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(input.size(), 'a');
    expected.append(input.size(), 'b');

    ASSERT_EQ(2048, stored.value.size());

    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestAppendInvalidCompressedData) {
    MemcachedConnection& conn = getConnection();
        document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.assign(1024, 'a');

    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    int einvalCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL);
    try {
        conn.mutate(document, 0, MutationType::Append);
        FAIL() << "It's not possible to append uncompressed documents if the "
                  "datatype is set as SNAPPY";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(einvalCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL));
    }
}

TEST_P(GetSetTest, TestAppendCompressedSourceAndData) {
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
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(input.size(), 'a');
    expected.append(append.size(), 'b');
    EXPECT_EQ(expected, stored.value);
}


TEST_P(GetSetTest, TestPrependCompressedSource) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);
    document.value.assign(input.size(), 'b');
    document.info.datatype = cb::mcbp::Datatype::Raw;

    conn.mutate(document, 0, MutationType::Prepend);
    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(input.size(), 'b');
    expected.append(input.size(), 'a');
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestPrependCompressedData) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.assign(1024, 'a');

    int successCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS);
    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    compress_vector(input, document.value);
    document.info.datatype = cb::mcbp::Datatype::Snappy;
    conn.mutate(document, 0, MutationType::Prepend);

    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(input.size(), 'b');
    expected.append(input.size(), 'a');
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestPrependInvalidCompressedData) {
    MemcachedConnection& conn = getConnection();
        document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.assign(1024, 'a');

    conn.mutate(document, 0, MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    int einvalCount = getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL);
    try {
        conn.mutate(document, 0, MutationType::Prepend);
        FAIL() << "It's not possible to prepend uncompressed documents if the "
                  "datatype is set as SNAPPY";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(einvalCount + 1,
                  getResponseCount(PROTOCOL_BINARY_RESPONSE_EINVAL));
    }
}

TEST_P(GetSetTest, TestPrependCompressedSourceAndData) {
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
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + statResps() + 3,
              getResponseCount(PROTOCOL_BINARY_RESPONSE_SUCCESS));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(input.size(), 'b');
    expected.append(append.size(), 'a');
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestGetMeta) {
    // Set the document value to valid JSON, so memcached sets the datatype
    // as such.
    document.value = R"("valid_json")";
    getConnection().mutate(document, 0, MutationType::Add);
    auto meta =
            getConnection().getMeta(document.info.id, 0, GetMetaVersion::V2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, meta.first);
    EXPECT_EQ(0, meta.second.deleted);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, meta.second.datatype);
    EXPECT_EQ(0, meta.second.expiry);
    meta = getConnection().getMeta(document.info.id, 0, GetMetaVersion::V1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, meta.first);
    EXPECT_EQ(0, meta.second.deleted);
    EXPECT_NE(PROTOCOL_BINARY_DATATYPE_JSON, meta.second.datatype);
    EXPECT_EQ(0, meta.second.expiry);

    // Set the document value to not-JSON, so memcached sets the datatype
    // as such.
    document.value = "[Invalid;JSON}";
    getConnection().mutate(document, 0, MutationType::Replace);
    meta = getConnection().getMeta(document.info.id, 0, GetMetaVersion::V2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, meta.first);
    EXPECT_EQ(0, meta.second.deleted);
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, meta.second.datatype);
}

TEST_P(GetSetTest, TestGetMetaExpiry) {
    // Case `expiry` <= `num_seconds_in_a_month`
    // When we set `document.info.expiration` to a value less than the number
    // of seconds in a month, the backend sets a `exptime` relative to `now`.
    // Memcached internal clock could have up to 1-second delay compared to Real
    // Time Clock. This is the scenario:
    //
    // RTC      0 ----- 1 ----- 2 ----- 3 -->
    // MCC              0 ----- 1 ----- 2 ----- 3 -->
    //
    // That is why we would set `expected = now + seconds - 1`.
    // But, while the following `EXPECT_GE` condition is always verified on
    // local runs, it fails on Jenkins with `meta.expiry < expected` (usually
    // with `meta.expiry` still behind of 1, suggesting that the Memcached
    // clock could have up to 2-second delay compared to RTC, even if this
    // should not be possible). This need more investigation.
    // For now we allow MCC to be 2 seconds behind RTC, so
    // `expected = now + seconds - 2` .
    uint32_t seconds = 60;
    document.info.expiration = seconds;
    time_t now = time(nullptr);
    getConnection().mutate(document, 0, MutationType::Add);
    auto meta =
            getConnection().getMeta(document.info.id, 0, GetMetaVersion::V1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, meta.first);
    uint32_t expected = now + seconds - 2;
    EXPECT_GE(meta.second.expiry, expected);
    EXPECT_LE(meta.second.expiry, expected + 3);

    // Case `expiry` > `num_seconds_in_a_month`
    document.info.expiration = now + 60;
    getConnection().mutate(document, 0, MutationType::Replace);
    meta = getConnection().getMeta(document.info.id, 0, GetMetaVersion::V1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, meta.first);
    EXPECT_EQ(meta.second.expiry, document.info.expiration);
}

// Test that memcached correctly detects documents are JSON irrespective
// of what the client sets the datatype to.
TEST_P(GetSetTest, ServerDetectsJSON) {
    auto& conn = getConnection();
    document.value = R"("valid_JSON_string")";
    document.info.datatype = cb::mcbp::Datatype::Raw;
    conn.mutate(document, 0, MutationType::Add);

    // Fetch the document to see what datatype is has. It should match
    // what our connection is capable of receiving.
    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, expectedJSONDatatype()));
}

// Test that memcached correctly detects documents are not JSON irrespective
// of what the client sets the datatype to.
TEST_P(GetSetTest, ServerDetectsNonJSON) {
    auto& conn = getConnection();
    document.value = R"(not;valid{JSON)";
    conn.mutate(document, 0, MutationType::Add);

    // Fetch the document to see what datatype is has. It should always
    // be raw.
    const auto stored = conn.get(name, 0);
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));
}
