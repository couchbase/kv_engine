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
#include <protocol/connection/client_greenstack_connection.h>
#include <protocol/connection/client_mcbp_connection.h>

#include <algorithm>
#include <platform/compress.h>

class GetSetTest : public TestappClientTest {
public:
    void SetUp() {
        document.info.cas = Greenstack::CAS::Wildcard;
        document.info.compression = Greenstack::Compression::None;
        document.info.datatype = Greenstack::Datatype::Json;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        const std::string content = to_string(memcached_cfg, false);
        std::copy(content.begin(), content.end(),
                  std::back_inserter(document.value));
    }

protected:
    Document document;
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        GetSetTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(GetSetTest, TestAdd) {
    MemcachedConnection& conn = getConnection();
    conn.mutate(document, 0, Greenstack::MutationType::Add);

    // Adding it one more time should fail
    try {
        conn.mutate(document, 0, Greenstack::MutationType::Add);
        FAIL() << "It should not be possible to add a document that exists";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }

    // Add with a cas should fail
    try {
        document.info.cas = Greenstack::CAS::Wildcard + 1;
        conn.mutate(document, 0, Greenstack::MutationType::Add);
        FAIL() << "It should not be possible to add a document that exists";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
    }
}

TEST_P(GetSetTest, TestReplace) {
    MemcachedConnection& conn = getConnection();

    // Replacing a nonexisting document should fail
    try {
        conn.mutate(document, 0, Greenstack::MutationType::Replace);
        FAIL() << "It's not possible to replace a nonexisting document";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }

    conn.mutate(document, 0, Greenstack::MutationType::Add);
    // Replace this time should be fine!
    MutationInfo info;
    info = conn.mutate(document, 0, Greenstack::MutationType::Replace);

    // Replace with invalid cas should fail
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, Greenstack::MutationType::Replace);
        FAIL() << "replace with CAS mismatch should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }
}

TEST_P(GetSetTest, TestReplaceWithXattr) {
    // The current code does not preserve XATTRs yet
    auto& conn = getConnection();
    conn.mutate(document, 0, Greenstack::MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas);
    conn.mutate(document, 0, Greenstack::MutationType::Replace);
    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));
}

TEST_P(GetSetTest, TestSet) {
    MemcachedConnection& conn = getConnection();
    // Set should fail if the key doesn't exists and we're using CAS
    document.info.cas = 1;
    try {
        conn.mutate(document, 0, Greenstack::MutationType::Set);
        FAIL() << "Set with CAS and no such doc should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }

    // set should work even if a nonexisting document should fail
    document.info.cas = Greenstack::CAS::Wildcard;
    conn.mutate(document, 0, Greenstack::MutationType::Set);

    // And it should be possible to set it once more
    auto info = conn.mutate(document, 0, Greenstack::MutationType::Set);

    // And it should be possible to set it with a CAS
    document.info.cas = info.cas;
    info = conn.mutate(document, 0, Greenstack::MutationType::Set);

    // Replace with invalid cas should fail
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, Greenstack::MutationType::Replace);
        FAIL() << "set with CAS mismatch should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }
}

TEST_P(GetSetTest, TestGetMiss) {
    MemcachedConnection& conn = getConnection();
    try {
        conn.get("TestGetMiss", 0);
        FAIL() << "Expected TestGetMiss to throw an exception";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }
}

TEST_P(GetSetTest, TestGetSuccess) {
    MemcachedConnection& conn = getConnection();
    conn.mutate(document, 0, Greenstack::MutationType::Set);

    const auto stored = conn.get(name, 0);

    EXPECT_NE(Greenstack::CAS::Wildcard, stored.info.cas);
    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Json, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppend) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value[0] = 'b';
    conn.mutate(document, 0, Greenstack::MutationType::Append);

    const auto stored = conn.get(name, 0);

    EXPECT_NE(Greenstack::CAS::Wildcard, stored.info.cas);
    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    document.value.push_back('b');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppendWithXattr) {
    // The current code does not preserve XATTRs
    auto& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');
    conn.mutate(document, 0, Greenstack::MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas);

    document.value[0] = 'b';
    conn.mutate(document, 0, Greenstack::MutationType::Append);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));

    const auto stored = conn.get(name, 0);

    // And the rest of the doc should look the same
    EXPECT_NE(Greenstack::CAS::Wildcard, stored.info.cas);
    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    document.value.push_back('b');
    EXPECT_EQ(document.value, stored.value);
}


TEST_P(GetSetTest, TestAppendCasSuccess) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    const auto info = conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas;
    conn.mutate(document, 0, Greenstack::MutationType::Append);

    const auto stored = conn.get(name, 0);

    EXPECT_NE(info.cas, stored.info.cas);
    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    document.value.push_back('b');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppendCasMismatch) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    const auto info = conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, Greenstack::MutationType::Append);
        FAIL() << "Append with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }

    // verify it didn't change..
    const auto stored = conn.get(name, 0);

    EXPECT_EQ(info.cas, stored.info.cas);
    EXPECT_EQ(document.info.compression, stored.info.compression);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPrepend) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value[0] = 'b';
    conn.mutate(document, 0, Greenstack::MutationType::Prepend);

    const auto stored = conn.get(name, 0);

    EXPECT_NE(Greenstack::CAS::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.compression, stored.info.compression);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value.push_back('a');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPrependWithXattr) {
    // The current code does not preserve XATTRs
    auto& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');
    conn.mutate(document, 0, Greenstack::MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas);

    document.value[0] = 'b';
    conn.mutate(document, 0, Greenstack::MutationType::Prepend);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas, getXattr("meta.cas"));

    const auto stored = conn.get(name, 0);

    // And the rest of the doc should look the same
    EXPECT_NE(Greenstack::CAS::Wildcard, stored.info.cas);
    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value.push_back('a');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPrependCasSuccess) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    const auto info = conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas;
    conn.mutate(document, 0, Greenstack::MutationType::Prepend);

    const auto stored = conn.get(name, 0);

    EXPECT_NE(Greenstack::CAS::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.compression, stored.info.compression);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value.push_back('a');
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestPerpendCasMismatch) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.clear();
    document.value.push_back('a');

    const auto info = conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value[0] = 'b';
    document.info.cas = info.cas + 1;
    try {
        conn.mutate(document, 0, Greenstack::MutationType::Prepend);
        FAIL() << "Prepend with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }
    const auto stored = conn.get(name, 0);

    EXPECT_NE(Greenstack::CAS::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.compression, stored.info.compression);
    EXPECT_EQ(document.info.datatype, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    document.value[0] = 'a';
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestIllegalVbucket) {
    auto& conn = getConnection();

    // A memcached bucket only use vbucket 0
    conn.createBucket("bucket", "", Greenstack::BucketType::Memcached);

    try {
        auto second_conn = conn.clone();
        second_conn->selectBucket("bucket");
        second_conn->get("TestGetMiss", 1);
        FAIL() << "Expected TestGetMiss to throw an exception";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotMyVbucket()) << error.what();
    }

    conn.deleteBucket("bucket");
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
    MemcachedConnection& conn = getConnection();
    document.info.compression = Greenstack::Compression::Snappy;
    document.info.datatype = Greenstack::Datatype::Raw;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value.resize(input.size());
    std::fill(document.value.begin(), document.value.end(), 'b');
    document.info.compression = Greenstack::Compression::None;

    conn.mutate(document, 0, Greenstack::MutationType::Append);
    const auto stored = conn.get(name, 0);

    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'a', input.size());
    memset(expected.data() + input.size(), 'b', input.size());
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestAppendCompressedData) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.resize(1024);
    std::fill(document.value.begin(), document.value.end(), 'a');
    conn.mutate(document, 0, Greenstack::MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    compress_vector(input, document.value);
    document.info.compression = Greenstack::Compression::Snappy;
    conn.mutate(document, 0, Greenstack::MutationType::Append);

    const auto stored = conn.get(name, 0);

    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'a', input.size());
    memset(expected.data() + input.size(), 'b', input.size());

    ASSERT_EQ(2048, stored.value.size());

    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestAppendCompressedSourceAndData) {
    MemcachedConnection& conn = getConnection();
    document.info.compression = Greenstack::Compression::Snappy;
    document.info.datatype = Greenstack::Datatype::Raw;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    conn.mutate(document, 0, Greenstack::MutationType::Set);

    std::vector<char> append(1024);
    std::fill(append.begin(), append.end(), 'b');
    compress_vector(append, document.value);
    conn.mutate(document, 0, Greenstack::MutationType::Append);
    const auto stored = conn.get(name, 0);

    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() + append.size());
    memset(expected.data(), 'a', input.size());
    memset(expected.data() + input.size(), 'b', append.size());
    EXPECT_EQ(expected, stored.value);
}


TEST_P(GetSetTest, TestPrependCompressedSource) {
    MemcachedConnection& conn = getConnection();
    document.info.compression = Greenstack::Compression::Snappy;
    document.info.datatype = Greenstack::Datatype::Raw;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    conn.mutate(document, 0, Greenstack::MutationType::Set);
    document.value.resize(input.size());
    std::fill(document.value.begin(), document.value.end(), 'b');
    document.info.compression = Greenstack::Compression::None;

    conn.mutate(document, 0, Greenstack::MutationType::Prepend);
    const auto stored = conn.get(name, 0);

    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'b', input.size());
    memset(expected.data() + input.size(), 'a', input.size());
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestPrependCompressedData) {
    MemcachedConnection& conn = getConnection();
    document.info.datatype = Greenstack::Datatype::Raw;
    document.value.resize(1024);
    std::fill(document.value.begin(), document.value.end(), 'a');
    conn.mutate(document, 0, Greenstack::MutationType::Set);

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'b');
    compress_vector(input, document.value);
    document.info.compression = Greenstack::Compression::Snappy;
    conn.mutate(document, 0, Greenstack::MutationType::Prepend);

    const auto stored = conn.get(name, 0);

    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() * 2);
    memset(expected.data(), 'b', input.size());
    memset(expected.data() + input.size(), 'a', input.size());
    EXPECT_EQ(expected, stored.value);
}

TEST_P(GetSetTest, TestPrepepndCompressedSourceAndData) {
    MemcachedConnection& conn = getConnection();
    document.info.compression = Greenstack::Compression::Snappy;
    document.info.datatype = Greenstack::Datatype::Raw;

    std::vector<char> input(1024);
    std::fill(input.begin(), input.end(), 'a');
    compress_vector(input, document.value);

    conn.mutate(document, 0, Greenstack::MutationType::Set);

    std::vector<char> append(1024);
    std::fill(append.begin(), append.end(), 'b');
    compress_vector(append, document.value);
    conn.mutate(document, 0, Greenstack::MutationType::Prepend);
    const auto stored = conn.get(name, 0);

    EXPECT_EQ(Greenstack::Compression::None, stored.info.compression);
    EXPECT_EQ(Greenstack::Datatype::Raw, stored.info.datatype);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::vector<uint8_t> expected(input.size() + append.size());
    memset(expected.data(), 'b', input.size());
    memset(expected.data() + input.size(), 'a', append.size());
    EXPECT_EQ(expected, stored.value);
}
