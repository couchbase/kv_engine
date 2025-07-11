/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/limits.h>
#include <algorithm>

class GetSetTest : public TestappXattrClientTest {
protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
    }

    void doTestAppend(bool compressedSource, bool compressedData);
    void doTestGetMetaValidJSON(bool compressedSource);
    void doTestPrepend(bool compressedSource, bool compressedData);
    void doTestServerDetectsJSON(bool compressedSource);
    void doTestServerDetectsNonJSON(bool compressedSource);
    void doTestServerStoresUncompressed(bool compressedSource);
    void doTestServerRejectsLargeSize(bool compressedSource);
    void doTestServerRejectsLargeSizeWithXattr(bool compressedSource);

    void verifyData(MemcachedConnection& conn,
                    int successCount,
                    int numOps,
                    cb::mcbp::Datatype expectedDatatype,
                    std::string_view expectedValue);

    void doTestGetRandomKey(bool collections);
};

void GetSetTest::doTestAppend(bool compressedSource, bool compressedData) {
    // Store an initial source value; along with an XATTR to check it's
    // preserved correctly.
    setBodyAndXattr(std::string(1024, 'a'),
                    {{"xattr", "\"X-value\""}},
                    compressedSource);

    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.assign(1024, 'b');
    if (compressedData) {
        document.compress();
    }

    int successCount = getResponseCount(cb::mcbp::Status::Success);

    userConnection->mutate(document, Vbid(0), MutationType::Append);
    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 3, getResponseCount(cb::mcbp::Status::Success));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(1024, 'a');
    expected.append(1024, 'b');
    EXPECT_EQ(expected, stored.value);
    EXPECT_EQ("\"X-value\"", getXattr("xattr").getDataView());
}

void GetSetTest::doTestGetMetaValidJSON(bool compressedSource) {
    // Set the document value to valid JSON, so memcached sets the datatype
    // as such.
    document.value = R"("valid_json")";
    document.info.datatype = cb::mcbp::Datatype::Raw;
    auto expectedDatatype = PROTOCOL_BINARY_DATATYPE_JSON;
    if (compressedSource) {
        document.compress();
        expectedDatatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
    }
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    auto meta = userConnection->getMeta(
            document.info.id, Vbid(0), GetMetaVersion::V2);
    EXPECT_EQ(0, meta.getDeleted());
    EXPECT_EQ(expectedDatatype, meta.getDatatype());
    EXPECT_EQ(0, meta.getExpiry());

    meta = userConnection->getMeta(
            document.info.id, Vbid(0), GetMetaVersion::V1);
    EXPECT_EQ(0, meta.getDeleted());
    EXPECT_NE(expectedDatatype, meta.getDatatype());
    EXPECT_EQ(0, meta.getExpiry());
}

void GetSetTest::doTestPrepend(bool compressedSource, bool compressedData) {
    // Store an initial source value; along with an XATTR to check it's
    // preserved correctly.
    setBodyAndXattr(std::string(1024, 'a'),
                    {{"xattr", "\"X-value\""}},
                    compressedSource);

    int successCount = getResponseCount(cb::mcbp::Status::Success);

    document.value.assign(1024, 'b');
    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.datatype = cb::mcbp::Datatype::Raw;
    if (compressedData) {
        document.compress();
    }
    userConnection->mutate(document, Vbid(0), MutationType::Prepend);
    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 3, getResponseCount(cb::mcbp::Status::Success));

    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    std::string expected(1024, 'b');
    expected.append(1024, 'a');
    EXPECT_EQ(expected, stored.value);
    EXPECT_EQ("\"X-value\"", getXattr("xattr").getDataView());
}

void GetSetTest::doTestServerDetectsJSON(bool compressedSource) {
    document.value = R"("valid_JSON_string")";
    document.info.datatype = cb::mcbp::Datatype::Raw;
    if (compressedSource) {
        document.compress();
    }

    setCompressionMode("passive"); // So server doesn't immediately inflate.

    userConnection->mutate(document, Vbid(0), MutationType::Add);

    // Fetch the document to see what datatype is has. It should be
    // marked as JSON if our connection is capable of receiving JSON,
    // and Snappy if we compressed it.
    const auto stored = userConnection->get(name, Vbid(0));
    auto expectedDatatype = expectedJSONDatatype();
    if (compressedSource) {
        expectedDatatype = cb::mcbp::Datatype(int(expectedDatatype) |
                                              int(cb::mcbp::Datatype::Snappy));
    }
    EXPECT_TRUE(hasCorrectDatatype(stored, expectedDatatype));
}

void GetSetTest::doTestServerDetectsNonJSON(bool compressedSource) {
    document.value = R"(not;valid{JSON)";
    document.info.datatype = cb::mcbp::Datatype::Raw;
    if (compressedSource) {
        document.compress();
    }
    userConnection->mutate(document, Vbid(0), MutationType::Add);

    // Fetch the document to see what datatype is has. It should be
    // Raw, plus Snappy if we compressed it.
    const auto stored = userConnection->get(name, Vbid(0));
    auto expectedDatatype = cb::mcbp::Datatype::Raw;
    if (compressedSource) {
        expectedDatatype = cb::mcbp::Datatype(int(expectedDatatype) |
                                              int(cb::mcbp::Datatype::Snappy));
    }
    EXPECT_TRUE(hasCorrectDatatype(stored, expectedDatatype));
}

void GetSetTest::doTestServerStoresUncompressed(bool compressedSource) {
    setMinCompressionRatio(2);

    std::string stringToStore{"valid_string_to_store"};
    document.value = stringToStore;
    document.info.datatype = cb::mcbp::Datatype::Raw;
    if (compressedSource) {
        document.compress();
    }

    int successCount = getResponseCount(cb::mcbp::Status::Success);
    userConnection->mutate(document, Vbid(0), MutationType::Set);

    // Fetch the document to see what the data is stored as. It should be
    // stored as Raw and the data is store as is
    verifyData(*userConnection,
               successCount,
               1,
               cb::mcbp::Datatype::Raw,
               stringToStore);
}

void GetSetTest::doTestServerRejectsLargeSize(bool compressedSource) {
    std::string valueToStore(GetTestBucket().getMaximumDocSize() + 1, 'a');
    document.value = valueToStore;
    document.info.datatype = cb::mcbp::Datatype::Raw;
    if (compressedSource) {
        document.compress();
    }

    int e2bigCount = getResponseCount(cb::mcbp::Status::E2big);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Set);
        FAIL() << "It should not be possible to add a document whose size is "
                  "greater than the max item size";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isTooBig()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(e2bigCount + 1, getResponseCount(cb::mcbp::Status::E2big));
    }
}

void GetSetTest::doTestServerRejectsLargeSizeWithXattr(bool compressedSource) {
    // Add a document with size 1KB of user data and with 1 user xattr
    setBodyAndXattr(std::string(1024, 'a'), {{"xattr", "\"X-value\""}});

    // Now add a value of size that is 10 bytes less than the maximum
    // permitted value. This would ideally succeed if there was no
    // existing user xattr but given that there is already one user xattr,
    // this should fail with E2BIG
    std::string userdata(GetTestBucket().getMaximumDocSize() - 10, 'a');
    document.value = userdata;
    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.datatype = cb::mcbp::Datatype::Raw;

    if (compressedSource) {
        document.compress();
    }

    int e2bigCount = getResponseCount(cb::mcbp::Status::E2big);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Set);
        FAIL() << "It should not be possible to add a document whose size is "
                "greater than the max item size";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isTooBig()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(e2bigCount + 1, getResponseCount(cb::mcbp::Status::E2big));
    }

    // Now add a document with system xattrs
    std::string sysXattr = "_sync";
    std::string xattrVal = "{\"eg\":";
    xattrVal.append("\"X-value\"}");
    setBodyAndXattr(std::string(1024, 'a'), {{sysXattr, xattrVal}});

    userdata.assign(GetTestBucket().getMaximumDocSize() - 250, 'a');
    // Now add a document with value size that is 250 bytes less than the
    // maximum permitted limit. This should succeed because system xattrs
    // doesn't fall into the quota for a regular document
    document.value = userdata;
    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.datatype = cb::mcbp::Datatype::Raw;

    if (compressedSource) {
        document.compress();
    }

    int successCount = getResponseCount(cb::mcbp::Status::Success);
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    EXPECT_EQ(successCount + 2, getResponseCount(cb::mcbp::Status::Success));

    // Add a system xattr that exceeds the 1MB quota limit. There is some
    // internal overhead in the xattr (4 byte total length field plus 6 bytes
    // per value pair)
    xattrVal.assign(R"({"eg":")");
    xattrVal.append(std::string(1048552, 'a'));
    xattrVal.append(std::string("\"}"));
    EXPECT_EQ(1_MiB, 4 + 6 + xattrVal.size() + sysXattr.size());

    setBodyAndXattr(userdata, {{sysXattr, xattrVal}});

    // Default bucket supports xattr, but the max document size is 1MB so we
    // can't store additional data in those buckets if the system xattr
    // occupies the entire document
    if (GetTestBucket().getMaximumDocSize() > cb::limits::PrivilegedBytes) {
        // Add a document value that is exactly the size of the
        // maximum allowed size. This should be allowed as the system
        // xattrs has its own storage limit
        userdata.assign(GetTestBucket().getMaximumDocSize(), 'a');
        setBodyAndXattr(userdata, {{sysXattr, xattrVal}});

        // But it should fail if we try to use a user xattr
        e2bigCount = getResponseCount(cb::mcbp::Status::E2big);
        try {
            setBodyAndXattr(userdata,
                            {{sysXattr, xattrVal}, {"foo", R"({"a":"b"})"}});
            FAIL() << "It should not be possible to add a document whose size "
                      "is greater than the max item size";
        } catch (const ConnectionError& error) {
            EXPECT_TRUE(error.isTooBig()) << error.what();
        }
        EXPECT_EQ(e2bigCount + 1, getResponseCount(cb::mcbp::Status::E2big));
    }
}

void GetSetTest::verifyData(MemcachedConnection& conn,
                            int successCount,
                            int numOps,
                            cb::mcbp::Datatype expectedDatatype,
                            std::string_view expectedValue) {
    const auto stored = conn.get(name, Vbid(0));
    EXPECT_EQ(successCount + numOps + 2,
              getResponseCount(cb::mcbp::Status::Success));

    EXPECT_TRUE(hasCorrectDatatype(stored, expectedDatatype));
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);

    ASSERT_EQ(expectedValue.size(), stored.value.size());
    EXPECT_EQ(expectedValue, stored.value);
}

/// Test fixture for Get/Set tests which want to run with Snappy both
/// Off and On.
class GetSetSnappyOnOffTest : public GetSetTest {
protected:
    void doTestCompressedRawData(const std::string& mode);
    void doTestCompressedJSON(const std::string& mode);
};

void GetSetSnappyOnOffTest::doTestCompressedRawData(const std::string& mode) {
    setCompressionMode(mode);

    const std::string valueData(1024, 'a');
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = valueData;
    document.compress();

    switch (hasSnappySupport()) {
    case ClientSnappySupport::Everywhere:
    case ClientSnappySupport::Yes: {
        // Should be accepted.
        int successCount = getResponseCount(cb::mcbp::Status::Success);

        userConnection->mutate(document, Vbid(0), MutationType::Set);

        // Expect to get Snappy-compressed data back in passive/active mode
        // (as it keeps data compressed), uncompressed if in off mode.
        auto expectedDatatype = (mode == "off") ? cb::mcbp::Datatype::Raw
                                                : cb::mcbp::Datatype::Snappy;
        auto expectedValue = (mode == "off") ? valueData : document.value;
        verifyData(*userConnection,
                   successCount,
                   1,
                   expectedDatatype,
                   expectedValue);
        break;
    }

    case ClientSnappySupport::No:
        // Should fail as client didn't negotiate Snappy.
        try {
            userConnection->mutate(document, Vbid(0), MutationType::Set);
            FAIL() << "Should not accept datatype.Snappy document if client "
                      "didn't negotiate Snappy.";
        } catch (ConnectionError& error) {
            EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        }
        break;
    }
}

void GetSetSnappyOnOffTest::doTestCompressedJSON(const std::string& mode) {
    setCompressionMode(mode);

    std::string valueData{R"({"aaaaaaaaa":10000000000})"};
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = valueData;
    document.compress();

    switch (hasSnappySupport()) {
    case ClientSnappySupport::Everywhere:
    case ClientSnappySupport::Yes: {
        // Should be accepted.
        int successCount = getResponseCount(cb::mcbp::Status::Success);
        userConnection->mutate(document, Vbid(0), MutationType::Set);

        // Expect to get Snappy-compressed data back in passive mode
        // (as it keeps data compressed), uncompressed if in off mode.
        auto expectedDatatype = (mode == "off") ? expectedJSONDatatype()
                                                : expectedJSONSnappyDatatype();
        auto expectedValue = (mode == "off") ? valueData : document.value;
        verifyData(*userConnection,
                   successCount,
                   1,
                   expectedDatatype,
                   expectedValue);
        break;
    }

    case ClientSnappySupport::No:
        // Should fail as client didn't negotiate Snappy.
        try {
            userConnection->mutate(document, Vbid(0), MutationType::Set);
            FAIL() << "Should not accept datatype.Snappy document if client "
                      "didn't negotiate Snappy.";
        } catch (ConnectionError& error) {
            EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        }
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        GetSetTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        GetSetSnappyOnOffTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

TEST_P(GetSetTest, TestAdd) {
    userConnection->mutate(document, Vbid(0), MutationType::Add);

    int eExistsCount = getResponseCount(cb::mcbp::Status::KeyEexists);
    // Adding it one more time should fail
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Add);
        FAIL() << "It should not be possible to add a document that exists";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eExistsCount + 1,
                  getResponseCount(cb::mcbp::Status::KeyEexists));
    }

    // Add with a cas should fail
    int invalCount = getResponseCount(cb::mcbp::Status::Einval);
    try {
        document.info.cas = cb::mcbp::cas::Wildcard + 1;
        userConnection->mutate(document, Vbid(0), MutationType::Add);
        FAIL() << "It should not be possible to add a document that exists";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(invalCount + 1, getResponseCount(cb::mcbp::Status::Einval));
    }
}

TEST_P(GetSetTest, TestReplace) {
    // Replacing a nonexisting document should fail
    int eNoentCount = getResponseCount(cb::mcbp::Status::KeyEnoent);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Replace);
        FAIL() << "It's not possible to replace a nonexisting document";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eNoentCount + 1,
                  getResponseCount(cb::mcbp::Status::KeyEnoent));
    }

    userConnection->mutate(document, Vbid(0), MutationType::Add);
    // Replace this time should be fine!
    auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Replace);

    // Replace with invalid cas should fail
    document.info.cas = info.cas + 1;

    int eExistsCount = getResponseCount(cb::mcbp::Status::KeyEexists);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Replace);
        FAIL() << "replace with CAS mismatch should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eExistsCount + 1,
                  getResponseCount(cb::mcbp::Status::KeyEexists));
    }

    // Trying to replace a deleted document should also fail
    userConnection->remove(name, Vbid(0), 0);
    document.info.cas = 0;
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Replace);
        FAIL() << "It's not possible to replace a nonexisting document (deleted)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }

    // And CAS replace
    document.info.cas = 1;
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Replace);
        FAIL() << "It's not possible to replace a nonexisting document (deleted)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }

}

TEST_P(GetSetTest, TestSet) {
    // Set should fail if the key doesn't exists and we're using CAS
    document.info.cas = 1;

    int eNoentCount = getResponseCount(cb::mcbp::Status::KeyEnoent);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Set);
        FAIL() << "Set with CAS and no such doc should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
        EXPECT_EQ(eNoentCount + 1,
                  getResponseCount(cb::mcbp::Status::KeyEnoent));
    }

    int successCount = getResponseCount(cb::mcbp::Status::Success);
    // set should work even if a nonexisting document should fail
    document.info.cas = cb::mcbp::cas::Wildcard;
    userConnection->mutate(document, Vbid(0), MutationType::Set);

    // And it should be possible to set it once more
    auto info = userConnection->mutate(document, Vbid(0), MutationType::Set);

    // And it should be possible to set it with a CAS
    document.info.cas = info.cas;
    info = userConnection->mutate(document, Vbid(0), MutationType::Set);
    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 4, getResponseCount(cb::mcbp::Status::Success));

    // Replace with invalid cas should fail
    document.info.cas = info.cas + 1;

    int eExistsCount = getResponseCount(cb::mcbp::Status::KeyEexists);

    try {
        userConnection->mutate(document, Vbid(0), MutationType::Replace);
        FAIL() << "set with CAS mismatch should fail!";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eExistsCount + 1,
                  getResponseCount(cb::mcbp::Status::KeyEexists));
    }
}

TEST_P(GetSetTest, TestGetMiss) {
    int eNoentCount = getResponseCount(cb::mcbp::Status::KeyEnoent);
    try {
        userConnection->get("TestGetMiss", Vbid(0));
        FAIL() << "Expected TestGetMiss to throw an exception";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(eNoentCount + 1,
                  getResponseCount(cb::mcbp::Status::KeyEnoent));
    }
}

TEST_P(GetSetTest, TestGetSuccess) {
    userConnection->mutate(document, Vbid(0), MutationType::Set);

    int successCount = getResponseCount(cb::mcbp::Status::Success);
    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, expectedJSONSnappyDatatype()));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 2, getResponseCount(cb::mcbp::Status::Success));

    EXPECT_NE(cb::mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(document.value, stored.value);
}

TEST_P(GetSetTest, TestAppend) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";
    int successCount = getResponseCount(cb::mcbp::Status::Success);
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    document.value = "b";
    userConnection->mutate(document, Vbid(0), MutationType::Append);

    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 4, getResponseCount(cb::mcbp::Status::Success));

    EXPECT_NE(cb::mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ab"), stored.value);
}

// Check that APPEND correctly maintains JSON datatype if we append something
// which keeps it JSON.
TEST_P(GetSetTest, TestAppendJsonToJson) {
    // Create a documement which is a valid JSON number.
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "10";
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    auto stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(expectedJSONDatatype(), stored.info.datatype);

    // Now append another digit to it - should still be valid JSON.
    document.value = '1';
    userConnection->mutate(document, Vbid(0), MutationType::Append);
    stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(expectedJSONDatatype(), stored.info.datatype);
    EXPECT_EQ("101", stored.value);
}

// Check that APPEND correctly sets JSON datatype if we append something to
// a binary doc which makes it JSON.
TEST_P(GetSetTest, TestAppendRawToJson) {
    // Create a documement which is not valid JSON (yet).
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "[1";
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    auto stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);

    // Now append closing square bracket - should become valid JSON array.
    document.value = ']';
    userConnection->mutate(document, Vbid(0), MutationType::Append);
    stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(expectedJSONDatatype(), stored.info.datatype);
    EXPECT_EQ("[1]", stored.value);
}

// Check that PREPEND correctly maintains JSON datatype if we append something
// which keeps it JSON.
TEST_P(GetSetTest, TestPrependJsonToJson) {
    // Create a documement which is a valid JSON number.
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "10";
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    auto stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(expectedJSONDatatype(), stored.info.datatype);

    // Now prepend another digit to it - should still be valid JSON.
    document.value = '1';
    userConnection->mutate(document, Vbid(0), MutationType::Prepend);
    stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(expectedJSONDatatype(), stored.info.datatype);
    EXPECT_EQ("110", stored.value);
}

// Check that PREPEND correctly sets JSON datatype if we append something to
// a binary doc which makes it JSON.
TEST_P(GetSetTest, TestPrependRawToJson) {
    // Create a documement which is not valid JSON (yet).
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "1]";
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    auto stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(cb::mcbp::Datatype::Raw, stored.info.datatype);

    // Now prepend closing square bracket - should become valid JSON array.
    document.value = '[';
    userConnection->mutate(document, Vbid(0), MutationType::Prepend);
    stored = userConnection->get(name, Vbid(0));
    EXPECT_EQ(expectedJSONDatatype(), stored.info.datatype);
    EXPECT_EQ("[1]", stored.value);
}

TEST_P(GetSetTest, TestAppendWithXattr) {
    // The current code does not preserve XATTRs
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";
    int sucCount = getResponseCount(cb::mcbp::Status::Success);
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getDataView());

    document.value = "b";
    userConnection->mutate(document, Vbid(0), MutationType::Append);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas.getDataView(), getXattr("meta.cas").getDataView());

    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat.
    // * We expect testSuccessCount successes for each command we ran
    // * Plus 1 more success to account for the stat call in the first
    //   getResponseCount
    int testSuccessCount = 6;
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        // We had 3x xattr operations fail (1x createXattr 2x getXattr)
        testSuccessCount = 3;
    }
    EXPECT_EQ(sucCount + testSuccessCount + 1,
              getResponseCount(cb::mcbp::Status::Success));

    // And the rest of the doc should look the same
    EXPECT_NE(cb::mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ab"), stored.value);
}


TEST_P(GetSetTest, TestAppendCasSuccess) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int successCount = getResponseCount(cb::mcbp::Status::Success);
    const auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas;
    userConnection->mutate(document, Vbid(0), MutationType::Append);

    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 4, getResponseCount(cb::mcbp::Status::Success));

    EXPECT_NE(info.cas, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ab"), stored.value);
}

TEST_P(GetSetTest, TestAppendCasMismatch) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    const auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas + 1;
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Append);
        FAIL() << "Append with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }

    // verify it didn't change..
    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    EXPECT_EQ(info.cas, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("a"), stored.value);
}

TEST_P(GetSetTest, TestPrepend) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int successCount = getResponseCount(cb::mcbp::Status::Success);
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    document.value = "b";
    userConnection->mutate(document, Vbid(0), MutationType::Prepend);

    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 4, getResponseCount(cb::mcbp::Status::Success));

    EXPECT_NE(cb::mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ba"), stored.value);
}

TEST_P(GetSetTest, TestPrependWithXattr) {
    // The current code does not preserve XATTRs
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int sucCount = getResponseCount(cb::mcbp::Status::Success);

    userConnection->mutate(document, Vbid(0), MutationType::Add);
    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getDataView());

    document.value = "b";
    userConnection->mutate(document, Vbid(0), MutationType::Prepend);

    // The xattr should have been preserved, and the macro should not
    // be expanded more than once..
    EXPECT_EQ(mutation_cas.getDataView(), getXattr("meta.cas").getDataView());

    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat.
    // * We expect helloResps because of getConnection in getResponseCount()
    // * We expect testSuccessCount successes for each command we ran
    // * Plus 1 more success to account for the stat call in the first
    //   getResponseCount
    int testSuccessCount = 6;
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        // We had xattr operations fail (1x createXattr 2x getXattr)
        testSuccessCount = 3;
    }
    EXPECT_EQ(sucCount + testSuccessCount + 1,
              getResponseCount(cb::mcbp::Status::Success));

    // And the rest of the doc should look the same
    EXPECT_NE(cb::mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ba"), stored.value);
}

TEST_P(GetSetTest, TestPrependCasSuccess) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    int successCount = getResponseCount(cb::mcbp::Status::Success);
    const auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas;
    userConnection->mutate(document, Vbid(0), MutationType::Prepend);

    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    // Check that we correctly increment the status counter stat
    EXPECT_EQ(successCount + 4, getResponseCount(cb::mcbp::Status::Success));

    EXPECT_NE(cb::mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("ba"), stored.value);
}

TEST_P(GetSetTest, TestPrependCasMismatch) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value = "a";

    const auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Set);
    document.value = "b";
    document.info.cas = info.cas + 1;
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Prepend);
        FAIL() << "Prepend with illegal CAS should fail";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }
    const auto stored = userConnection->get(name, Vbid(0));
    EXPECT_TRUE(hasCorrectDatatype(stored, cb::mcbp::Datatype::Raw));

    EXPECT_NE(cb::mcbp::cas::Wildcard, stored.info.cas);
    EXPECT_EQ(document.info.flags, stored.info.flags);
    EXPECT_EQ(document.info.id, stored.info.id);
    EXPECT_EQ(std::string("a"), stored.value);
}

TEST_P(GetSetTest, TestIllegalVbucket) {
    auto& conn = getConnection();
    conn.authenticate("@admin");
    mcd_env->getTestBucket().createBucket("bucket", {}, *adminConnection);
    conn.selectBucket("bucket");

    try {
        conn.get("TestGetInvalidVbucket", Vbid(1));
        FAIL() << "Expected fetch of item in illegal vbucket to throw an exception";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotMyVbucket()) << error.what();
    }

    conn.deleteBucket("bucket");
    conn.reconnect();
}

// Test if we have a compressed document that has xattrs and the client supports
// compression, then once the server uncompresses the doc (to strip off the
// xattrs) we remove the Snappy datatype from the document.
TEST_P(GetSetTest, TestCorrectWithXattrs) {
    // Create a compressed document with a body and xattr
    setBodyAndXattr("{\"TestField\":56788}", {{"_sync", "4543"}});
    ASSERT_TRUE(cb::mcbp::datatype::is_snappy(
            protocol_binary_datatype_t(document.info.datatype)));
    const auto stored = userConnection->get(name, Vbid(0));
    // The test requires the client to support compression
    ASSERT_TRUE(hasSnappySupport() == ClientSnappySupport::Yes);
    auto expected = hasJSONSupport() == ClientJSONSupport::Yes
                            ? cb::mcbp::Datatype::JSON
                            : cb::mcbp::Datatype::Raw;
    EXPECT_TRUE(hasCorrectDatatype(stored, expected));
}

// Test sending compressed raw data; check server handles correctly.
TEST_P(GetSetSnappyOnOffTest, TestCompressedData) {
    doTestCompressedRawData("off");
}

TEST_P(GetSetSnappyOnOffTest, TestCompressedJSON) {
    doTestCompressedJSON("off");
}

TEST_P(GetSetSnappyOnOffTest, TestCompressedDataInPassiveMode) {
    doTestCompressedRawData("passive");
}

TEST_P(GetSetSnappyOnOffTest, TestCompressedJSONInPassiveMode) {
    doTestCompressedJSON("passive");
}

TEST_P(GetSetSnappyOnOffTest, TestCompressedDataInActiveMode) {
    doTestCompressedRawData("active");
}

TEST_P(GetSetSnappyOnOffTest, TestCompressedJSONInActiveMode) {
    doTestCompressedJSON("active");
}

TEST_P(GetSetSnappyOnOffTest, TestInvalidCompressedData) {
    document.value = "uncompressed JSON string";
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    std::vector<char> input(1024);
    std::ranges::fill(input, 'a');

    // Replacing a nonexisting document should fail
    int einvalCount = getResponseCount(cb::mcbp::Status::Einval);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Set);
        FAIL() << "It's not possible to set uncompressed documents if the "
                  "datatype is set as SNAPPY";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(einvalCount + 1, getResponseCount(cb::mcbp::Status::Einval));
    }
}

// Test appending uncompressed data to an compresssed existing value
TEST_P(GetSetTest, TestAppendCompressedSource) {
    doTestAppend(/*compressedSource*/ true, /*compressedData*/ false);
}

// Test appending compressed data to an uncompresssed existing value
TEST_P(GetSetTest, TestAppendCompressedData) {
    doTestAppend(/*compressedSource*/ false, /*compressedData*/ true);
}

// Test appending compressed data to an compresssed existing value
TEST_P(GetSetTest, TestAppendCompressedSourceAndData) {
    doTestAppend(/*compressedSource*/ true, /*compressedData*/ true);
}

TEST_P(GetSetTest, TestAppendInvalidCompressedData) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.assign(1024, 'a');

    userConnection->mutate(document, Vbid(0), MutationType::Set);

    std::vector<char> input(1024);
    std::ranges::fill(input, 'b');
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    int einvalCount = getResponseCount(cb::mcbp::Status::Einval);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Append);
        FAIL() << "It's not possible to append uncompressed documents if the "
                  "datatype is set as SNAPPY";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(einvalCount + 1, getResponseCount(cb::mcbp::Status::Einval));
    }
}

TEST_P(GetSetTest, TestPrependCompressedSource) {
    doTestPrepend(/*compressedSource*/ true, /*compressedData*/ false);
}

TEST_P(GetSetTest, TestPrependCompressedData) {
    doTestPrepend(/*compressedSource*/ false, /*compressedData*/ true);
}

TEST_P(GetSetTest, TestPrependCompressedSourceCompressedData) {
    doTestPrepend(/*compressedSource*/ true, /*compressedData*/ true);
}

TEST_P(GetSetTest, TestPrependInvalidCompressedData) {
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.assign(1024, 'a');

    userConnection->mutate(document, Vbid(0), MutationType::Set);

    std::vector<char> input(1024);
    std::ranges::fill(input, 'b');
    document.info.datatype = cb::mcbp::Datatype::Snappy;

    int einvalCount = getResponseCount(cb::mcbp::Status::Einval);
    try {
        userConnection->mutate(document, Vbid(0), MutationType::Prepend);
        FAIL() << "It's not possible to prepend uncompressed documents if the "
                  "datatype is set as SNAPPY";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        // Check that we correctly increment the status counter stat
        EXPECT_EQ(einvalCount + 1, getResponseCount(cb::mcbp::Status::Einval));
    }
}

TEST_P(GetSetTest, TestGetMetaValidJSON) {
    doTestGetMetaValidJSON(/*compressedSource*/ false);
}

TEST_P(GetSetTest, TestGetMetaValidJSONCompressed) {
    doTestGetMetaValidJSON(/*compressedSource*/ true);
}

TEST_P(GetSetTest, TestGetMetaInvalidJSON) {
    // Set the document value to not-JSON, so memcached sets the datatype
    // as such.
    document.value = "[Invalid;JSON}";
    document.info.datatype = cb::mcbp::Datatype::Raw;
    auto expectedDatatype = PROTOCOL_BINARY_RAW_BYTES;
    if (hasSnappySupport() == ClientSnappySupport::Yes) {
        document.compress();
        expectedDatatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
    }
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    auto meta = userConnection->getMeta(
            document.info.id, Vbid(0), GetMetaVersion::V2);
    EXPECT_EQ(0, meta.getDeleted());
    EXPECT_EQ(expectedDatatype, meta.getDatatype());
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
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    auto meta = userConnection->getMeta(
            document.info.id, Vbid(0), GetMetaVersion::V1);
    uint32_t expected = gsl::narrow<uint32_t>(now) + seconds - 2;
    EXPECT_GE(meta.getExpiry(), expected);
    EXPECT_LE(meta.getExpiry(), expected + 3);

    // Case `expiry` > `num_seconds_in_a_month`
    document.info.expiration = gsl::narrow<uint32_t>(now) + 60;
    userConnection->mutate(document, Vbid(0), MutationType::Replace);
    meta = userConnection->getMeta(
            document.info.id, Vbid(0), GetMetaVersion::V1);
    EXPECT_EQ(meta.getExpiry(), document.info.expiration);
}

// Test that memcached correctly detects documents are JSON irrespective
// of what the client sets the datatype to.
TEST_P(GetSetTest, ServerDetectsJSON) {
    doTestServerDetectsJSON(/*compressedSource*/ false);
}
TEST_P(GetSetTest, ServerDetectsJSONCompressed) {
    doTestServerDetectsJSON(/*compressedSource*/ true);
}

// Test that memcached correctly detects documents are not JSON irrespective
// of what the client sets the datatype to.
TEST_P(GetSetTest, ServerDetectsNonJSON) {
    doTestServerDetectsNonJSON(/*compressedSource*/ false);
}
TEST_P(GetSetTest, ServerDetectsNonJSONCompressed) {
    doTestServerDetectsNonJSON(/*compressedSource*/ true);
}

// Test that memcached correctly stores documents as uncompressed
TEST_P(GetSetTest, ServerStoresUncompressed) {
    doTestServerStoresUncompressed(/*compressedSource*/true);
}

// Test that memcached rejects documents of size greater than
// max item size
TEST_P(GetSetTest, ServerRejectsLargeSize) {
    doTestServerRejectsLargeSize(/*compressedSource*/false);
}
// Test that memcached rejects compressed documents whose
// uncompressed size is greater than max item size
TEST_P(GetSetTest, ServerRejectsLargeSizeCompressed) {
    doTestServerRejectsLargeSize(/*compressedSource*/true);
}

// Test that memcached rejects documents with xattrs whose size
// is greater than max item size
TEST_P(GetSetTest, ServerRejectsLargeSizeWithXattr) {
    doTestServerRejectsLargeSizeWithXattr(/*compressedSource*/false);
}

// Test that memcached rejects compressed documents with xattrs of size
// greater than maximum item size
TEST_P(GetSetTest, ServerRejectsLargeSizeWithXattrCompressed) {
    doTestServerRejectsLargeSizeWithXattr(/*compressedSource*/true);
}

// Test get random key. The test always sets a key so it can run standalone,
// but if ran after other tests, the returned key could be any key stored in
// the bucket, this limits the expect statements we can use
void GetSetTest::doTestGetRandomKey(bool collections) {
    storeAndPersistItem(*userConnection, Vbid(0), "doTestGetRandomKey");

    if (collections) {
        userConnection->setFeatures({cb::mcbp::Feature::XERROR,
                                     cb::mcbp::Feature::Collections,
                                     cb::mcbp::Feature::SNAPPY,
                                     cb::mcbp::Feature::JSON});
    } else {
        userConnection->setFeatures({cb::mcbp::Feature::XERROR,
                                     cb::mcbp::Feature::SNAPPY,
                                     cb::mcbp::Feature::JSON});
    }

    const auto stored = userConnection->getRandomKey(Vbid(0));

    // reset the features
    prepare(*userConnection);

    try {
        CollectionID prefix(cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
                                    {reinterpret_cast<const uint8_t*>(
                                             stored.info.id.data()),
                                     stored.info.id.size()})
                                    .first);
        if (collections) {
            EXPECT_TRUE(prefix.isDefaultCollection());
        }
    } catch (const std::exception&) {
        EXPECT_FALSE(collections) << "Collections enabled, yet could not "
                                     "decode leb128 collection-ID from key";
    }
}

TEST_P(GetSetTest, TestGetRandomKey) {
    doTestGetRandomKey(false);
}

TEST_P(GetSetTest, TestGetRandomKeyCollections) {
    doTestGetRandomKey(true);
}
