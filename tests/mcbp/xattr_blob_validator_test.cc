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
#include <folly/portability/GTest.h>
#include <xattr/utils.h>
#include <gsl/gsl>
#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

class XattrValidatorTest : public ::testing::Test {
public:
    XattrValidatorTest() : blob(4) {
    }

protected:
    void addKvPair(const std::string& key, const std::string& value) {
        auto offset = blob.size();
        // set aside room for the length
        blob.resize(offset + 4);

        std::copy(key.begin(), key.end(), std::back_inserter(blob));
        blob.push_back(0x00);
        std::copy(value.begin(), value.end(), std::back_inserter(blob));
        blob.push_back(0x00);

        uint32_t len = htonl(gsl::narrow<uint32_t>(blob.size() - (offset + 4)));
        memcpy(blob.data() + offset, &len, 4);

        // Update the root block
        len = htonl(gsl::narrow<uint32_t>(blob.size() - 4));
        memcpy(blob.data(), &len, 4);
    }

    std::string_view getBuffer() {
        return {blob.data(), blob.size()};
    }

    std::vector<char> blob;
};

TEST_F(XattrValidatorTest, TestEmptyXAttrBlob) {
    EXPECT_TRUE(cb::xattr::validate(getBuffer()));

    // We may also have data after the xattr blob
    blob.resize(100);
    EXPECT_TRUE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrSingleKV) {
    addKvPair("_sync", R"({ "foo" : "bar" })");
    EXPECT_TRUE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrMultipleKV) {
    for (int ii = 0; ii < 100; ++ii) {
        addKvPair("_sync" + std::to_string(ii), R"({ "foo" : "bar" })");
    }
    EXPECT_TRUE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrInvalidRootLength) {
    addKvPair("_sync", R"({ "foo" : "bar" })");
    uint32_t len;

    // One byte too long
    memcpy(&len, blob.data(), sizeof(len));
    len = htonl(ntohl(len) + 1);
    memcpy(blob.data(), &len, sizeof(len));
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));

    // A byte too short
    len = htonl(ntohl(len) - 2);
    memcpy(blob.data(), &len, sizeof(len));
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrInvalidKeyLength) {
    addKvPair("_sync", R"({ "foo" : "bar" })");
    uint32_t len;

    // One byte too long
    memcpy(&len, blob.data() + 4, sizeof(len));
    len = htonl(ntohl(len) + 1);
    memcpy(blob.data() + 4, &len, sizeof(len));
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));

    // A byte too short
    len = htonl(ntohl(len) - 2);
    memcpy(blob.data() + 4, &len, sizeof(len));
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrKeyNotTerminated) {
    addKvPair("_sync", R"({ "foo" : "bar" })");

    // 4 byte header, 4 byte kv header, 5 characters keyname
    EXPECT_EQ(0, blob[13]);
    blob[13] = 'a';
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrValueNotTerminated) {
    addKvPair("_sync", R"({ "foo" : "bar" })");

    EXPECT_EQ(0, blob.back());
    blob.back() = 'a';
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrDuplicateKeysNotAllowed) {
    addKvPair("_sync", R"({ "foo" : "bar" })");
    addKvPair("_sync", R"({ "foo" : "bar" })");

    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}
