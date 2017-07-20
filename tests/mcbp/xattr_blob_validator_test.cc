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
#include "config.h"

#include <gtest/gtest.h>
#include <xattr/utils.h>

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

        uint32_t len = htonl(uint32_t(blob.size() - (offset + 4)));
        memcpy(blob.data() + offset, &len, 4);

        // Update the root block
        len = htonl(blob.size() - 4);
        memcpy(blob.data(), &len, 4);
    }

    cb::const_char_buffer getBuffer() {
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
    addKvPair("_sync", "{ \"foo\" : \"bar\" }");
    EXPECT_TRUE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrMultipleKV) {
    for (int ii = 0; ii < 100; ++ii) {
        addKvPair("_sync" + std::to_string(ii), "{ \"foo\" : \"bar\" }");
    }
    EXPECT_TRUE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrInvalidRootLength) {
    addKvPair("_sync", "{ \"foo\" : \"bar\" }");
    size_t len;

    // One byte too long
    memcpy(&len, blob.data(), 4);
    len = htonl(ntohl(len) + 1);
    memcpy(blob.data(), &len, 4);
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));

    // A byte too short
    len = htonl(ntohl(len) - 2);
    memcpy(blob.data(), &len, 4);
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrInvalidKeyLength) {
    addKvPair("_sync", "{ \"foo\" : \"bar\" }");
    size_t len;

    // One byte too long
    memcpy(&len, blob.data() + 4, 4);
    len = htonl(ntohl(len) + 1);
    memcpy(blob.data() + 4, &len, 4);
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));

    // A byte too short
    len = htonl(ntohl(len) - 2);
    memcpy(blob.data() + 4, &len, 4);
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrKeyNotTerminated) {
    addKvPair("_sync", "{ \"foo\" : \"bar\" }");

    // 4 byte header, 4 byte kv header, 5 characters keyname
    EXPECT_EQ(0, blob[13]);
    blob[13] = 'a';
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrValueNotTerminated) {
    addKvPair("_sync", "{ \"foo\" : \"bar\" }");

    EXPECT_EQ(0, blob.back());
    blob.back() = 'a';
    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}

TEST_F(XattrValidatorTest, TestXattrDuplicateKeysNotAllowed) {
    addKvPair("_sync", "{ \"foo\" : \"bar\" }");
    addKvPair("_sync", "{ \"foo\" : \"bar\" }");

    EXPECT_FALSE(cb::xattr::validate(getBuffer()));
}
