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
#include <gtest/gtest.h>

#include <xattr/blob.h>
#include <xattr/utils.h>

static cb::const_byte_buffer to_const_byte_buffer(const char* key) {
    return {reinterpret_cast<const uint8_t*>(key), strlen(key)};
}

static std::string to_string(cb::byte_buffer buf) {
    return std::string(reinterpret_cast<const char*>(buf.buf), buf.len);
}

void validate(cb::byte_buffer buffer) {
    EXPECT_TRUE(cb::xattr::validate(
            {reinterpret_cast<const char*>(buffer.buf), buffer.len}));
}

TEST(XattrBlobBlob, TestBlob) {
    cb::xattr::Blob blob;

    // Get from an empty buffer should return an empty value
    auto value = to_string(blob.get(to_const_byte_buffer("_sync")));
    EXPECT_TRUE(value.empty());

    // Add a couple of values
    blob.set(to_const_byte_buffer("user"),
             to_const_byte_buffer("{\"author\":\"bubba\"}"));
    blob.set(to_const_byte_buffer("_sync"),
             to_const_byte_buffer("{\"cas\":\"0xdeadbeefcafefeed\"}"));
    blob.set(to_const_byte_buffer("meta"),
             to_const_byte_buffer("{\"content-type\":\"text\"}"));

    // Validate the the blob is correctly built
    validate(blob.finalize());

    // Try to fetch all of the values
    EXPECT_EQ(std::string{"{\"cas\":\"0xdeadbeefcafefeed\"}"},
              to_string(blob.get(to_const_byte_buffer("_sync"))));
    EXPECT_EQ(std::string{"{\"author\":\"bubba\"}"},
              to_string(blob.get(to_const_byte_buffer("user"))));
    EXPECT_EQ(std::string{"{\"content-type\":\"text\"}"},
              to_string(blob.get(to_const_byte_buffer("meta"))));

    // Change the order of some bytes (that should just do an in-place
    // replacement)
    blob.set(to_const_byte_buffer("_sync"),
             to_const_byte_buffer("{\"cas\":\"0xcafefeeddeadbeef\"}"));
    validate(blob.finalize());

    // Try to fetch all of the values
    EXPECT_EQ(std::string{"{\"cas\":\"0xcafefeeddeadbeef\"}"},
              to_string(blob.get(to_const_byte_buffer("_sync"))));
    EXPECT_EQ(std::string{"{\"author\":\"bubba\"}"},
              to_string(blob.get(to_const_byte_buffer("user"))));
    EXPECT_EQ(std::string{"{\"content-type\":\"text\"}"},
              to_string(blob.get(to_const_byte_buffer("meta"))));

    // Remove one
    blob.remove(to_const_byte_buffer("meta"));
    validate(blob.finalize());
    value = to_string(blob.get(to_const_byte_buffer("meta")));
    EXPECT_TRUE(value.empty());

    // Try to fetch all of the values
    EXPECT_EQ(std::string{"{\"cas\":\"0xcafefeeddeadbeef\"}"},
              to_string(blob.get(to_const_byte_buffer("_sync"))));
    EXPECT_EQ(std::string{"{\"author\":\"bubba\"}"},
              to_string(blob.get(to_const_byte_buffer("user"))));

    // remove the last ones
    blob.remove(to_const_byte_buffer("user"));
    EXPECT_TRUE(to_string(blob.get(to_const_byte_buffer("user"))).empty());
    blob.remove(to_const_byte_buffer("_sync"));
    EXPECT_TRUE(to_string(blob.get(to_const_byte_buffer("_sync"))).empty());

    // An empty buffer should be finalized to size 0
    const auto last = blob.finalize();
    EXPECT_EQ(0, last.len);
}

TEST(XattrBlobBlob, TestPruneUser) {
    cb::xattr::Blob blob;

    // Add a single system xattr
    blob.set(to_const_byte_buffer("_sync"),
             to_const_byte_buffer("{\"cas\":\"0xdeadbeefcafefeed\"}"));
    blob.set(to_const_byte_buffer("_rbac"),
             to_const_byte_buffer("{\"foo\":\"bar\"}"));

    const auto systemsize = blob.finalize().len;
    EXPECT_NE(0, systemsize);

    // Add a couple of user xattrs
    blob.set(to_const_byte_buffer("user"),
             to_const_byte_buffer("{\"author\":\"bubba\"}"));
    blob.set(to_const_byte_buffer("meta"),
             to_const_byte_buffer("{\"content-type\":\"text\"}"));

    // And I know that when I change something that won't fit in the
    // current place it'll be moved to the end. Let's modify one of
    // the keys..
    blob.set(to_const_byte_buffer("_rbac"),
             to_const_byte_buffer("{\"auth\":\"needed\"}"));
    // and then set it back so that the size should be the same..
    blob.set(to_const_byte_buffer("_rbac"),
             to_const_byte_buffer("{\"foo\":\"bar\"}"));
    validate(blob.finalize());
    EXPECT_LT(systemsize, blob.finalize().len);

    // Now prune off the user keys (we should have a system xattr first and
    // and last)
    blob.prune_user_keys();
    validate(blob.finalize());

    // And we should be back at the size we had before adding all of the
    // user xattr
    EXPECT_EQ(systemsize, blob.finalize().len);
    EXPECT_EQ(systemsize, blob.get_system_size());

    // and we should be able to get the system xattr's
    EXPECT_EQ(std::string{"{\"cas\":\"0xdeadbeefcafefeed\"}"},
              to_string(blob.get(to_const_byte_buffer("_sync"))));
    EXPECT_EQ(std::string{"{\"foo\":\"bar\"}"},
              to_string(blob.get(to_const_byte_buffer("_rbac"))));
}
