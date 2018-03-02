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

#include "utilities/string_utilities.h"

void validate(cb::char_buffer buffer) {
    EXPECT_TRUE(cb::xattr::validate(
            {static_cast<const char*>(buffer.data()), buffer.size()}));
}

TEST(XattrBlob, TestBlob) {
    cb::xattr::Blob blob;

    // Get from an empty buffer should return an empty value
    auto value = to_string(blob.get("_sync"));
    EXPECT_TRUE(value.empty());

    // Add a couple of values
    blob.set("user", "{\"author\":\"bubba\"}");
    blob.set("_sync", "{\"cas\":\"0xdeadbeefcafefeed\"}");
    blob.set("meta", "{\"content-type\":\"text\"}");

    // Validate the the blob is correctly built
    validate(blob.finalize());

    // Try to fetch all of the values
    EXPECT_EQ(std::string{"{\"cas\":\"0xdeadbeefcafefeed\"}"},
              to_string(blob.get("_sync")));
    EXPECT_EQ(std::string{"{\"author\":\"bubba\"}"},
              to_string(blob.get("user")));
    EXPECT_EQ(std::string{"{\"content-type\":\"text\"}"},
              to_string(blob.get("meta")));

    // Change the order of some bytes (that should just do an in-place
    // replacement)
    blob.set("_sync", "{\"cas\":\"0xcafefeeddeadbeef\"}");
    validate(blob.finalize());

    // Try to fetch all of the values
    EXPECT_EQ(std::string{"{\"cas\":\"0xcafefeeddeadbeef\"}"},
              to_string(blob.get("_sync")));
    EXPECT_EQ(std::string{"{\"author\":\"bubba\"}"},
              to_string(blob.get("user")));
    EXPECT_EQ(std::string{"{\"content-type\":\"text\"}"},
              to_string(blob.get("meta")));

    // Remove one
    blob.remove("meta");
    validate(blob.finalize());
    value = to_string(blob.get("meta"));
    EXPECT_TRUE(value.empty());

    // Try to fetch all of the values
    EXPECT_EQ(std::string{"{\"cas\":\"0xcafefeeddeadbeef\"}"},
              to_string(blob.get("_sync")));
    EXPECT_EQ(std::string{"{\"author\":\"bubba\"}"},
              to_string(blob.get("user")));

    // remove the last ones
    blob.remove("user");
    EXPECT_TRUE(to_string(blob.get("user")).empty());
    blob.remove("_sync");
    EXPECT_TRUE(to_string(blob.get("_sync")).empty());

    // An empty buffer should be finalized to size 0
    const auto last = blob.finalize();
    EXPECT_EQ(0, last.len);
}

TEST(XattrBlob, TestPruneUser) {
    cb::xattr::Blob blob;

    // Add a single system xattr
    blob.set("_sync", "{\"cas\":\"0xdeadbeefcafefeed\"}");
    blob.set("_rbac", "{\"foo\":\"bar\"}");

    const auto systemsize = blob.finalize().len;
    EXPECT_NE(0, systemsize);

    // Add a couple of user xattrs
    blob.set("user", "{\"author\":\"bubba\"}");
    blob.set("meta", "{\"content-type\":\"text\"}");

    // And I know that when I change something that won't fit in the
    // current place it'll be moved to the end. Let's modify one of
    // the keys..
    blob.set("_rbac", "{\"auth\":\"needed\"}");
    // and then set it back so that the size should be the same..
    blob.set("_rbac", "{\"foo\":\"bar\"}");
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
              to_string(blob.get("_sync")));
    EXPECT_EQ(std::string{"{\"foo\":\"bar\"}"}, to_string(blob.get("_rbac")));
}

TEST(XattrBlob, TestToJson) {
    cb::xattr::Blob blob;
    blob.set("_sync",
             "{\"cas\":\"0xdeadbeefcafefeed\", "
             "\"user\":\"trond\"}");
    blob.set("_rbac", "{\"foo\":\"bar\"}");

    const std::string expected{"{\"_sync\":{\"cas\":\"0xdeadbeefcafefeed\","
                                   "\"user\":\"trond\"},"
                                   "\"_rbac\":{\"foo\":\"bar\"}}"};
    EXPECT_EQ(expected, to_string(blob.to_json(), false));
}

/**
 * Verify that get(key) check that it is an exact match for
 * a key, and not just a substring of a key
 */
TEST(XattrBlob, MB_22691) {
    cb::xattr::Blob blob;

    // Add a couple of values
    blob.set(std::string("integer_extra"), std::string("1"));

    // Validate the the blob is correctly built
    validate(blob.finalize());

    auto value = blob.get("integer");
    EXPECT_EQ(0, value.len);

    const std::vector<std::string> keys = {"start", "integer", "in", "int",
                                           "double", "for", "try", "as",
                                           "while", "else", "end"};
    for (const auto& key : keys) {
        blob.set(key, std::string("1"));
    }

    for (const auto& key : keys) {
        auto entry = blob.get(key);
        EXPECT_FALSE(entry.empty()) << "Key: " << key << " is missing";
    }
}

TEST(XattrBlob, iterator_simple_checks) {
    cb::xattr::Blob blob;
    EXPECT_EQ(blob.begin(), blob.end());
    std::vector<std::string> keys = {"key1", "key2", "key3"};
    for (auto& k : keys) {
        blob.set(k, k + ".value");
    }
    EXPECT_NE(blob.begin(), blob.end());

    // Check some loop varieties
    int iterations = 0;
    for (auto itr = blob.begin(); itr != blob.end(); itr++) {
        iterations++;
    }
    EXPECT_EQ(keys.size() * 1, iterations);
    for (auto itr = blob.begin(); itr != blob.end(); ++itr) {
        iterations++;
    }
    EXPECT_EQ(keys.size() * 2, iterations);
    for (auto kv : blob) {
        (void)kv;
        iterations++;
    }
    EXPECT_EQ(keys.size() * 3, iterations);

    // Check we get an end iterator when we keep increasing
    auto itr = blob.begin();
    for (const auto& k: keys) {
        (void)k;
        itr++;
    }
    itr++;
    EXPECT_EQ(itr, blob.end());

    auto kItr = keys.begin();
    iterations = 0;
    for (auto kv : blob) {
        iterations++;
        EXPECT_EQ(*kItr, to_string(kv.first));
        EXPECT_EQ(*kItr + ".value", to_string(kv.second));
        kItr++;
    }
}

TEST(XattrBlob, iterator_insert) {
    cb::xattr::Blob blob;
    std::vector<std::string> keys = {"key1", "key2", "key3"};
    for (auto& k : keys) {
        blob.set(k, k + ".value");
    }

    auto kItr = keys.begin();
    for (auto itr = blob.begin(); itr != blob.end(); itr++) {
        if (to_string((*itr).first) == "key2") {
            blob.set("inserted", "inserted.value");
        }

        if (kItr != keys.end()) {
            EXPECT_EQ(*kItr, to_string((*itr).first));
            kItr++;
        } else {
            EXPECT_EQ("inserted", to_string((*itr).first));
        }
    }
}
