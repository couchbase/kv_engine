/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include <folly/portability/GTest.h>
#include <mcbp/protocol/framebuilder.h>

using namespace cb::mcbp;

class McdTopkeysTest : public McdTestappTest {};

/**
 * Returns the current access count of the test key ("someval") as an integer
 * via the old string format.
 */
int get_topkeys_legacy_value(MemcachedConnection& conn,
                             const std::string& wanted_key) {
    int ret = 0;
    conn.stats(
            [&ret, &wanted_key](const std::string& k, const std::string& v) {
                if (k == wanted_key) {
                    // Extract the 'get_hits' stat (which actually the aggregate
                    // of all operations now).
                    auto value = v;
                    const std::string token("get_hits=");
                    auto pos = value.find(token);
                    EXPECT_NE(std::string::npos, pos)
                            << "Failed to locate '" << token
                            << "' substring in topkey '" << wanted_key
                            << "' value '" << value << "'";
                    // Move iterator to the other side of the equals sign (the
                    // value) and erase before that point.
                    pos += token.size();
                    value.erase(0, pos);
                    ret = std::stoi(value);
                }
            },
            "topkeys");
    return ret;
}

/**
 * Accesses the current value of the key via the JSON formatted topkeys return.
 * @return True if the specified key was found (and sets count to the
 *         keys' access count) or false if not found.
 */
bool get_topkeys_json_value(MemcachedConnection& conn,
                            const std::string& key,
                            int& count) {
    std::string value;
    conn.stats([&value](const std::string,
                        const std::string& v) { value.assign(v); },
               "topkeys_json");

    // Check for response string
    auto json = nlohmann::json::parse(value);

    auto topkeys = json["topkeys"];
    EXPECT_TRUE(topkeys.is_array());

    // Search the array for the specified key's information.
    for (auto record : topkeys) {
        if (key == record["key"]) {
            count = record["access_count"].get<int>();
            return true;
        }
    }

    return false;
}

/**
 * Set a key a number of times and assert that the return value matches the
 * change after the number of set operations.
 */
static void test_set_topkeys(MemcachedConnection& conn,
                             const std::string& key,
                             const int operations) {
    // In theory we should start with no record of a key; but there's no
    // explicit way to clear topkeys; and a previous test run against the same
    // memcached instance may have used the same key.
    // Therefore for robustness don't assume the key doesn't exist; and fetch
    // the initial count.
    int initial_count = 0;
    get_topkeys_json_value(conn, key, initial_count);

    // Send CMD_SET for current key 'sum' number of times (and validate
    // response).
    for (int ii = 0; ii < operations; ii++) {
        conn.store(key, Vbid{0}, key);
    }

    EXPECT_EQ(initial_count + operations, get_topkeys_legacy_value(conn, key));
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(conn, key, json_value));
    EXPECT_EQ(initial_count + operations, json_value);
}

/**
 * Get a key a number of times and assert that the return value matches the
 * change after the number of get operations.
 */
static void test_get_topkeys(MemcachedConnection& conn,
                             const std::string& key,
                             int operations) {
    int initial_count = 0;
    ASSERT_TRUE(get_topkeys_json_value(conn, key, initial_count));

    for (int ii = 0; ii < operations; ii++) {
        conn.get(key, Vbid{0});
    }

    const int expected_count = initial_count + operations;
    EXPECT_EQ(expected_count, get_topkeys_legacy_value(conn, key))
            << "Unexpected topkeys legacy count for key:" << key;
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(conn, key, json_value));
    EXPECT_EQ(expected_count, json_value);
}

/**
 * Delete a key and assert that the return value matches the change
 * after the delete operation.
 */
static void test_delete_topkeys(MemcachedConnection& conn,
                                const std::string& key) {
    int initial_count = 0;
    ASSERT_TRUE(get_topkeys_json_value(conn, key, initial_count));

    conn.remove(key, Vbid{0});

    EXPECT_EQ(initial_count + 1, get_topkeys_legacy_value(conn, key))
            << "Unexpected topkeys legacy count for key:" << key;
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(conn, key, json_value));
    EXPECT_EQ(initial_count + 1, json_value);
}

/**
 * Test for JSON document formatted topkeys (part of bucket_engine). Tests for
 * correct values when issuing CMD_SET, CMD_GET, and CMD_DELETE.
 */
TEST_P(McdTopkeysTest, test_topkeys) {
    /* Perform sets on a few different keys. */
    auto& conn = getConnection();
    test_set_topkeys(conn, "key1", 1);
    test_set_topkeys(conn, "key2", 2);
    test_set_topkeys(conn, "key3", 3);

    test_get_topkeys(conn, "key1", 10);

    test_delete_topkeys(conn, "key1");
}

INSTANTIATE_TEST_SUITE_P(
        Transport,
        McdTopkeysTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
        McdTestappTest::PrintToStringCombinedName);
