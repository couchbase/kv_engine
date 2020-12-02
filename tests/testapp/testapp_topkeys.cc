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
int get_topkeys_legacy_value(const std::string& wanted_key) {
    BinprotGenericCommand st(ClientOpcode::Stat, "topkeys");
    std::vector<uint8_t> blob;
    st.encode(blob);
    safe_send(blob);

    // We expect a variable number of response packets (one per top key);
    // take them all off the wire, recording the one for our key which we
    // parse at the end.
    std::string value;
    while (true) {
        if (!safe_recv_packet(blob)) {
            ADD_FAILURE() << "Failed to receive topkeys packet";
            return -1;
        }
        auto& response = *reinterpret_cast<Response*>(blob.data());
        mcbp_validate_response_header(
                response, ClientOpcode::Stat, Status::Success);

        auto k = response.getKey();
        // A packet with key length zero indicates end of the stats.
        if (k.empty()) {
            break;
        }
        const auto key =
                std::string{reinterpret_cast<const char*>(k.data()), k.size()};
        if (key == wanted_key) {
            // Got our key. Save the value to one side; and finish consuming
            // the STAT reponse packets.
            EXPECT_TRUE(value.empty())
                    << "Unexpectedly found a second topkey for wanted key '"
                    << wanted_key;

            auto val = response.getValue();
            EXPECT_FALSE(val.empty());
            value = std::string{reinterpret_cast<const char*>(val.data()),
                                val.size()};
        }
    }

    if (!value.empty()) {
        // Extract the 'get_hits' stat (which actually the aggregate of all
        // operations now).
        const std::string token("get_hits=");
        auto pos = value.find(token);
        EXPECT_NE(std::string::npos, pos)
                << "Failed to locate '" << token << "' substring in topkey '"
                << wanted_key << "' value '" << value << "'";

        // Move iterator to the other side of the equals sign (the value) and
        // erase before that point.
        pos += token.size();
        value.erase(0, pos);

        return std::stoi(value);
    } else {
        // If we got here then we failed to find the given key.
        return 0;
    }
}

/**
 * Accesses the current value of the key via the JSON formatted topkeys return.
 * @return True if the specified key was found (and sets count to the
 *         keys' access count) or false if not found.
 */
bool get_topkeys_json_value(const std::string& key, int& count) {
    BinprotGenericCommand st(ClientOpcode::Stat, "topkeys_json");
    std::vector<uint8_t> blob;
    st.encode(blob);
    safe_send(blob);

    // Expect 1 valid packet followed by 1 null
    if (!safe_recv_packet(blob)) {
        ADD_FAILURE() << "Failed to recv topkeys_json response";
        return false;
    }
    auto& response = *reinterpret_cast<Response*>(blob.data());
    mcbp_validate_response_header(
            response, ClientOpcode::Stat, Status::Success);

    EXPECT_NE(0, response.getKeylen());
    auto val = response.getValue();
    EXPECT_FALSE(val.empty());
    const std::string value(reinterpret_cast<const char*>(val.data()),
                            val.size());

    // Consume NULL stats packet.
    if (!safe_recv_packet(blob)) {
        ADD_FAILURE() << "Failed to recv null stats packet";
        return false;
    }
    response = *reinterpret_cast<Response*>(blob.data());
    mcbp_validate_response_header(
            response, ClientOpcode::Stat, Status::Success);
    EXPECT_EQ(0, response.getKeylen());

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
static void test_set_topkeys(const std::string& key, const int operations) {
    // In theory we should start with no record of a key; but there's no
    // explicit way to clear topkeys; and a previous test run against the same
    // memcached instance may have used the same key.
    // Therefore for robustness don't assume the key doesn't exist; and fetch
    // the initial count.
    int initial_count = 0;
    get_topkeys_json_value(key, initial_count);

    BinprotMutationCommand mut;
    mut.setMutationType(MutationType::Set);
    mut.setKey(key);
    std::vector<uint8_t> set_command;
    mut.encode(set_command);

    // Send CMD_SET for current key 'sum' number of times (and validate
    // response).
    for (int ii = 0; ii < operations; ii++) {
        safe_send(set_command);
        std::vector<uint8_t> blob;
        ASSERT_TRUE(safe_recv_packet(blob));
        mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                      ClientOpcode::Set,
                                      Status::Success);
    }

    EXPECT_EQ(initial_count + operations, get_topkeys_legacy_value(key));
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(initial_count + operations, json_value);
}

/**
 * Get a key a number of times and assert that the return value matches the
 * change after the number of get operations.
 */
static void test_get_topkeys(const std::string& key, int operations) {
    int initial_count = 0;
    ASSERT_TRUE(get_topkeys_json_value(key, initial_count));

    BinprotGenericCommand get(ClientOpcode::Get, key);
    std::vector<uint8_t> get_command;
    get.encode(get_command);

    for (int ii = 0; ii < operations; ii++) {
        safe_send(get_command);
        std::vector<uint8_t> buffer;
        safe_recv_packet(buffer);
        mcbp_validate_response_header(
                *reinterpret_cast<Response*>(buffer.data()),
                ClientOpcode::Get,
                Status::Success);
    }

    const int expected_count = initial_count + operations;
    EXPECT_EQ(expected_count, get_topkeys_legacy_value(key))
            << "Unexpected topkeys legacy count for key:" << key;
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(expected_count, json_value);
}

/**
 * Delete a key and assert that the return value matches the change
 * after the delete operation.
 */
static void test_delete_topkeys(const std::string& key) {
    int initial_count = 0;
    ASSERT_TRUE(get_topkeys_json_value(key, initial_count));

    BinprotGenericCommand cmd(ClientOpcode::Delete, key);
    std::vector<uint8_t> blob;
    cmd.encode(blob);
    safe_send(blob);

    ASSERT_TRUE(safe_recv_packet(blob));
    mcbp_validate_response_header(*reinterpret_cast<Response*>(blob.data()),
                                  ClientOpcode::Delete,
                                  Status::Success);

    EXPECT_EQ(initial_count + 1, get_topkeys_legacy_value(key))
            << "Unexpected topkeys legacy count for key:" << key;
    int json_value = 0;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(initial_count + 1, json_value);
}

/**
 * Test for JSON document formatted topkeys (part of bucket_engine). Tests for
 * correct values when issuing CMD_SET, CMD_GET, and CMD_DELETE.
 */
TEST_P(McdTopkeysTest, test_topkeys) {
    /* Perform sets on a few different keys. */
    test_set_topkeys("key1", 1);
    test_set_topkeys("key2", 2);
    test_set_topkeys("key3", 3);

    test_get_topkeys("key1", 10);

    test_delete_topkeys("key1");
}

INSTANTIATE_TEST_SUITE_P(
        Transport,
        McdTopkeysTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
        McdTestappTest::PrintToStringCombinedName);
