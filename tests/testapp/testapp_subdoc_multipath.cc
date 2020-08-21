/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/*
 * Sub-document API multi-path tests
 */

#include "testapp_subdoc_common.h"

#include <nlohmann/json.hpp>

// Test multi-path lookup command - simple single SUBDOC_GET
TEST_P(SubdocTestappTest, SubdocMultiLookup_GetSingle) {
    store_document("dict", R"({"key1":1,"key2":"two", "key3":3.0})");

    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    lookup.specs.push_back(
            {cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "key1"});
    std::vector<SubdocMultiLookupResult> expected{
            {cb::mcbp::Status::Success, "1"}};
    expect_subdoc_cmd(lookup, cb::mcbp::Status::Success, expected);

    // Attempt to access non-existent key.
    lookup.key = "dictXXX";
    expected.clear();
    expect_subdoc_cmd(lookup, cb::mcbp::Status::KeyEnoent, expected);

    // Attempt to access non-existent path.
    lookup.key = "dict";
    lookup.specs.at(0) = {
            cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "keyXX"};
    expected.emplace_back(cb::mcbp::Status::SubdocPathEnoent, "");
    expect_subdoc_cmd(
            lookup, cb::mcbp::Status::SubdocMultiPathFailure, expected);

    delete_object("dict");
}

// Test multi-path lookup command - simple single SUBDOC_EXISTS
TEST_P(SubdocTestappTest, SubdocMultiLookup_ExistsSingle) {
    store_document("dict", R"({"key1":1,"key2":"two", "key3":3.0})");

    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    lookup.specs.push_back(
            {cb::mcbp::ClientOpcode::SubdocExists, SUBDOC_FLAG_NONE, "key1"});
    std::vector<SubdocMultiLookupResult> expected{
            {cb::mcbp::Status::Success, ""}};
    expect_subdoc_cmd(lookup, cb::mcbp::Status::Success, expected);

    delete_object("dict");
}

/* Creates a flat dictionary with the specified number of key/value pairs
 *   Keys are named "key_0", "key_1"...
 *   Values are strings of the form "value_0", value_1"...
 */
nlohmann::json make_flat_dict(int nelements) {
    nlohmann::json dict;
    for (int i = 0; i < nelements; i++) {
        dict[std::string{"key_"} + std::to_string(i)] =
                std::string{"value_"} + std::to_string(i);
    }
    return dict;
}

static void test_subdoc_multi_lookup_getmulti() {
    auto dict = make_flat_dict(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS + 1);
    store_document("dict", dict.dump());

    // Lookup the maximum number of allowed paths - should succeed.
    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    std::vector<SubdocMultiLookupResult> expected;
    for (int ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++)
    {
        std::string key("key_" + std::to_string(ii));
        std::string value("\"value_" + std::to_string(ii) + '"');
        lookup.specs.push_back(
                {cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, key});

        expected.emplace_back(cb::mcbp::Status::Success, value);
    }
    expect_subdoc_cmd(lookup, cb::mcbp::Status::Success, expected);

    // Add one more - should fail.
    lookup.specs.push_back(
            {cb::mcbp::ClientOpcode::SubdocGet,
             SUBDOC_FLAG_NONE,
             "key_" + std::to_string(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS)});
    expected.clear();
    expect_subdoc_cmd(lookup, cb::mcbp::Status::SubdocInvalidCombo, expected);
    reconnect_to_server();

    delete_object("dict");
}
// Test multi-path lookup - multiple GET lookups
TEST_P(SubdocTestappTest, SubdocMultiLookup_GetMulti) {
    test_subdoc_multi_lookup_getmulti();
}

// Test multi-path lookup - multiple GET lookups with various invalid paths.
TEST_P(SubdocTestappTest, SubdocMultiLookup_GetMultiInvalid) {
    store_document("dict", R"({"key1":1,"key2":"two","key3":[0,1,2]})");

    // Build a multi-path LOOKUP with a variety of invalid paths
    std::vector<std::pair<std::string, cb::mcbp::Status> > bad_paths({
            {"[0]", cb::mcbp::Status::SubdocPathMismatch},
            {"key3[3]", cb::mcbp::Status::SubdocPathEnoent},
    });

    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    std::vector<SubdocMultiLookupResult> expected;
    for (const auto& path : bad_paths) {
        lookup.specs.push_back({cb::mcbp::ClientOpcode::SubdocGet,
                                SUBDOC_FLAG_NONE,
                                path.first});
        expected.emplace_back(path.second, "");
    }
    expect_subdoc_cmd(
            lookup, cb::mcbp::Status::SubdocMultiPathFailure, expected);

    delete_object("dict");
}

// Test multi-path lookup - multiple EXISTS lookups
TEST_P(SubdocTestappTest, SubdocMultiLookup_ExistsMulti) {
    auto dict = make_flat_dict(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS + 1);
    store_document("dict", dict.dump());

    // Lookup the maximum number of allowed paths - should succeed.
    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    std::vector<SubdocMultiLookupResult> expected;
    for (int ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++) {
        std::string key("key_" + std::to_string(ii));

        lookup.specs.push_back(
                {cb::mcbp::ClientOpcode::SubdocExists, SUBDOC_FLAG_NONE, key});
        expected.emplace_back(cb::mcbp::Status::Success, "");
    }
    expect_subdoc_cmd(lookup, cb::mcbp::Status::Success, expected);

    // Add one more - should fail.
    lookup.specs.push_back(
            {cb::mcbp::ClientOpcode::SubdocExists,
             SUBDOC_FLAG_NONE,
             "key_" + std::to_string(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS)});
    expected.clear();
    expect_subdoc_cmd(lookup, cb::mcbp::Status::SubdocInvalidCombo, expected);
    reconnect_to_server();

    delete_object("dict");
}

/******************* Multi-path mutation tests *******************************/

// Test multi-path mutation command - simple single SUBDOC_DICT_ADD
TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddSingle) {
    store_document("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key",
                              "\"value\""});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    // Check the update actually occurred.
    validate_json_document("dict", R"({"key":"value"})");

    delete_object("dict");
}

// Test multi-path mutation command - simple multiple SUBDOC_DICT_ADD
TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddMulti) {
    store_document("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key1",
                              "1"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key2",
                              "2"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key3",
                              "3"});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    // Check the update actually occurred.
    validate_json_document("dict", R"({"key1":1,"key2":2,"key3":3})");

    delete_object("dict");
}

// Test multi-path mutation command - test maximum supported SUBDOC_DICT_ADD
// paths.
static void test_subdoc_multi_mutation_dict_add_max() {
    store_document("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    for (int ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++) {
        std::string path("key_" + std::to_string(ii));
        std::string value("\"value_" + std::to_string(ii) + '"');

        mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                                  SUBDOC_FLAG_NONE,
                                  path,
                                  value});
    }
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    // Check the update actually occurred.
    auto dict = make_flat_dict(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS);
    validate_json_document("dict", dict.dump());

    delete_object("dict");

    // Try with one more mutation spec - should fail and document should be
    // unmodified.
    store_document("dict", "{}");
    auto max_id = std::to_string(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS);
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key_" + max_id,
                              "\"value_" + max_id + '"'});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::SubdocInvalidCombo, {});

    reconnect_to_server();

    // Document should be unmodified.
    validate_json_document("dict", "{}");

    delete_object("dict");
}
TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddMax) {
    test_subdoc_multi_mutation_dict_add_max();
}

// Test attempting to add the same key twice in a multi-path command.
TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddInvalidDuplicate) {
    store_document("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key",
                              "\"value\""});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key",
                              "\"value2\""});
    // Should return failure, with the index of the failing op (1).
    expect_subdoc_cmd(mutation,
                      cb::mcbp::Status::SubdocMultiPathFailure,
                      {{1, cb::mcbp::Status::SubdocPathEexists}});

    // Document should be unmodified.
    validate_json_document("dict", "{}");

    delete_object("dict");
}

// Test multi-path mutation command - 2x DictAdd with a Counter update
TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddCounter) {
    store_document("dict", R"({"count":0,"items":{}})");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "items.foo",
                              "1"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "items.bar",
                              "2"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocCounter,
                              SUBDOC_FLAG_NONE,
                              "count",
                              "2"});
    expect_subdoc_cmd(mutation,
                      cb::mcbp::Status::Success,
                      {{2, cb::mcbp::Status::Success, "2"}});

    // Check the update actually occurred.
    validate_json_document("dict",
                           R"({"count":2,"items":{"foo":1,"bar":2}})");

    delete_object("dict");
}

// Test multi-path mutation command - 2x DictAdd with specific CAS.
TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddCAS) {
    store_document("dict", "{\"int\":1}");

    // Use SUBDOC_EXISTS to obtain the current CAS.
    BinprotSubdocResponse resp;
    BinprotSubdocCommand request(cb::mcbp::ClientOpcode::SubdocExists);
    request.setPath("int").setKey("dict");
    subdoc_verify_cmd(request, cb::mcbp::Status::Success, "", resp);
    auto cas = resp.getCas();

    // 1. Attempt to mutate with an incorrect CAS - should fail.
    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.cas = cas - 1;
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "float",
                              "2.0"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "string",
                              "\"value\""});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::KeyEexists, {});
    // Document should be unmodified.
    validate_json_document("dict", R"({"int":1})");

    // 2. Attempt to mutate with correct CAS.
    mutation.cas = cas;
    uint64_t new_cas =
            expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    // CAS should have changed.
    EXPECT_NE(cas, new_cas);

    // Document should have been updated.
    validate_json_document("dict", R"({"int":1,"float":2.0,"string":"value"})");

    delete_object("dict");
}

// Test multi-path mutation command - create a bunch of dictionary elements
// then delete them. (Not a very useful operation but should work).
void test_subdoc_multi_mutation_dictadd_delete() {
    store_document("dict", R"({"count":0,"items":{}})");

    // 1. Add a series of paths, then remove two of them.
    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "items.1",
                              "1"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "items.2",
                              "2"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "items.3",
                              "3"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocCounter,
                              SUBDOC_FLAG_NONE,
                              "count",
                              "3"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDelete,
                              SUBDOC_FLAG_NONE,
                              "items.1",
                              {}});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDelete,
                              SUBDOC_FLAG_NONE,
                              "items.3",
                              {}});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocCounter,
                              SUBDOC_FLAG_NONE,
                              "count",
                              "-2"});
    expect_subdoc_cmd(mutation,
                      cb::mcbp::Status::Success,
                      {{3, cb::mcbp::Status::Success, "3"},
                       {6, cb::mcbp::Status::Success, "1"}});

    // Document should have been updated.
    validate_json_document("dict", R"({"count":1,"items":{"2":2}})");

    // 2. Delete the old 'items' dictionary and create a new one.
    mutation.specs.clear();
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDelete,
                              SUBDOC_FLAG_NONE,
                              "items",
                              {}});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictUpsert,
                              SUBDOC_FLAG_NONE,
                              "count",
                              "0"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_MKDIR_P,
                              "items.4",
                              "4"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "items.5",
                              "5"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocCounter,
                              SUBDOC_FLAG_NONE,
                              "count",
                              "2"});
    expect_subdoc_cmd(mutation,
                      cb::mcbp::Status::Success,
                      {{4, cb::mcbp::Status::Success, "2"}});

    validate_json_document("dict", R"({"count":2,"items":{"4":4,"5":5}})");

    delete_object("dict");
}

TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddDelete) {
    test_subdoc_multi_mutation_dictadd_delete();
}

TEST_P(SubdocTestappTest, SubdocMultiMutation_DictAddDelete_MutationSeqno) {
    set_mutation_seqno_feature(true);
    test_subdoc_multi_mutation_dictadd_delete();
    set_mutation_seqno_feature(false);
}

// Test support for expiration on multi-path commands.
TEST_P(SubdocTestappTest, SubdocMultiMutation_Expiry) {
    // Create two documents; one to be used for an exlicit 1s expiry and one
    // for an explicit 0s (i.e. never) expiry.
    uint32_t expiryTime = 5;
    store_document("ephemeral", "[\"a\"]");
    store_document("permanent", "[\"a\"]");

    // Expiry not permitted for MULTI_LOOKUP operations.
    SubdocMultiLookupCmd lookup;
    lookup.key = "ephemeral";
    lookup.expiry = 666;
    lookup.specs.push_back(
            {cb::mcbp::ClientOpcode::SubdocExists, SUBDOC_FLAG_NONE, "[0]"});
    expect_subdoc_cmd(lookup, cb::mcbp::Status::Einval, {});
    reconnect_to_server();

    // Perform a MULTI_REPLACE operation, setting a expiry of 1s.
    SubdocMultiMutationCmd mutation;
    mutation.key = "ephemeral";
    mutation.expiry = expiryTime;
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocReplace,
                              SUBDOC_FLAG_NONE,
                              "[0]",
                              "\"b\""});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    // Try to read the document immediately - should exist.
    auto result = fetch_value("ephemeral");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("[\"b\"]", result.second);

    // Perform a REPLACE on permanent, explicitly encoding an expiry of 0s.
    mutation.key = "permanent";
    mutation.expiry = 0;
    mutation.encode_zero_expiry_on_wire = true;
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    // Try to read the second document immediately - should exist.
    result = fetch_value("permanent");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("[\"b\"]", result.second);

    // Move memcached time forward to expire the ephemeral document
    adjust_memcached_clock(
            (expiryTime * 2),
            cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);

    // Try to read the ephemeral document - shouldn't exist.
    result = fetch_value("ephemeral");
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, result.first);

    // Try to read the permanent document - should still exist.
    result = fetch_value("permanent");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("[\"b\"]", result.second);

    // Cleanup
    delete_object("permanent");
    // Reset memcached clock
    adjust_memcached_clock(
            0, cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);
}

// Test statistics support for multi-lookup commands
TEST_P(SubdocTestappTest, SubdocStatsMultiLookup) {
    // A multi-lookup counts as a single operation, irrespective of how many
    // path specs it contains.

    // Get initial stats
    auto stats = request_stats();
    auto count_before = extract_single_stat(stats, "cmd_subdoc_lookup");
    auto bytes_before_total = extract_single_stat(stats, "bytes_subdoc_lookup_total");
    auto bytes_before_subset = extract_single_stat(stats, "bytes_subdoc_lookup_extracted");

    // Perform a multi-lookup containing >1 path.
    test_subdoc_multi_lookup_getmulti();

    // Get subsequent stats, check stat increased by one.
    stats = request_stats();
    auto count_after = extract_single_stat(stats, "cmd_subdoc_lookup");
    auto bytes_after_total = extract_single_stat(stats, "bytes_subdoc_lookup_total");
    auto bytes_after_subset = extract_single_stat(stats, "bytes_subdoc_lookup_extracted");
    EXPECT_EQ(1, count_after - count_before);
    EXPECT_EQ(321, bytes_after_total - bytes_before_total);
    EXPECT_EQ(246, bytes_after_subset - bytes_before_subset);
}

// Test statistics support for multi-mutation commands
TEST_P(SubdocTestappTest, SubdocStatsMultiMutation) {
    // A multi-mutation counts as a single operation, irrespective of how many
    // path specs it contains.

    // Get initial stats
    auto stats = request_stats();
    auto count_before = extract_single_stat(stats, "cmd_subdoc_mutation");
    auto bytes_before_total = extract_single_stat(stats, "bytes_subdoc_mutation_total");
    auto bytes_before_subset = extract_single_stat(stats, "bytes_subdoc_mutation_inserted");

    // Perform a multi-mutation containing >1 path.
    test_subdoc_multi_mutation_dict_add_max();

    // Get subsequent stats, check stat increased by one.
    stats = request_stats();
    auto count_after = extract_single_stat(stats, "cmd_subdoc_mutation");
    auto bytes_after_total = extract_single_stat(stats, "bytes_subdoc_mutation_total");
    auto bytes_after_subset = extract_single_stat(stats, "bytes_subdoc_mutation_inserted");
    EXPECT_EQ(count_before + 1, count_after);
    EXPECT_EQ(301, bytes_after_total - bytes_before_total);
    EXPECT_EQ(150, bytes_after_subset - bytes_before_subset);
}

// Test support for multi-mutations returning values - maximum spec count
TEST_P(SubdocTestappTest, SubdocMultiMutation_MaxResultSpecValue) {
    // Create an array of PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS counters.
    std::string input("[");
    std::string expected_json("[");
    for (int ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++) {
        input += "0,";
        expected_json += std::to_string(ii + 1) + ",";
    }
    input.pop_back();
    input += ']';
    store_document("array", input);
    expected_json.pop_back();
    expected_json += ']';

    SubdocMultiMutationCmd mutation;
    mutation.key = "array";
    std::vector<SubdocMultiMutationResult> expected_results;
    for (uint8_t ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++) {
        std::string value("[" + std::to_string(ii) + "]");
        mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocCounter,
                                  SUBDOC_FLAG_NONE,
                                  value,
                                  std::to_string(ii + 1)});

        expected_results.emplace_back(ii, cb::mcbp::Status::Success, std::to_string(ii + 1));
    }

    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, expected_results);

    validate_json_document("array", expected_json);

    delete_object("array");

}

// Test that flags are preserved by subdoc multipath mutation operations.
TEST_P(SubdocTestappTest, SubdocMultiMutation_Flags) {
    const uint32_t flags = 0xcafebabe;
    store_object_w_datatype("array", "[]", flags, 0, cb::mcbp::Datatype::Raw);

    SubdocMultiMutationCmd mutation;
    mutation.key = "array";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                              SUBDOC_FLAG_NONE,
                              "",
                              "0"});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    // Check the update actually occurred.
    validate_json_document("array", "[0]");
    validate_flags("array", flags);

    delete_object("array");
}

// Test that you can create a document with the Add doc flag
TEST_P(SubdocTestappTest, SubdocMultiMutation_AddDocFlag) {
    SubdocMultiMutationCmd mutation;
    mutation.addDocFlag(mcbp::subdoc::doc_flag::Add);
    mutation.key = "AddDocTest";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictUpsert,
                              SUBDOC_FLAG_NONE,
                              "test",
                              "56"});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    validate_json_document("AddDocTest", R"({"test":56})");

    delete_object("AddDocTest");
}

// Test that a command with an Add doc flag fails if the key exists
TEST_P(SubdocTestappTest, SubdocMultiMutation_AddDocFlagEEXists) {
    store_document("AddDocExistsTest", "[1,2,3,4]");

    SubdocMultiMutationCmd mutation;
    mutation.addDocFlag(mcbp::subdoc::doc_flag::Add);
    mutation.key = "AddDocExistsTest";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictUpsert,
                              SUBDOC_FLAG_NONE,
                              "test",
                              "56"});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::KeyEexists, {});

    delete_object("AddDocExistsTest");

    // Now the doc is deleted, we should be able to Add successfully
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    validate_json_document("AddDocExistsTest", R"({"test":56})");

    delete_object("AddDocExistsTest");
}

// An Addd doesn't make sense with a cas, check that it's rejected
TEST_P(SubdocTestappTest, SubdocMultiMutation_AddDocFlagInavlidCas) {
    SubdocMultiMutationCmd mutation;
    mutation.addDocFlag(mcbp::subdoc::doc_flag::Add);
    mutation.key = "AddDocCas";
    mutation.cas = 123456;
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictUpsert,
                              SUBDOC_FLAG_NONE,
                              "test",
                              "56"});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Einval, {});
}

// MB-30278: Perform a multi-mutation with two paths with backticks in them.
TEST_P(SubdocTestappTest, MB_30278_SubdocBacktickMultiMutation) {
    store_document("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key1``",
                              "1"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key2``",
                              "2"});
    mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                              SUBDOC_FLAG_NONE,
                              "key3``",
                              "3"});
    expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});

    validate_json_document("dict", R"({"key1`":1,"key2`":2,"key3`":3})");

    delete_object("dict");
}

// Test that when a wholedoc set is performed as part of a multi mutation
// the datatype is set JSON/RAW based on whether the document is actually valid
// json
TEST_P(SubdocTestappTest, WholedocMutationUpdatesDatatype) {
    // store an initial json document
    store_document("item", "{}");

    bool jsonSupported = hasJSONSupport() == ClientJSONSupport::Yes;

    // check datatype is json (if supported)
    validate_datatype_is_json("item", jsonSupported);

    // do a wholedoc mutation setting a non-json value
    {
        SubdocMultiMutationCmd mutation;
        mutation.key = "item";
        mutation.specs.push_back({cb::mcbp::ClientOpcode::Set,
                                  SUBDOC_FLAG_NONE,
                                  "",
                                  "RAW ;: NO JSON HERE"});

        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    }

    // check the datatype has been updated
    validate_datatype_is_json("item", false);

    // wholedoc mutation to set a json doc again
    {
        SubdocMultiMutationCmd mutation;
        mutation.key = "item";
        mutation.specs.push_back({cb::mcbp::ClientOpcode::Set,
                                  SUBDOC_FLAG_NONE,
                                  "",
                                  R"({"json":"goodness"})"});

        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    }

    // check datatype is back to json
    validate_datatype_is_json("item", jsonSupported);
    validate_json_document("item", R"({"json":"goodness"})");

    delete_object("item");
}

TEST_P(SubdocTestappTest, TestSubdocOpAfterWholedocSetNonJson) {
    // store an initial json document
    store_document("item", "{}");

    bool jsonSupported = hasJSONSupport() == ClientJSONSupport::Yes;

    // check datatype is json (if supported)
    validate_datatype_is_json("item", jsonSupported);

    std::vector<cb::mcbp::ClientOpcode> mutationOpcodes = {
            cb::mcbp::ClientOpcode::SubdocDictAdd,
            cb::mcbp::ClientOpcode::SubdocDictUpsert,
            cb::mcbp::ClientOpcode::SubdocReplace,
            cb::mcbp::ClientOpcode::SubdocArrayPushLast,
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
            cb::mcbp::ClientOpcode::SubdocArrayAddUnique};

    // expect that any Subdoc mutation following a non-json wholedoc will fail
    for (size_t i = 0; i < mutationOpcodes.size(); i++) {
        SubdocMultiMutationCmd mutation;
        mutation.key = "item";

        std::vector<SubdocMultiMutationResult> expected;

        // whole doc non-json
        mutation.specs.push_back({cb::mcbp::ClientOpcode::Set,
                                  SUBDOC_FLAG_NONE,
                                  "",
                                  "RAW ;: NO JSON HERE"});

        // subdoc op on any path should fail - doc is not json
        mutation.specs.push_back({mutationOpcodes.at(i),
                                  SUBDOC_FLAG_NONE,
                                  "path-" + std::to_string(i),
                                  "anyvalue"});

        expect_subdoc_cmd(
                mutation,
                cb::mcbp::Status::SubdocMultiPathFailure,
                {{1, cb::mcbp::Status::SubdocDocNotJson, ""}});
    }

    // doc should be unchanged
    validate_json_document("item", "{}");

    delete_object("item");
}

TEST_P(SubdocTestappTest,
       TestSubdocOpInSeperateMultiMutationAfterWholedocSetNonJson) {
    // store an initial json document
    store_document("item", "{}");

    bool jsonSupported = hasJSONSupport() == ClientJSONSupport::Yes;

    // check datatype is json (if supported)
    validate_datatype_is_json("item", jsonSupported);

    // do a wholedoc mutation setting a non-json value
    {
        SubdocMultiMutationCmd mutation;
        mutation.key = "item";
        mutation.specs.push_back({cb::mcbp::ClientOpcode::Set,
                                  SUBDOC_FLAG_NONE,
                                  "",
                                  "RAW ;: NO JSON HERE"});

        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    }

    // check the datatype has been updated
    validate_datatype_is_json("item", false);

    // expect that any Subdoc lookup op will fail
    {
        SubdocMultiLookupCmd lookup;
        lookup.key = "item";
        for (auto opcode : {cb::mcbp::ClientOpcode::SubdocGet,
                            cb::mcbp::ClientOpcode::SubdocExists}) {
            lookup.specs.push_back({opcode, SUBDOC_FLAG_NONE, "anypath"});
        }

        expect_subdoc_cmd(lookup,
                          cb::mcbp::Status::SubdocMultiPathFailure,
                          {{cb::mcbp::Status::SubdocDocNotJson, ""},
                           {cb::mcbp::Status::SubdocDocNotJson, ""}});
    }

    std::vector<cb::mcbp::ClientOpcode> opcodes = {
            cb::mcbp::ClientOpcode::SubdocDictAdd,
            cb::mcbp::ClientOpcode::SubdocDictUpsert,
            cb::mcbp::ClientOpcode::SubdocReplace,
            cb::mcbp::ClientOpcode::SubdocArrayPushLast,
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
            cb::mcbp::ClientOpcode::SubdocArrayAddUnique};

    // expect that any Subdoc mutation will fail
    for (size_t i = 0; i < opcodes.size(); i++) {
        SubdocMultiMutationCmd mutation;
        mutation.key = "item";

        std::vector<SubdocMultiMutationResult> expected;

        mutation.specs.push_back({opcodes.at(i),
                                  SUBDOC_FLAG_NONE,
                                  "path-" + std::to_string(i),
                                  "anyvalue"});

        expect_subdoc_cmd(
                mutation,
                cb::mcbp::Status::SubdocMultiPathFailure,
                {{0, cb::mcbp::Status::SubdocDocNotJson, ""}});
    }

    auto pair = fetch_value("item");
    EXPECT_EQ(cb::mcbp::Status::Success, pair.first);
    EXPECT_EQ("RAW ;: NO JSON HERE", pair.second);

    delete_object("item");
}
