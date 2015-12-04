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

#include "testapp_subdoc.h"

#include "utilities/subdoc_encoder.h"

// Test multi-path lookup command - simple single SUBDOC_GET
TEST_P(McdTestappTest, SubdocMultiLookup_GetSingle)
{
    store_object("dict", "{\"key1\":1,\"key2\":\"two\", \"key3\":3.0}");

    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_GET, SUBDOC_FLAG_NONE,
                            "key1"});
    std::vector<SubdocMultiLookupResult> expected{{PROTOCOL_BINARY_RESPONSE_SUCCESS, "1"}};
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_SUCCESS, expected);

    // Attempt to access non-existent key.
    lookup.key = "dictXXX";
    expected.clear();
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, expected);

    // Attempt to access non-existent path.
    lookup.key = "dict";
    lookup.specs.at(0) = {PROTOCOL_BINARY_CMD_SUBDOC_GET, SUBDOC_FLAG_NONE,
                          "keyXX"};
    expected.push_back({PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, ""});
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE,
                      expected);

    delete_object("dict");
}

// Test multi-path lookup command - simple single SUBDOC_EXISTS
TEST_P(McdTestappTest, SubdocMultiLookup_ExistsSingle)
{
    store_object("dict", "{\"key1\":1,\"key2\":\"two\", \"key3\":3.0}");

    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                            SUBDOC_FLAG_NONE, "key1"});
    std::vector<SubdocMultiLookupResult> expected{{PROTOCOL_BINARY_RESPONSE_SUCCESS, ""}};
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_SUCCESS, expected);

    delete_object("dict");
}

/* Creates a flat dictionary with the specified number of key/value pairs
 *   Keys are named "key_0", "key_1"...
 *   Values are strings of the form "value_0", value_1"...
 */
unique_cJSON_ptr make_flat_dict(int nelements) {
    cJSON* dict = cJSON_CreateObject();
    for (int i = 0; i < nelements; i++) {
        std::string key("key_" + std::to_string(i));
        std::string value("value_" + std::to_string(i));
        cJSON_AddStringToObject(dict, key.c_str(), value.c_str());
    }
    return unique_cJSON_ptr(dict);
}

static void test_subdoc_multi_lookup_getmulti() {
    auto dict = make_flat_dict(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS + 1);
    auto* dict_str = cJSON_Print(dict.get());
    store_object("dict", dict_str);
    cJSON_Free(dict_str);

    // Lookup the maximum number of allowed paths - should succeed.
    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    std::vector<SubdocMultiLookupResult> expected;
    for (int ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++)
    {
        std::string key("key_" + std::to_string(ii));
        std::string value("\"value_" + std::to_string(ii) + '"');
        lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_GET,
                                SUBDOC_FLAG_NONE, key});

        expected.push_back({PROTOCOL_BINARY_RESPONSE_SUCCESS,
                            value});
    }
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_SUCCESS, expected);

    // Add one more - should fail.
    lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_GET, SUBDOC_FLAG_NONE,
                            "key_" + std::to_string(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS)});
    expected.clear();
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
                      expected);

    delete_object("dict");
}
// Test multi-path lookup - multiple GET lookups
TEST_P(McdTestappTest, SubdocMultiLookup_GetMulti) {
    test_subdoc_multi_lookup_getmulti();
}

// Test multi-path lookup - multiple GET lookups with various invalid paths.
TEST_P(McdTestappTest, SubdocMultiLookup_GetMultiInvalid)
{
    store_object("dict", "{\"key1\":1,\"key2\":\"two\",\"key3\":[0,1,2]}");

    // Build a multi-path LOOKUP with a variety of invalid paths
    std::vector<std::pair<std::string, protocol_binary_response_status > > bad_paths({
            {"[0]", PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH},
            {"key3[3]", PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT},
    });

    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    std::vector<SubdocMultiLookupResult> expected;
    for (const auto& path : bad_paths) {
        lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_GET,
                                SUBDOC_FLAG_NONE, path.first});
        expected.push_back({path.second, ""});
    }
    expect_subdoc_cmd(lookup,
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE,
                      expected);

    delete_object("dict");
}

// Test multi-path lookup - multiple EXISTS lookups
TEST_P(McdTestappTest, SubdocMultiLookup_ExistsMulti)
{
    auto dict = make_flat_dict(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS + 1);
    auto* dict_str = cJSON_Print(dict.get());
    store_object("dict", dict_str);
    cJSON_Free(dict_str);

    // Lookup the maximum number of allowed paths - should succeed.
    SubdocMultiLookupCmd lookup;
    lookup.key = "dict";
    std::vector<SubdocMultiLookupResult> expected;
    for (int ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++) {
        std::string key("key_" + std::to_string(ii));

        lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                SUBDOC_FLAG_NONE, key });
        expected.push_back({PROTOCOL_BINARY_RESPONSE_SUCCESS, ""});
    }
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_SUCCESS, expected);

    // Add one more - should fail.
    lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                            SUBDOC_FLAG_NONE,
                            "key_" + std::to_string(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS)});
    expected.clear();
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
                      expected);

    delete_object("dict");
}

/******************* Multi-path mutation tests *******************************/

// Test multi-path mutation command - simple single SUBDOC_DICT_ADD
TEST_P(McdTestappTest, SubdocMultiMutation_DictAddSingle)
{
    store_object("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "key", "\"value\""});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Check the update actually occurred.
    validate_object("dict", "{\"key\":\"value\"}");

    delete_object("dict");
}

// Test multi-path mutation command - simple multiple SUBDOC_DICT_ADD
TEST_P(McdTestappTest, SubdocMultiMutation_DictAddMulti)
{
    store_object("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "key1", "1"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "key2", "2"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "key3", "3"});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Check the update actually occurred.
    validate_object("dict", "{\"key1\":1,\"key2\":2,\"key3\":3}");

    delete_object("dict");
}

// Test multi-path mutation command - test maximum supported SUBDOC_DICT_ADD
// paths.
static void test_subdoc_multi_mutation_dict_add_max() {
    store_object("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    for (int ii = 0; ii < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS; ii++) {
        std::string path("key_" + std::to_string(ii));
        std::string value("\"value_" + std::to_string(ii) + '"');

        mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                                  SUBDOC_FLAG_NONE, path, value});
    }
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Check the update actually occurred.
    auto dict = make_flat_dict(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS);
    auto* expected_str = cJSON_PrintUnformatted(dict.get());
    validate_object("dict", expected_str);
    cJSON_Free(expected_str);

    delete_object("dict");

    // Try with one more mutation spec - should fail and document should be
    // unmodified.
    store_object("dict", "{}");
    auto max_id = std::to_string(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS);
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "key_" + max_id,
                              "\"value_" + max_id + '"'});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Document should be unmodified.
    validate_object("dict", "{}");

    delete_object("dict");
}
TEST_P(McdTestappTest, SubdocMultiMutation_DictAddMax) {
    test_subdoc_multi_mutation_dict_add_max();
}

// Test attempting to add the same key twice in a multi-path command.
TEST_P(McdTestappTest, SubdocMultiMutation_DictAddInvalidDuplicate) {
    store_object("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "key", "\"value\""});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "key", "\"value2\""});
    // Should return failure, with the index of the failing op (1).
    expect_subdoc_cmd(mutation,
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS, 1));

    // Document should be unmodified.
    validate_object("dict", "{}");

    delete_object("dict");
}

// Test multi-path mutation command - 2x DictAdd with a Counter update
TEST_P(McdTestappTest, SubdocMultiMutation_DictAddCounter) {
    store_object("dict", "{\"count\":0,\"items\":{}}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "items.foo", "1"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "items.bar", "2"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              SUBDOC_FLAG_NONE, "count", "2"});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Check the update actually occurred.
    validate_object("dict",
                    "{\"count\":2,\"items\":{\"foo\":1,\"bar\":2}}");

    delete_object("dict");
}

// Test multi-path mutation command - 2x DictAdd with specific CAS.
TEST_P(McdTestappTest, SubdocMultiMutation_DictAddCAS) {
    store_object("dict", "{\"int\":1}");

    // Use SUBDOC_EXISTS to obtain the current CAS.
    uint64_t cas = expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                               "dict", "int"),
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS, "");

    // 1. Attempt to mutate with an incorrect CAS - should fail.
    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.cas = cas - 1;
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "float", "2.0"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "string", "\"value\""});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));
    // Document should be unmodified.
    validate_object("dict", "{\"int\":1}");

    // 2. Attempt to mutate with correct CAS.
    mutation.cas = cas;
    uint64_t new_cas = expect_subdoc_cmd(mutation,
                                         PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                         std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // CAS should have changed.
    EXPECT_NE(cas, new_cas);

    // Document should have been updated.
    validate_object("dict", "{\"int\":1,\"float\":2.0,\"string\":\"value\"}");

    delete_object("dict");
}

// Test multi-path mutation command - create a bunch of dictionary elements
// then delete them. (Not a very useful operation but should work).
void test_subdoc_multi_mutation_dictadd_delete() {
    store_object("dict", "{\"count\":0,\"items\":{}}");

    // 1. Add a series of paths, then remove two of them.
    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "items.1", "1"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "items.2", "2"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "items.3", "3"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              SUBDOC_FLAG_NONE, "count", "3"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                              SUBDOC_FLAG_NONE, "items.1"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                              SUBDOC_FLAG_NONE, "items.3"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              SUBDOC_FLAG_NONE, "count", "-2"});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Document should have been updated.
    validate_object("dict", "{\"count\":1,\"items\":{\"2\":2}}");

    // 2. Delete the old 'items' dictionary and create a new one.
    mutation.specs.clear();
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                              SUBDOC_FLAG_NONE, "items"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                              SUBDOC_FLAG_NONE, "count", "0"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_MKDIR_P, "items.4", "4"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              SUBDOC_FLAG_NONE, "items.5", "5"});
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              SUBDOC_FLAG_NONE, "count", "2"});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    validate_object("dict", "{\"count\":2,\"items\":{\"4\":4,\"5\":5}}");

    delete_object("dict");
}

TEST_P(McdTestappTest, SubdocMultiMutation_DictAddDelete) {
    test_subdoc_multi_mutation_dictadd_delete();
}

TEST_P(McdTestappTest, SubdocMultiMutation_DictAddDelete_MutationSeqno) {
    set_mutation_seqno_feature(true);
    test_subdoc_multi_mutation_dictadd_delete();
    set_mutation_seqno_feature(false);
}

// Test support for expiration on multi-path commands.
TEST_P(McdTestappTest, SubdocMultiMutation_Expiry) {
    // Create two documents; one to be used for an exlicit 1s expiry and one
    // for an explicit 0s (i.e. never) expiry.
    store_object("ephemeral", "[\"a\"]");
    store_object("permanent", "[\"a\"]");

    // Expiry not permitted for MULTI_LOOKUP operations.
    SubdocMultiLookupCmd lookup;
    lookup.key = "ephemeral";
    lookup.expiry = 666;
    lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                            SUBDOC_FLAG_NONE, "[0]" });
    expect_subdoc_cmd(lookup, PROTOCOL_BINARY_RESPONSE_EINVAL, {});

    // Perform a MULTI_REPLACE operation, setting a expiry of 1s.
    SubdocMultiMutationCmd mutation;
    mutation.key = "ephemeral";
    mutation.expiry = 1;
    mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                              SUBDOC_FLAG_NONE, "[0]", "\"b\""});
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Try to read the document immediately - should exist.
    auto result = fetch_value("ephemeral");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("[\"b\"]", result.second);

    // Perform a REPLACE on permanent, explicitly encoding an expiry of 0s.
    mutation.key = "permanent";
    mutation.expiry = 0;
    mutation.encode_zero_expiry_on_wire = true;
    expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS, 0));

    // Try to read the second document immediately - should exist.
    result = fetch_value("permanent");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("[\"b\"]", result.second);

    // Sleep for 2s seconds.
    // TODO: it would be great if we could somehow accelerate time from the
    // harness, and not add 2s to the runtime of the test...
    usleep(2 * 1000 * 1000);

    // Try to read the ephemeral document - shouldn't exist.
    result = fetch_value("ephemeral");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, result.first);

    // Try to read the permanent document - should still exist.
    result = fetch_value("permanent");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("[\"b\"]", result.second);
}

// Test statistics support for multi-lookup commands
TEST_P(McdTestappTest, SubdocStatsMultiLookup) {
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
    EXPECT_EQ(373, bytes_after_total - bytes_before_total);
    EXPECT_EQ(246, bytes_after_subset - bytes_before_subset);
}

// Test statistics support for multi-mutation commands
TEST_P(McdTestappTest, SubdocStatsMultiMutation) {
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
