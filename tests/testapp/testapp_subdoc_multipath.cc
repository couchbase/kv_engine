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

// Test multi-path lookup - multiple GET lookups
TEST_P(McdTestappTest, SubdocMultiLookup_GetMulti)
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
        lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_GET,
                                SUBDOC_FLAG_NONE,
                                "key_" + std::to_string(ii)});
        expected.push_back({PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           "\"value_" + std::to_string(ii) + '"'});
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
        lookup.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                SUBDOC_FLAG_NONE,
                                "key_" + std::to_string(ii)});
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
