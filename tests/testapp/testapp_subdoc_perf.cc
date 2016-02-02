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
 * Performance tests for the Sub-Document API.
 *
 * Test groups:
 * - Array5k: Operate on an array of 5,000 integers. For the PUSH/ADD tests,
 *            array is initially empty and we add 5,000 elements one by one.
 *            For the REMOVE tests an initial array of 5,000 elements is
 *            stored and the elements are removed one by one.
 *
 * - Dict5k: As per Array5k, except start with an empty dictionary and add
 *           K/V pairs of the form <num>: value_<num>.
 */

#include "testapp_subdoc.h"

#include "../utilities/subdoc_encoder.h"

#include <unordered_map>

class SubdocPerfTest : public TestappTest {
public:
    void SetUp() {
        TestappTest::SetUp();
        // Performance test - disable ewouldblock_engine.
        ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode::Next_N,
                                     0);
        iterations = 5000;
    }

protected:
    size_t iterations;
};


/* Create a JSON document consisting of a flat array of N elements, using
 * the specified opcode.
 */
static void subdoc_perf_test_array(protocol_binary_command cmd,
                                   size_t iterations) {
    store_object("list", "[]");

    for (size_t i = 0; i < iterations; i++) {
        expect_subdoc_cmd(SubdocCmd(cmd, "list", "", std::to_string(i)),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }

    delete_object("list");
}

/*****************************************************************************
 * Sub-document API Performance Tests - Single path.
 *
 * These test various access patterns implemented using using single-path
 * sub-document API commands.
 ****************************************************************************/

TEST_F(SubdocPerfTest, Array5k_PushFirst) {
    subdoc_perf_test_array(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                           iterations);
}

TEST_F(SubdocPerfTest, Array5k_PushLast) {
    subdoc_perf_test_array(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                           iterations);
}

TEST_F(SubdocPerfTest, Array5k_AddUnique) {
    subdoc_perf_test_array(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                           iterations);
}

// Build an array of the given number of elements.
static std::string subdoc_create_array(size_t elements) {
    std::string list("[");
    for (size_t i = 0; i < elements; i++) {
        std::string key(std::to_string(i));
        list.append(key + ',');
    }
    // Replace the last comma with the closing bracket.
    list.pop_back();
    list.push_back(']');

    return list;
}

// Baseline test case for Array5k_Remove tests; this 'test' just creates the
// document with 5k elements to operate on. Can then subtract the runtime of
// this from Array5k_Remove tests to see actual performance.
TEST_F(SubdocPerfTest, Array5k_RemoveBaseline) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());
    delete_object("list");
}


// Create an N-element array, then benchmark removing N elements individually
// by removing the first element each time.
TEST_F(SubdocPerfTest, Array5k_RemoveFirst) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    for (size_t i = 0; i < iterations; i++) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                                    "list", "[0]"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }
    delete_object("list");
}

// Create an N-element array, then benchmark removing N elements individually
// by removing the last element each time.
TEST_F(SubdocPerfTest, Array5k_RemoveLast) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    for (size_t i = 0; i < iterations; i++) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                                    "list", "[-1]"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }
    delete_object("list");
}


// Create an N-element array, then benchmark replacing the first element.
TEST_F(SubdocPerfTest, Array5k_ReplaceFirst) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    for (size_t i = 0; i < iterations; i++) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                                    "list", "[0]", "1"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }
    delete_object("list");
}

// Create an N-element array, then benchmark replacing the middle element.
TEST_F(SubdocPerfTest, Array5k_ReplaceMiddle) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    std::string path(std::string("[") + std::to_string(iterations / 2) + "]");
    for (size_t i = 0; i < iterations; i++) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                                    "list", path, "1"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }
    delete_object("list");
}

// Create an N-element array, then benchmark replacing the first element.
TEST_F(SubdocPerfTest, Array5k_ReplaceLast) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list, /*JSON*/true, /*compress*/false);

    for (size_t i = 0; i < iterations; i++) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                                    "list", "[-1]", "1"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }
    delete_object("list");
}


TEST_F(SubdocPerfTest, Dict5k_Add) {
    store_object("dict", "{}");

    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                                    "dict", key, value),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }

    delete_object("dict");
}

static void subdoc_create_dict(const std::string& name, size_t elements) {
    std::string dict("{");
    for (size_t i = 0; i < elements; i++) {
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');
        dict.append('"' + key + "\":" + value + ',');
    }
    // Replace the last comma with the closing brace.
    dict.pop_back();
    dict.push_back('}');

    store_object(name, dict, /*JSON*/true, /*compress*/false);
}

// Baseline test case for Dict5k test; this 'test' just creates the document to
// operate on. Can then subtract the runtime of this from Dict5k tests to
// see actual performance.
TEST_F(SubdocPerfTest, Dict5k_RemoveBaseline) {
    subdoc_create_dict("dict", iterations);
    delete_object("dict");
}

TEST_F(SubdocPerfTest, Dict5k_Remove) {
    subdoc_create_dict("dict", iterations);

    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                                    "dict", key),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }
    delete_object("dict");
}

static void subdoc_perf_test_dict(protocol_binary_command cmd,
                                  size_t iterations) {
    subdoc_create_dict("dict", iterations);

    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "dict",
                                    key),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, value);
    }
    delete_object("dict");
}

// Measure GETing all keys in a dictionary.
TEST_F(SubdocPerfTest, Dict5k_Get) {
    subdoc_perf_test_dict(PROTOCOL_BINARY_CMD_SUBDOC_GET, iterations);
}

// Measure checking for EXISTence of all keys in a dictionary.
TEST_F(SubdocPerfTest, Dict5k_Exists) {
    subdoc_perf_test_dict(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, iterations);
}


/*****************************************************************************
 * Sub-document API Performance Tests - Multi path.
 *
 * These test equivilent functionality to the single-path variants above,
 * but packing in multiple paths into a single command where possible.
 ****************************************************************************/

TEST_F(SubdocPerfTest, Array5k_PushFirst_Multipath) {
    store_object("list", "[]");

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                  SUBDOC_FLAG_NONE, "", std::to_string(i)});

        // Once we have accumulated the maximum number of mutation specs
        // (paths) permitted, send the request.
        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
    }

    delete_object("list");
}

// Create an N-element array, then benchmark removing N elements using
// multi-path commands by removing the first element each time.
TEST_F(SubdocPerfTest, Array5k_RemoveFirst_Multipath) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                                  SUBDOC_FLAG_NONE, "[0]"});

        // Once we have accumulated the maximum number of mutation specs
        // (paths) permitted, send the request.
        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
    }

    delete_object("list");
}

// Create an N-element array, then benchmark replacing the first element
// using multi-path operations.
TEST_F(SubdocPerfTest, Array5k_ReplaceFirst_Multipath) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                                  SUBDOC_FLAG_NONE, "[0]", "1"});

        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
    }

    delete_object("list");
}

// Create an N-element array, then benchmark replacing the middle element
// using multi-path operations.
TEST_F(SubdocPerfTest, Array5k_ReplaceMiddle_Multipath) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    std::string path(std::string("[") + std::to_string(iterations / 2) + "]");
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                                  SUBDOC_FLAG_NONE, path, "1"});

        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
    }

    delete_object("list");
}

TEST_F(SubdocPerfTest, Dict5k_Add_Multipath) {
    store_object("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');

        mutation.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                                  SUBDOC_FLAG_NONE, key, value});

        // Once we have accumulated the maximum number of mutation specs
        // (paths) permitted, send the request.
        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, PROTOCOL_BINARY_RESPONSE_SUCCESS, {});
    }

    delete_object("dict");
}


/*****************************************************************************
 * 'Fulldoc' Performance Tests
 *
 * These test equivalent functionality to their subdoc siblings above, but
 * implemented using the traditional operations which operate on the entire
 * document - aka "Fulldoc" operations.
 ****************************************************************************/

TEST_F(SubdocPerfTest, Array5k_PushFirst_Fulldoc) {
    store_object("list", "[]");

    // At each iteration create a new document with one more element in it,
    // and store to the server.
    std::string list("]");
    for (unsigned int i = 0; i < iterations; i++) {
        if (i != 0) {
            // After the first element, replace the opening bracket with a
            // comma.
            list.front() = ',';
        }
        // Prepend the number and close the list.
        list.insert(0, std::to_string(i));
        list.insert(0, 1, '[');

        store_object("list", list.c_str(), /*validate*/false);
    }

    delete_object("list");
}

TEST_F(SubdocPerfTest, Array5k_PushLast_Fulldoc) {
    store_object("list", "[]");

    // At each iteration create a new document with one more element in it,
    // and store to the server.
    std::string list("[");
    for (unsigned int i = 0; i < iterations; i++) {
        if (i != 0) {
            // After the first element, replace the closing bracket with a
            // comma.
            list.back() = ',';
        }
        // Append the number and close the list.
        list.append(std::to_string(i));
        list.push_back(']');

        store_object("list", list.c_str(), /*validate*/false);
    }

    delete_object("list");
}

TEST_F(SubdocPerfTest, Dict5k_Add_Fulldoc) {
    store_object("dict", "{}");

    // At each iteration add another key to the dictionary, and SET the whole
    // thing.
    std::string dict("{");
    for (size_t i = 0; i < iterations; i++) {
        if (i != 0) {
            // After the first element, replace the closing bracket with a
            // comma.
            dict.back() = ',';
        }
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');
        dict.append('"' + key + "\":" + value);

        // Add the closing brace.
        dict.push_back('}');

        store_object("dict", dict.c_str(), /*validate*/false);
    }

    delete_object("dict");
}

// Create an N-element array, then benchmark replacing (any) element. (For
// fulldoc commands we are sending the whole document, so doesn't matter what
// we replace).
TEST_F(SubdocPerfTest, Array5k_Replace_Fulldoc) {
    std::string list(subdoc_create_array(iterations));
    store_object("list", list.c_str());

    // No point in actually 'replacing' anything, just send the same original
    // thing.
    for (size_t i = 0; i < iterations; i++) {
        store_object("list", list.c_str(), /*validate*/false);
    }

    delete_object("list");
}
