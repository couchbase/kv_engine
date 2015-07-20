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

#include <unordered_map>

class SubdocPerfTest : public TestappTest {
public:
    void SetUp() {
        TestappTest::SetUp();
        // Performance test - disable ewouldblock_engine.
        ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode_NEXT_N,
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

static void subdoc_create_array(const std::string& name, size_t elements) {
    std::string list("[");
    for (size_t i = 0; i < elements; i++) {
        std::string key(std::to_string(i));
        list.append(key + ',');
    }
    // Replace the last comma with the closing bracket.
    list.pop_back();
    list.push_back(']');

    store_object(name, list, /*JSON*/true, /*compress*/false);
}

// Baseline test case for Array5k_Remove tests; this 'test' just creates the
// document with 5k elements to operate on. Can then subtract the runtime of
// this from Array5k_Remove tests to see actual performance.
TEST_F(SubdocPerfTest, Array5k_RemoveBaseline) {
    subdoc_create_array("list", iterations);
    delete_object("list");
}


// Create an N-element array, then benchmark removing N elements individually
// by removing the first element each time.
TEST_F(SubdocPerfTest, Array5k_RemoveFirst) {
    subdoc_create_array("list", iterations);

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
    subdoc_create_array("list", iterations);

    for (size_t i = 0; i < iterations; i++) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                                    "list", "[-1]"),
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
