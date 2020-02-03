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
 * - Array: Operate on an array of 5,000 integers. For the PUSH/ADD tests,
 *          array is initially empty and we add 5,000 elements one by one.
 *          For the REMOVE tests an initial array of 5,000 elements is
 *          stored and the elements are removed one by one.
 *
 * - Dict: As per Array, except start with an empty dictionary and add
 *         K/V pairs of the form <num>: value_<num>.
 */

#include "testapp_subdoc_common.h"

#include <folly/Portability.h>
#include <valgrind/valgrind.h>
#include <unordered_map>

class SubdocPerfTest : public SubdocTestappTest {
protected:
    void SetUp() {
        McdTestappTest::SetUp();
        // Performance test - disable ewouldblock_engine.
        ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode::Next_N,
                                     0);

        if (folly::kIsSanitize || RUNNING_ON_VALGRIND ||
            (folly::kIsWindows && folly::kIsDebug)) {
            // Reduce the iterations to a minimal value if we're running under
            // a configuration which is not running at release speed - we aren't
            // benchmarking under these modes, just checking for bugs.
            iterations = 10;
        } else {
            iterations = 5000;
        }
    }

    void subdoc_perf_test_array(cb::mcbp::ClientOpcode cmd, size_t iterations);

    void subdoc_perf_test_dict(cb::mcbp::ClientOpcode cmd, size_t iterations);

    size_t iterations;
};


/* Create a JSON document consisting of a flat array of N elements, using
 * the specified opcode.
 */
void SubdocPerfTest::subdoc_perf_test_array(cb::mcbp::ClientOpcode cmd,
                                            size_t iterations) {
    store_document("list", "[]");

    for (size_t i = 0; i < iterations; i++) {
        subdoc_verify_cmd(BinprotSubdocCommand(cmd, "list", "", std::to_string(i)));
    }

    delete_object("list");
}

/*****************************************************************************
 * Sub-document API Performance Tests - Single path.
 *
 * These test various access patterns implemented using using single-path
 * sub-document API commands.
 ****************************************************************************/

TEST_P(SubdocPerfTest, Array_PushFirst) {
    subdoc_perf_test_array(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                           iterations);
}

TEST_P(SubdocPerfTest, Array_PushLast) {
    subdoc_perf_test_array(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                           iterations);
}

TEST_P(SubdocPerfTest, Array_AddUnique) {
    subdoc_perf_test_array(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
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

// Baseline test case for Array_Remove tests; this 'test' just creates the
// document with  elements to operate on. Can then subtract the runtime of
// this from Array_Remove tests to see actual performance.
TEST_P(SubdocPerfTest, Array_RemoveBaseline) {
    store_document("list", subdoc_create_array(iterations));
    delete_object("list");
}


// Create an N-element array, then benchmark removing N elements individually
// by removing the first element each time.
TEST_P(SubdocPerfTest, Array_RemoveFirst) {
    store_document("list", subdoc_create_array(iterations));

    for (size_t i = 0; i < iterations; i++) {
        subdoc_verify_cmd(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocDelete, "list", "[0]"));
    }
    delete_object("list");
}

// Create an N-element array, then benchmark removing N elements individually
// by removing the last element each time.
TEST_P(SubdocPerfTest, Array_RemoveLast) {
    store_document("list", subdoc_create_array(iterations));

    for (size_t i = 0; i < iterations; i++) {
        subdoc_verify_cmd(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocDelete, "list", "[-1]"));
    }
    delete_object("list");
}


// Create an N-element array, then benchmark replacing the first element.
TEST_P(SubdocPerfTest, Array_ReplaceFirst) {
    store_document("list", subdoc_create_array(iterations));

    for (size_t i = 0; i < iterations; i++) {
        subdoc_verify_cmd(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocReplace, "list", "[0]", "1"));
    }
    delete_object("list");
}

// Create an N-element array, then benchmark replacing the middle element.
TEST_P(SubdocPerfTest, Array_ReplaceMiddle) {
    store_document("list", subdoc_create_array(iterations));

    std::string path(std::string("[") + std::to_string(iterations / 2) + "]");
    for (size_t i = 0; i < iterations; i++) {
        subdoc_verify_cmd(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocReplace, "list", path, "1"));
    }
    delete_object("list");
}

// Create an N-element array, then benchmark replacing the first element.
TEST_P(SubdocPerfTest, Array_ReplaceLast) {
    store_document("list", subdoc_create_array(iterations));

    for (size_t i = 0; i < iterations; i++) {
        subdoc_verify_cmd(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocReplace, "list", "[-1]", "1"));
    }
    delete_object("list");
}

TEST_P(SubdocPerfTest, Dict_Add) {
    store_document("dict", "{}");

    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');
        subdoc_verify_cmd(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocDictAdd, "dict", key, value));
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

    store_document(name, dict);
}

// Baseline test case for Dict test; this 'test' just creates the document to
// operate on. Can then subtract the runtime of this from Dict tests to
// see actual performance.
TEST_P(SubdocPerfTest, Dict_RemoveBaseline) {
    subdoc_create_dict("dict", iterations);
    delete_object("dict");
}

TEST_P(SubdocPerfTest, Dict_Remove) {
    subdoc_create_dict("dict", iterations);

    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        subdoc_verify_cmd(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocDelete, "dict", key));
    }
    delete_object("dict");
}

void SubdocPerfTest::subdoc_perf_test_dict(cb::mcbp::ClientOpcode cmd,
                                           size_t iterations) {
    subdoc_create_dict("dict", iterations);

    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');
        subdoc_verify_cmd(
                BinprotSubdocCommand(
                        cb::mcbp::ClientOpcode::SubdocGet, "dict", key),
                cb::mcbp::Status::Success,
                value);
    }
    delete_object("dict");
}

// Measure GETing all keys in a dictionary.
TEST_P(SubdocPerfTest, Dict_Get) {
    subdoc_perf_test_dict(cb::mcbp::ClientOpcode::SubdocGet, iterations);
}

// Measure checking for EXISTence of all keys in a dictionary.
TEST_P(SubdocPerfTest, Dict_Exists) {
    subdoc_perf_test_dict(cb::mcbp::ClientOpcode::SubdocExists, iterations);
}


/*****************************************************************************
 * Sub-document API Performance Tests - Multi path.
 *
 * These test equivilent functionality to the single-path variants above,
 * but packing in multiple paths into a single command where possible.
 ****************************************************************************/

TEST_P(SubdocPerfTest, Array_PushFirst_Multipath) {
    store_document("list", "[]");

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                  SUBDOC_FLAG_NONE,
                                  "",
                                  std::to_string(i)});

        // Once we have accumulated the maximum number of mutation specs
        // (paths) permitted, send the request.
        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    }

    delete_object("list");
}

// Create an N-element array, then benchmark removing N elements using
// multi-path commands by removing the first element each time.
TEST_P(SubdocPerfTest, Array_RemoveFirst_Multipath) {
    store_document("list", subdoc_create_array(iterations));

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDelete,
                                  SUBDOC_FLAG_NONE,
                                  "[0]"});

        // Once we have accumulated the maximum number of mutation specs
        // (paths) permitted, send the request.
        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    }

    delete_object("list");
}

// Create an N-element array, then benchmark replacing the first element
// using multi-path operations.
TEST_P(SubdocPerfTest, Array_ReplaceFirst_Multipath) {
    store_document("list", subdoc_create_array(iterations));

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocReplace,
                                  SUBDOC_FLAG_NONE,
                                  "[0]",
                                  "1"});

        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    }

    delete_object("list");
}

// Create an N-element array, then benchmark replacing the middle element
// using multi-path operations.
TEST_P(SubdocPerfTest, Array_ReplaceMiddle_Multipath) {
    store_document("list", subdoc_create_array(iterations));

    SubdocMultiMutationCmd mutation;
    mutation.key = "list";
    std::string path(std::string("[") + std::to_string(iterations / 2) + "]");
    for (size_t i = 0; i < iterations; i++) {
        mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocReplace,
                                  SUBDOC_FLAG_NONE,
                                  path,
                                  "1"});

        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
    }

    delete_object("list");
}

TEST_P(SubdocPerfTest, Dict_Add_Multipath) {
    store_document("dict", "{}");

    SubdocMultiMutationCmd mutation;
    mutation.key = "dict";
    for (size_t i = 0; i < iterations; i++) {
        std::string key(std::to_string(i));
        std::string value("\"value_" + std::to_string(i) + '"');

        mutation.specs.push_back({cb::mcbp::ClientOpcode::SubdocDictAdd,
                                  SUBDOC_FLAG_NONE,
                                  key,
                                  value});

        // Once we have accumulated the maximum number of mutation specs
        // (paths) permitted, send the request.
        if (mutation.specs.size() == PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) {
            expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
            mutation.specs.clear();
        }
    }

    // If there are any remaining specs, send them.
    if (!mutation.specs.empty()) {
        expect_subdoc_cmd(mutation, cb::mcbp::Status::Success, {});
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

TEST_P(SubdocPerfTest, Array_PushFirst_Fulldoc) {
    store_document("list", "[]");

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

        store_document("list", list);
    }

    delete_object("list");
}

TEST_P(SubdocPerfTest, Array_PushLast_Fulldoc) {
    store_document("list", "[]");

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

        store_document("list", list);
    }

    delete_object("list");
}

TEST_P(SubdocPerfTest, Dict_Add_Fulldoc) {
    store_document("dict", "{}");

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

        store_document("dict", dict);
    }

    delete_object("dict");
}

// Create an N-element array, then benchmark replacing (any) element. (For
// fulldoc commands we are sending the whole document, so doesn't matter what
// we replace).
TEST_P(SubdocPerfTest, Array_Replace_Fulldoc) {
    store_document("list", subdoc_create_array(iterations));

    // No point in actually 'replacing' anything, just send the same original
    // thing.
    for (size_t i = 0; i < iterations; i++) {
        store_document("list", subdoc_create_array(iterations));
    }

    delete_object("list");
}

INSTANTIATE_TEST_CASE_P(
        SDPerf,
        SubdocPerfTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(ClientJSONSupport::No)),
        McdTestappTest::PrintToStringCombinedName);
