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

#include <cstring>
#include <gsl/gsl>
#include <limits>
#include <string>
#include <vector>
#include "testapp_client_test.h"
#include "testapp_subdoc_common.h"

#include <memcached/protocol_binary.h>
#include <memcached/util.h> // for memcached_protocol_errcode_2_text()
#include <nlohmann/json.hpp>
#include <platform/cb_malloc.h>
#include <platform/socket.h>
#include <protocol/mcbp/ewb_encode.h>

/*
 * testapp testcases for sub-document API - single path.
 */

// Maximum depth for a document (and path) is 32. Create documents
// that large and one bigger to test with.
const int MAX_SUBDOC_PATH_COMPONENTS = 32;


// Ensure the execution of the operation is successful
#define EXPECT_SD_OK(cmd) EXPECT_PRED_FORMAT1(subdoc_pred_ok, cmd)
#define ASSERT_SD_OK(cmd) ASSERT_PRED_FORMAT1(subdoc_pred_ok, cmd)

// Ensure the execution of the operation returns an error
#define EXPECT_SD_ERR(cmd, err) EXPECT_PRED_FORMAT2(subdoc_pred_errcode, cmd, err)

// Ensure the returned value is equal to val
#define EXPECT_SD_VALEQ(cmd, val) EXPECT_PRED_FORMAT2(subdoc_pred_value, cmd, val)
#define ASSERT_SD_VALEQ(cmd, val) ASSERT_PRED_FORMAT2(subdoc_pred_value, cmd, val)

// Ensure the path p in the document k is equal to v
#define EXPECT_SD_GET(k, p, v) \
    EXPECT_SD_VALEQ(           \
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocGet, k, p), v)

// Ensure that the given error AND value are returned
#define EXPECT_SUBDOC_CMD(cmd, err, val) EXPECT_PRED_FORMAT3(subdoc_pred_compat, cmd, err, val)

// Ensure the given error and value are returned. resp is used as an 'out'
// value.
#define EXPECT_SUBDOC_CMD_RESP(cmd, err, val, resp) EXPECT_PRED_FORMAT4(subdoc_pred_full, cmd, err, val, resp)

// Non JSON document, optionally compressed. Subdoc commands should fail.
void SubdocTestappTest::test_subdoc_get_binary(bool compress,
                                               cb::mcbp::ClientOpcode cmd,
                                               MemcachedConnection& conn) {
    const std::string not_JSON{"not; json"};
    store_document("binary", not_JSON);

    int notJsonCount = getResponseCount(cb::mcbp::Status::SubdocDocNotJson);

    // a). Check that access fails with DOC_NOTJSON
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "binary", "[0]"),
                  cb::mcbp::Status::SubdocDocNotJson);

    EXPECT_EQ(notJsonCount + 1,
              getResponseCount(cb::mcbp::Status::SubdocDocNotJson));

    delete_object("binary");
}

TEST_P(SubdocTestappTest, SubdocGet_BinaryRaw) {
    test_subdoc_get_binary(/*compress*/ false,
                           cb::mcbp::ClientOpcode::SubdocGet,
                           getConnection());
}
TEST_P(SubdocTestappTest, SubdocGet_BinaryCompressed) {
    test_subdoc_get_binary(
            /*compress*/ true,
            cb::mcbp::ClientOpcode::SubdocGet,
            getConnection());
}

TEST_P(SubdocTestappTest, SubdocExists_BinaryRaw) {
    test_subdoc_get_binary(/*compress*/ false,
                           cb::mcbp::ClientOpcode::SubdocExists,
                           getConnection());
}
TEST_P(SubdocTestappTest, SubdocExists_BinaryCompressed) {
    test_subdoc_get_binary(/*compress*/ true,
                           cb::mcbp::ClientOpcode::SubdocExists,
                           getConnection());
}

// retrieve from a JSON document consisting of a toplevel array.
void SubdocTestappTest::test_subdoc_fetch_array_simple(
        bool compressed, cb::mcbp::ClientOpcode cmd) {
    ASSERT_TRUE((cmd == cb::mcbp::ClientOpcode::SubdocGet) ||
                (cmd == cb::mcbp::ClientOpcode::SubdocExists));

    const std::string array{R"([ 0, "one", 2.0 ])"};
    store_document("array", array, 0 /* flags*/, 0 /* expiry */, compressed);

    // a). Check successful access to each array element.
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "array", "[0]"), "0");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "array", "[1]"), "\"one\"");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "array", "[2]"), "2.0");

    // b). Check successful access to last element (using -1).
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "array", "[-1]"), "2.0");

    // c). Check -2 treated as invalid index (only -1 permitted).
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "array", "[-2]"),
                  cb::mcbp::Status::SubdocPathEinval);
    reconnect_to_server();

    // d). Check failure accessing out-of-range index.
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "array", "[3]"),
                  cb::mcbp::Status::SubdocPathEnoent);
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "array", "[9999]"),
                  cb::mcbp::Status::SubdocPathEnoent);

    // e). Check failure accessing array as dict.
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "array", "missing_key"),
                  cb::mcbp::Status::SubdocPathMismatch);
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "array", "[2].nothing_here"),
                  cb::mcbp::Status::SubdocPathMismatch);

    // f). Check path longer than SUBDOC_PATH_MAX_LENGTH is invalid.
    std::string too_long_path(1024 + 1, '.');
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "array", too_long_path),
                  cb::mcbp::Status::Einval);
    reconnect_to_server();

    // g). Check that incorrect flags (i.e. non-zero) is invalid.
    EXPECT_SD_ERR(
            BinprotSubdocCommand(cmd, "array", "[0]", "", SUBDOC_FLAG_MKDIR_P),
            cb::mcbp::Status::Einval);
    reconnect_to_server();

    delete_object("array");
}

TEST_P(SubdocTestappTest, SubdocGet_ArraySimpleRaw) {
    test_subdoc_fetch_array_simple(/*compressed*/ false,
                                   cb::mcbp::ClientOpcode::SubdocGet);
}
TEST_P(SubdocTestappTest, SubdocGet_ArraySimpleCompressed) {
    test_subdoc_fetch_array_simple(/*compressed*/ true,
                                   cb::mcbp::ClientOpcode::SubdocGet);
}

TEST_P(SubdocTestappTest, SubdocExists_ArraySimpleRaw) {
    test_subdoc_fetch_array_simple(/*compressed*/ false,
                                   cb::mcbp::ClientOpcode::SubdocExists);
}
TEST_P(SubdocTestappTest, SubdocExists_ArraySimpleCompressed) {
    test_subdoc_fetch_array_simple(/*compressed*/ true,
                                   cb::mcbp::ClientOpcode::SubdocExists);
}

// JSON document containing toplevel dict.
void SubdocTestappTest::test_subdoc_fetch_dict_simple(
        bool compressed, cb::mcbp::ClientOpcode cmd) {
    ASSERT_TRUE((cmd == cb::mcbp::ClientOpcode::SubdocGet) ||
                (cmd == cb::mcbp::ClientOpcode::SubdocExists));

    const char dict[] = "{ \"int\": 1,"
                        "  \"string\": \"two\","
                        "  \"true\": true,"
                        "  \"false\": false }";
    store_document("dict", dict, 0 /*flags*/, 0 /*exptime*/, compressed);

    // a). Check successful access to each dict element.
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict", "int"), "1");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict", "string"), "\"two\"");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict", "true"), "true");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict", "false"), "false");

    // b). Check failure accessing non-existent keys.
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", "missing_key"),
                  cb::mcbp::Status::SubdocPathEnoent);

    // c). Check failure accessing object incorrectly (wrong type).
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", "[0]"),
                  cb::mcbp::Status::SubdocPathMismatch);
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", "[-1]"),
                  cb::mcbp::Status::SubdocPathMismatch);
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", "int.nothing_here"),
                  cb::mcbp::Status::SubdocPathMismatch);

    delete_object("dict");
}

TEST_P(SubdocTestappTest, SubdocGet_DictSimpleRaw) {
    test_subdoc_fetch_dict_simple(/*compressed*/ false,
                                  cb::mcbp::ClientOpcode::SubdocGet);
}
TEST_P(SubdocTestappTest, SubdocGet_DictSimpleCompressed) {
    test_subdoc_fetch_dict_simple(/*compressed*/ true,
                                  cb::mcbp::ClientOpcode::SubdocGet);
}

TEST_P(SubdocTestappTest, SubdocExists_DictSimpleRaw) {
    test_subdoc_fetch_dict_simple(/*compressed*/ false,
                                  cb::mcbp::ClientOpcode::SubdocExists);
}
TEST_P(SubdocTestappTest, SubdocExists_DictSimpleCompressed) {
    test_subdoc_fetch_dict_simple(/*compressed*/ true,
                                  cb::mcbp::ClientOpcode::SubdocExists);
}

// JSON document containing nested dictionary.
void SubdocTestappTest::test_subdoc_fetch_dict_nested(
        bool compressed, cb::mcbp::ClientOpcode cmd) {
    ASSERT_TRUE((cmd == cb::mcbp::ClientOpcode::SubdocGet) ||
                (cmd == cb::mcbp::ClientOpcode::SubdocExists));

    // Getting a bit complex to do raw (with all the quote escaping so use
    // JSON API
    nlohmann::json dict;
    dict["name"]["title"] = "Mr";
    dict["name"]["first"] = "Joseph";
    dict["name"]["last"] = "Bloggs";
    dict["orders"] = nlohmann::json::array();
    for (int i = 0; i < 10; i++) {
        nlohmann::json o;
        o["date"] = "2020-04-04T18:17:04Z";
        o["count"] = i * 3;
        o["description"] = std::string{"Cool project #"} + std::to_string(i);
        dict["orders"].emplace_back(o);
    }
    const auto dict_str = dict.dump();

    // Store to Couchbase, optionally compressing first.
    store_document("dict2", dict_str, 0 /*flags*/, 0 /*exptime*/, compressed);

    // a). Check successful access to individual nested components.
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict2", "name.title"), "\"Mr\"");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict2", "name.first"), "\"Joseph\"");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict2", "name.last"), "\"Bloggs\"");

    // b). Check successful access to a whole sub-dictionary.
    const auto name_str = dict["name"].dump();
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict2", "name"), name_str);

    // c). Check successful access to a whole sub-array.
    const auto orders_str = dict["orders"].dump();
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict2", "orders"), orders_str);

    // d). Check access to dict in array.
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "dict2", "orders[0].date"),
                    "\"2020-04-04T18:17:04Z\"");

    delete_object("dict2");
}

TEST_P(SubdocTestappTest, SubdocGet_DictNestedRaw) {
    test_subdoc_fetch_dict_nested(/*compressed*/ false,
                                  cb::mcbp::ClientOpcode::SubdocGet);
}
TEST_P(SubdocTestappTest, SubdocGet_DictNestedCompressed) {
    test_subdoc_fetch_dict_nested(/*compressed*/ true,
                                  cb::mcbp::ClientOpcode::SubdocGet);
}
TEST_P(SubdocTestappTest, SubdocExists_DictNestedRaw) {
    test_subdoc_fetch_dict_nested(/*compressed*/ false,
                                  cb::mcbp::ClientOpcode::SubdocExists);
}
TEST_P(SubdocTestappTest, SubdocExists_DictNestedCompressed) {
    test_subdoc_fetch_dict_nested(/*compressed*/ true,
                                  cb::mcbp::ClientOpcode::SubdocExists);
}

// Creates a nested dictionary with the specified number of levels.
static std::string make_nested_dict(int nlevels) {
    std::string ret = "{}";

    for (int depth = nlevels-1; depth > 0; depth--) {
        std::string s = "{\"" + std::to_string(depth) + "\":" + ret + "}";
        std::swap(ret, s);
    }

    return ret;
}

// Deeply nested JSON dictionary; verify limits on how deep documents can be.
void SubdocTestappTest::test_subdoc_fetch_dict_deep(
        cb::mcbp::ClientOpcode cmd) {
    // a). Should be able to access a deeply nested document as long as the
    // path we ask for is no longer than MAX_SUBDOC_PATH_COMPONENTS.
    store_document("max_dict", make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS));

    std::string valid_max_path(std::to_string(1));
    for (int depth = 2; depth < MAX_SUBDOC_PATH_COMPONENTS; depth++) {
        valid_max_path += std::string(".") + std::to_string(depth);
    }
    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "max_dict", valid_max_path), "{}");

    delete_object("max_dict");

    // b). Accessing a deeper document should fail.
    store_document("too_deep_dict",
                   make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS + 1));

    std::string too_long_path(std::to_string(1));
    for (int depth = 2; depth < MAX_SUBDOC_PATH_COMPONENTS + 1; depth++) {
        too_long_path += std::string(".") + std::to_string(depth);
    }
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "too_deep_dict", too_long_path),
                  cb::mcbp::Status::SubdocPathE2big);

    delete_object("too_deep_dict");
}

TEST_P(SubdocTestappTest, SubdocGet_DictDeep) {
    test_subdoc_fetch_dict_deep(cb::mcbp::ClientOpcode::SubdocGet);
}
TEST_P(SubdocTestappTest, SubdocExists_DictDeep) {
    test_subdoc_fetch_dict_deep(cb::mcbp::ClientOpcode::SubdocExists);
}

// Creates a nested array with the specified number of levels.

static std::string make_nested_array(int nlevels) {
    std::string ret = "[]";
    for (int depth = nlevels-1; depth > 0; depth--) {
        std::string s = "[" + ret + "]";
        std::swap(ret, s);
    }

    return ret;
}

std::string make_nested_array_path(int nlevels) {
    std::string path;
    for (int depth = 1; depth < nlevels; depth++) {
        path += "[0]";
    }
    return path;
}

// Deeply nested JSON array; verify limits on how deep documents can be.
void SubdocTestappTest::test_subdoc_fetch_array_deep(
        cb::mcbp::ClientOpcode cmd) {
    // a). Should be able to access a deeply nested document as long as the
    // path we ask for is no longer than MAX_SUBDOC_PATH_COMPONENTS.
    store_document("max_array", make_nested_array(MAX_SUBDOC_PATH_COMPONENTS));

    std::string valid_max_path(make_nested_array_path(MAX_SUBDOC_PATH_COMPONENTS));

    EXPECT_SD_VALEQ(BinprotSubdocCommand(cmd, "max_array", valid_max_path), "[]");
    delete_object("max_array");

    // b). Accessing a deeper array should fail.
    store_document("too_deep_array",
                   make_nested_array(MAX_SUBDOC_PATH_COMPONENTS + 1));

    std::string too_long_path(make_nested_array_path(MAX_SUBDOC_PATH_COMPONENTS + 1));

    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "too_deep_array", too_long_path),
                  cb::mcbp::Status::SubdocPathE2big);
    delete_object("too_deep_array");
}

TEST_P(SubdocTestappTest, SubdocGet_ArrayDeep) {
    test_subdoc_fetch_array_deep(cb::mcbp::ClientOpcode::SubdocGet);
}
TEST_P(SubdocTestappTest, SubdocExists_ArrayDeep) {
    test_subdoc_fetch_array_deep(cb::mcbp::ClientOpcode::SubdocExists);
}

/* Test adding to a JSON dictionary.
 * @param compress If true operate on compressed JSON documents.
 * @param cmd The binary protocol command to test. Permitted values are:
 *            - cb::mcbp::ClientOpcode::SubdocDictAdd
 *            - cb::mcbp::ClientOpcode::SubdocDictUpsert
 */
void SubdocTestappTest::test_subdoc_dict_add_simple(
        bool compress, cb::mcbp::ClientOpcode cmd) {
    ASSERT_TRUE((cmd == cb::mcbp::ClientOpcode::SubdocDictAdd) ||
                (cmd == cb::mcbp::ClientOpcode::SubdocDictUpsert));

    const std::vector<std::pair<std::string, std::string>> key_vals(
            {{"int", "2"},
             {"float", "2.0"},
             {"object", R"({ "foo": "bar" })"},
             {"array", R"([ "a", "b", "c"])"},
             {"true", "true"},
             {"false", "false"},
             {"null", "null"}});

    // a). Attempt to add to non-existent document should fail.
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", "int", "2"),
                  cb::mcbp::Status::KeyEnoent);

    // b). Attempt to add to non-JSON document should return ENOT_JSON
    const char not_JSON[] = "not; valid, JSON";
    store_document("binary", not_JSON, 0 /*flags*/, 0 /*exptime*/, compress);
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "binary", "int", "2"),
                  cb::mcbp::Status::SubdocDocNotJson);
    delete_object("binary");

    // Store a simple JSON document to work on.
    const std::string dict = R"({ "key1": 1 })";
    store_document("dict", dict, 0 /*flags*/, 0 /*exptime*/, compress);

    // c). Addition of primitive types to the dict.
    for (const auto& kv : key_vals) {
        EXPECT_SD_OK(BinprotSubdocCommand(cmd, "dict", kv.first, kv.second));
        EXPECT_SD_GET("dict", kv.first, kv.second);
    }

    // d). Check that attempts to add keys which already exist fail for DICT_ADD,
    // and are permitted for DICT_UPSERT.
    for (const auto& kv : key_vals) {
        BinprotSubdocCommand sd_cmd(cmd, "dict", kv.first, kv.second);
        if (cmd == cb::mcbp::ClientOpcode::SubdocDictAdd) {
            EXPECT_SD_ERR(sd_cmd, cb::mcbp::Status::SubdocPathEexists);
        } else { // DICT_UPSERT
            EXPECT_SD_OK(sd_cmd);
            EXPECT_SD_GET("dict", kv.first, kv.second);
        }
    }

    // e). Check that attempts to add keys with a missing intermediate
    // dict path fail.
    for (const auto& kv : key_vals) {
        auto key = "intermediate." + kv.first;
        EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", key, kv.second),
                      cb::mcbp::Status::SubdocPathEnoent);
    }

    // f). Check that attempts to add keys with missing intermediate
    // array path fail.
    for (const auto& kv : key_vals) {
        auto key = "intermediate_array[0]." + kv.first;
        EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", key, kv.second),
                      cb::mcbp::Status::SubdocPathEnoent);
    }

    // g). ... and they still fail even if MKDIR_P flag is specified (as
    // intermediate array paths are never automatically created).
    for (const auto& kv : key_vals) {
        auto key = "intermediate_array[0]." + kv.first;
        EXPECT_SD_ERR(BinprotSubdocCommand(
                              cmd, "dict", key, kv.second, SUBDOC_FLAG_MKDIR_P),
                      cb::mcbp::Status::SubdocPathEnoent);
    }

    // h) However attempts to add keys with _dict_ intermediate paths should
    // succeed if the MKDIR_P flag is set.
    for (const auto& kv : key_vals) {
        auto key = "intermediate." + kv.first;
        EXPECT_SD_OK(BinprotSubdocCommand(cmd, "dict", key, kv.second, SUBDOC_FLAG_MKDIR_P));
        EXPECT_SD_GET("dict", key, kv.second);
    }

    // i). Check that attempts to add various invalid JSON fragments all fail.
    const std::vector<std::pair<std::string, std::string>> invalid_key_vals({
            {"bad_int", "\"2"},
            {"bad_int2", "2a"},
            {"bad_int3", "0x2"},
            {"bad_int4", "2."},
            {"bad_float", "2.0a"},
            {"bad_float2", "2.0.0"},
            {"bad_object", "{ \"foo\": }"},
            {"bad_array", "[ \"a\" "},
            {"bad_array2", "[ \"a\" }"},
            {"bad_array3", "[ \"a\", }"},
            {"bad_true", "TRUE"},
            {"bad_false", "FALSE"},
            {"bad_null", "nul"},
    });
    for (const auto& kv : invalid_key_vals) {
        EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", kv.first, kv.second),
                      cb::mcbp::Status::SubdocValueCantinsert);
    }

    // j). Check CAS support - cmd with correct CAS should succeed.
    // Get the current CAS.
    BinprotSubdocResponse resp;
    EXPECT_SUBDOC_CMD_RESP(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocExists, "dict", "int"),
            cb::mcbp::Status::Success,
            "",
            resp);
    uint64_t cas = resp.getCas();
    EXPECT_NE(0, cas);
    EXPECT_SUBDOC_CMD_RESP(BinprotSubdocCommand(cmd,
                                                "dict",
                                                "new_int",
                                                "3",
                                                SUBDOC_FLAG_NONE,
                                                mcbp::subdoc::doc_flag::None,
                                                cas),
                           cb::mcbp::Status::Success,
                           "",
                           resp);

    uint64_t new_cas = resp.getCas();
    EXPECT_NE(cas, new_cas);

    // k). CAS - cmd with old cas should fail.
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd,
                                       "dict",
                                       "new_int2",
                                       "4",
                                       SUBDOC_FLAG_NONE,
                                       mcbp::subdoc::doc_flag::None,
                                       cas),
                  cb::mcbp::Status::KeyEexists);

    // l). CAS - manually corrupted (off by one) cas should fail.
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd,
                                       "dict",
                                       "new_int2",
                                       "4",
                                       SUBDOC_FLAG_NONE,
                                       mcbp::subdoc::doc_flag::None,
                                       new_cas + 1),
                  cb::mcbp::Status::KeyEexists);

    delete_object("dict");

    // m). Attempt to perform dict command on array should fail.
    store_document("array", "[1,2]", 0 /*flags*/, 0 /*exptime*/, compress);
    EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "array", "foo", "\"bar\""),
                  cb::mcbp::Status::SubdocPathMismatch);
    delete_object("array");

    // n). Check that attempts to add keys to a valid JSON fragment which is
    // not in a container fail. (We cannot operate on non-dict or array JSON
    // objects).
    store_document("dict", "\"string\"", 0 /*flags*/, 0 /*exptime*/, compress);
    for (const auto& kv : key_vals) {
        EXPECT_SD_ERR(BinprotSubdocCommand(cmd, "dict", kv.first, kv.second),
                      cb::mcbp::Status::SubdocDocNotJson);
    }
    delete_object("dict");
}

TEST_P(SubdocTestappTest, SubdocDictAdd_SimpleRaw) {
    test_subdoc_dict_add_simple(/*compress*/ false,
                                cb::mcbp::ClientOpcode::SubdocDictAdd);
}

TEST_P(SubdocTestappTest, SubdocDictAdd_SimpleCompressed) {
    test_subdoc_dict_add_simple(/*compress*/ true,
                                cb::mcbp::ClientOpcode::SubdocDictAdd);
}

TEST_P(SubdocTestappTest, SubdocDictUpsert_SimpleRaw) {
    test_subdoc_dict_add_simple(/*compress*/ false,
                                cb::mcbp::ClientOpcode::SubdocDictUpsert);
}

TEST_P(SubdocTestappTest, SubdocDictUpsert_SimpleCompressed) {
    test_subdoc_dict_add_simple(/*compress*/ true,
                                cb::mcbp::ClientOpcode::SubdocDictUpsert);
}

// Test FEATURE_MUTATION_SEQNO support.
TEST_P(SubdocTestappTest, SubdocDictAdd_SimpleRaw_MutationSeqno) {
    set_mutation_seqno_feature(true);
    test_subdoc_dict_add_simple(/*compress*/ false,
                                cb::mcbp::ClientOpcode::SubdocDictAdd);
    set_mutation_seqno_feature(false);
}

void SubdocTestappTest::test_subdoc_dict_add_cas(bool compress,
                                                 cb::mcbp::ClientOpcode cmd) {
    ASSERT_TRUE((cmd == cb::mcbp::ClientOpcode::SubdocDictAdd) ||
                (cmd == cb::mcbp::ClientOpcode::SubdocDictUpsert));

    // Store a simple JSON document to work on.
    store_document("dict", "{}", 0 /*flags*/, 0 /*exptime*/, compress);

    // a). Check that a CAS mismatch internally (between reading the JSON
    // (doc to operate on and storing it), is correctly retried.
    // (Note: the auto-retry only occurs when there is no CAS specified by the
    // user).

    // Configure the ewouldblock_engine to inject fake CAS failure for the
    // 3rd call (i.e. the 1st engine->store() attempt). We only expect 6 calls
    // total, so also make anything after that fail.
    ewouldblock_engine_configure({ewb::Passthrough,
                                  ewb::Passthrough,
                                  cb::engine_errc::key_already_exists,
                                  ewb::Passthrough,
                                  ewb::Passthrough,
                                  ewb::Passthrough});

    // .. Yet a client request should succeed, as internal CAS failure should
    // be retried.
    BinprotSubdocResponse resp;
    EXPECT_SUBDOC_CMD_RESP(BinprotSubdocCommand(cmd, "dict", "new_int3", "3"),
                           cb::mcbp::Status::Success,
                           "",
                           resp);
    uint64_t new_cas = resp.getCas();

    // b). Check that if the user specifies an explicit CAS, then a mismatch
    // isn't retried and EEXISTS is returned back to the user.

    // Setup ewouldblock_engine - first two calls succeed, 3rd (engine->store)
    // fails. Do not expect more than 3 calls so make any further calls error.
    ewouldblock_engine_configure({ewb::Passthrough,
                                  ewb::Passthrough,
                                  cb::engine_errc::key_already_exists});

    EXPECT_SD_ERR(BinprotSubdocCommand(cmd,
                                       "dict",
                                       "new_int4",
                                       "4",
                                       SUBDOC_FLAG_NONE,
                                       mcbp::subdoc::doc_flag::None,
                                       new_cas),
                  cb::mcbp::Status::KeyEexists);

    // Cleanup.
    ewouldblock_engine_disable();
    delete_object("dict");
}

TEST_P(SubdocTestappTest, SubdocDictAdd_CasRaw) {
    test_subdoc_dict_add_cas(/*compress*/ false,
                             cb::mcbp::ClientOpcode::SubdocDictAdd);
}
TEST_P(SubdocTestappTest, SubdocDictAdd_CasCompressed) {
    test_subdoc_dict_add_cas(/*compress*/ true,
                             cb::mcbp::ClientOpcode::SubdocDictAdd);
}
TEST_P(SubdocTestappTest, SubdocDictUpsert_CasRaw) {
    test_subdoc_dict_add_cas(/*compress*/ false,
                             cb::mcbp::ClientOpcode::SubdocDictUpsert);
}
TEST_P(SubdocTestappTest, SubdocDictUpsert_CasCompressed) {
    test_subdoc_dict_add_cas(/*compress*/ true,
                             cb::mcbp::ClientOpcode::SubdocDictUpsert);
}

// Check that if an item is set by another client after the fetch but
// before the store then we return NotStored.
// (MB-34393)
TEST_P(SubdocTestappTest, SubdocAddFlag_BucketStoreCas) {
    // Setup ewouldblock_engine - first two calls succeed, 3rd (engine->store)
    // fails with NOT_STORED (key already exists).
    ewouldblock_engine_configure({ewb::Passthrough,
                                  ewb::Passthrough,
                                  cb::engine_errc::not_stored});

    EXPECT_SD_ERR(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                       "AddCasTest",
                                       "element",
                                       "4",
                                       SUBDOC_FLAG_NONE,
                                       mcbp::subdoc::doc_flag::Add,
                                       0),
                  cb::mcbp::Status::NotStored);
}

void SubdocTestappTest::test_subdoc_dict_add_upsert_deep(
        cb::mcbp::ClientOpcode cmd) {
    ASSERT_TRUE((cmd == cb::mcbp::ClientOpcode::SubdocDictAdd) ||
                (cmd == cb::mcbp::ClientOpcode::SubdocDictUpsert));

    // a). Check that we can add elements to a document at the maximum nested
    // level.
    store_document("dict", make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS - 1));

    std::string one_less_max_path(std::to_string(1));
    for (int depth = 2; depth < MAX_SUBDOC_PATH_COMPONENTS - 1; depth++) {
        one_less_max_path += std::string(".") + std::to_string(depth);
    }
    // Check precondition - should have an empty dict we can access.
    EXPECT_SD_GET("dict", one_less_max_path, "{}");

    // a). Check we can add primitive elements to this path.
    const std::vector<std::pair<std::string, std::string>> primitive_key_vals({
            {"int", "2"},
            {"float", "2.0"},
            {"true", "true"},
            {"false", "false"},
            {"null", "null"}});
    for (const auto& kv : primitive_key_vals) {
        const auto key = one_less_max_path + "." + kv.first;
        EXPECT_SD_OK(BinprotSubdocCommand(cmd, "dict", key, kv.second));
        EXPECT_SD_GET("dict", key, kv.second);
    }

    delete_object("dict");
}

TEST_P(SubdocTestappTest, SubdocDictAdd_Deep) {
    test_subdoc_dict_add_upsert_deep(cb::mcbp::ClientOpcode::SubdocDictAdd);
}

TEST_P(SubdocTestappTest, SubdocDictUpsert_Deep) {
    test_subdoc_dict_add_upsert_deep(cb::mcbp::ClientOpcode::SubdocDictUpsert);
}

void SubdocTestappTest::test_subdoc_delete_simple(bool compress) {
    // a). Create a document containing each of the primitive types, and then
    // ensure we can successfully delete each type.
    const char dict[] = "{"
            "\"0\": 1,"
            "\"1\": 2.0,"
            "\"2\": 3.141e3,"
            "\"3\": \"four\","
            "\"4\": {\"foo\": \"bar\"},"
            "\"5\": [1, 1, 1, 1],"
            "\"6\": true,"
            "\"7\": false"
            "}";
    store_document("dict", dict, 0 /*flags*/, 0 /*exptime*/, compress);

    // Attempts to delete non-existent elements should fail.
    EXPECT_SD_ERR(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocDelete, "dict", "bad_key"),
            cb::mcbp::Status::SubdocPathEnoent);

    for (unsigned int ii = 0; ii < 8; ii++) {
        // Assert we can access it initially:
        std::string path(std::to_string(ii));
        BinprotSubdocResponse resp;
        EXPECT_SUBDOC_CMD_RESP(
                BinprotSubdocCommand(
                        cb::mcbp::ClientOpcode::SubdocExists, "dict", path),
                cb::mcbp::Status::Success,
                "",
                resp);
        uint64_t cas = resp.getCas();

        // Deleting with the wrong CAS should fail:
        EXPECT_SD_ERR(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDelete,
                                           "dict",
                                           path,
                                           "",
                                           SUBDOC_FLAG_NONE,
                                           mcbp::subdoc::doc_flag::None,
                                           cas + 1),
                      cb::mcbp::Status::KeyEexists);

        EXPECT_SD_OK(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocExists, "dict", path));

        // Should be able to delete with no CAS specified.
        EXPECT_SD_OK(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocDelete, "dict", path));

        // ... and should no longer exist:
        EXPECT_SD_ERR(
                BinprotSubdocCommand(
                        cb::mcbp::ClientOpcode::SubdocExists, "dict", path),
                cb::mcbp::Status::SubdocPathEnoent);
    }

    // After deleting everything the dictionary should be empty.
    validate_json_document("dict", "{}");
    delete_object("dict");
}

TEST_P(SubdocTestappTest, SubdocDelete_SimpleRaw) {
    test_subdoc_delete_simple(/*compress*/false);
}

TEST_P(SubdocTestappTest, SubdocDelete_SimpleCompressed) {
    test_subdoc_delete_simple(/*compress*/true);
}

TEST_P(SubdocTestappTest, SubdocDelete_Array) {
    // Create an array, then test deleting elements.
    store_document("a", "[0,1,2,3,4]");

    // Sanity check - 3rd element should be 2
    EXPECT_SD_GET("a", "[2]", "2");

    // a). Attempts to delete out of range elements should fail.
    EXPECT_SD_ERR(BinprotSubdocCommand(
                          cb::mcbp::ClientOpcode::SubdocDelete, "a", "[5]"),
                  cb::mcbp::Status::SubdocPathEnoent);

    // b). Test deleting at end of array.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "a", "[4]"));
    //     3rd element should still be 2; last element should now be 3.
    EXPECT_SD_GET("a", "[2]", "2");
    EXPECT_SD_GET("a", "[-1]", "3");
    validate_json_document("a", "[0,1,2,3]");

    // c). Test deleting at start of array; elements are shuffled down.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "a", "[0]"));
    //     3rd element should now be 3; last element should still be 3.
    EXPECT_SD_GET("a", "[2]", "3");
    EXPECT_SD_GET("a", "[-1]", "3");
    validate_json_document("a", "[1,2,3]");

    // d). Test deleting of last element using [-1].
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "a", "[-1]"));
    //     Last element should now be 2.
    EXPECT_SD_GET("a", "[-1]", "2");
    validate_json_document("a", "[1,2]");

    // e). Delete remaining elements.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "a", "[0]"));
    validate_json_document("a", "[2]");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "a", "[0]"));
    // Should have an empty array.
    validate_json_document("a", "[]");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocDelete_ArrayNested) {
    // Nested array containing different objects.
    store_document("b", R"([0,[10,20,[100]],{"key":"value"}])");

    // Sanity check - 2nd element should be "[10,20,[100]]"
    EXPECT_SD_GET("b", "[1]", "[10,20,[100]]");

    // a). Delete nested array element
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "b", "[1][2][0]"));
    EXPECT_SD_GET("b", "[1]", "[10,20,[]]");

    // b). Delete the (now empty) nested array.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "b", "[1][2]"));
    EXPECT_SD_GET("b", "[1]", "[10,20]");

    // c). Delete the next level up array.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "b", "[1]"));
    // element [1] should now be the dict.
    EXPECT_SD_GET("b", "[1]", "{\"key\":\"value\"}");

    delete_object("b");
}

const std::vector<std::string> JSON_VALUES({"1.1",
                                            "\"value\"",
                                            R"({"inner":"dict"})",
                                            "[1,2]",
                                            "true",
                                            "false",
                                            "null"});

TEST_P(SubdocTestappTest, SubdocReplace_SimpleDict) {
    // Simple dictionary, replace first element with various types.
    store_document("a", R"({"key":0,"key2":1})");

    // Sanity check - 'key' should be "0"
    EXPECT_SD_GET("a", "key", "0");

    // Replace the initial key with each primitive type:
    for (const auto& replace : JSON_VALUES) {
        EXPECT_SD_OK(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocReplace, "a", "key", replace));
        EXPECT_SD_GET("a", "key", replace);
    }
    // Sanity-check the final document
    validate_json_document("a", R"({"key":null,"key2":1})");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocReplace_SimpleArray) {
    // Simple array, replace first element with various types.
    store_document("a", "[0,1]");

    // Sanity check - [0] should be "0"
    EXPECT_SD_GET("a", "[0]", "0");

    // Replace the first element with each primitive type:
    for (const auto& replace : JSON_VALUES) {
        EXPECT_SD_OK(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocReplace, "a", "[0]", replace));
        EXPECT_SD_GET("a", "[0]", replace);
    }
    // Sanity-check the final document
    validate_json_document("a", "[null,1]");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocReplace_ArrayDeep) {
    // Test replacing in deeply nested arrays.

    // Create an array at one less than the maximum depth and an associated path.
    store_document("a", make_nested_array(MAX_SUBDOC_PATH_COMPONENTS));

    std::string valid_max_path(make_nested_array_path(MAX_SUBDOC_PATH_COMPONENTS));
    EXPECT_SD_GET("a", valid_max_path, "[]");

    // a). Should be able to replace an element at the max depth.
    std::string new_value("\"deep\"");
    EXPECT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocReplace,
                                      "a",
                                      valid_max_path,
                                      new_value));
    EXPECT_SD_GET("a", valid_max_path, new_value);

    // b). But adding a nested array (taking the document over the maximum
    // depth) should fail.
    EXPECT_SD_ERR(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocReplace,
                                       "a",
                                       valid_max_path,
                                       "[0]"),
                  cb::mcbp::Status::SubdocValueEtoodeep);

    // c). Replace the whole deep array with a single toplevel element.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocReplace, "a", "[0]", "[]"));
    EXPECT_SD_GET("a", "[0]", "[]");
    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocArrayPushLast_Simple) {
    // a). Empty array, append to it.
    store_document("a", "[]");
    BinprotSubdocCommand request(cb::mcbp::ClientOpcode::SubdocArrayPushLast);
    request.setPath("").setValue("0").setKey("a");
    EXPECT_SD_OK(request);
    EXPECT_SD_GET("a", "[0]", "0");
    validate_json_document("a", "[0]");

    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, "a", "", "1"));
    EXPECT_SD_GET("a", "[1]", "1");
    validate_json_document("a", "[0,1]");

    request.setValue("2").setKey("a");
    BinprotSubdocResponse resp;
    EXPECT_SUBDOC_CMD_RESP(request, cb::mcbp::Status::Success, "", resp);
    EXPECT_SD_GET("a", "[2]", "2");
    validate_json_document("a", "[0,1,2]");

    // b). Check that using the correct CAS succeeds.
    EXPECT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "a",
                                 "",
                                 "3",
                                 SUBDOC_FLAG_NONE,
                                 mcbp::subdoc::doc_flag::None,
                                 resp.getCas()));
    EXPECT_SD_GET("a", "[3]", "3"), validate_json_document("a", "[0,1,2,3]");

    // c). But using the wrong one fails.
    EXPECT_SD_ERR(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "a",
                                 "",
                                 "4",
                                 SUBDOC_FLAG_NONE,
                                 mcbp::subdoc::doc_flag::None,
                                 resp.getCas()),
            cb::mcbp::Status::KeyEexists);
    validate_json_document("a", "[0,1,2,3]");
    delete_object("a");

    // d). Check various other object types append successfully.
    store_document("b", "[]");
    int index = 0;
    for (const auto& value : JSON_VALUES) {
        EXPECT_SD_OK(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocArrayPushLast, "b", "", value));
        std::string path("[" + std::to_string(index) + "]");
        EXPECT_SD_GET("b", path, value);
        index++;
    }
    delete_object("b");

    // e). Check we can append multiple values at once.
    store_document("c", "[]");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, "c", "", "0,1"));
    validate_json_document("c", "[0,1]");
    EXPECT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "c",
                                 "",
                                 "\"two\",3.141,{\"four\":4}"));
    validate_json_document("c", R"([0,1,"two",3.141,{"four":4}])");

    delete_object("c");

    // f). Check MKDIR_P flag works.
    store_document("d", "{}");
    EXPECT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "d",
                                 "foo",
                                 "0",
                                 SUBDOC_FLAG_MKDIR_P));
    delete_object("d");
}

TEST_P(SubdocTestappTest, SubdocArrayPushLast_Nested) {
    // Operations on a nested array,
    // a). Begin with an empty nested array, append to it.
    store_document("a", "[[]]");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, "a", "", "1"));
    EXPECT_SD_GET("a", "[0]", "[]");
    EXPECT_SD_GET("a", "[1]", "1");
    validate_json_document("a", "[[],1]");

    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, "a", "", "2"));
    EXPECT_SD_GET("a", "[2]", "2");
    validate_json_document("a", "[[],1,2]");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocArrayPushFirst_Simple) {
    // a). Empty array, prepend to it.
    store_document("a", "[]");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, "a", "", "0"));
    EXPECT_SD_GET("a", "[0]", "0");
    validate_json_document("a", "[0]");

    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, "a", "", "1"));
    EXPECT_SD_GET("a", "[0]", "1");
    validate_json_document("a", "[1,0]");

    BinprotSubdocResponse resp;
    EXPECT_SUBDOC_CMD_RESP(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayPushFirst, "a", "", "2"),
            cb::mcbp::Status::Success,
            "",
            resp);
    EXPECT_SD_GET("a", "[0]", "2");
    validate_json_document("a", "[2,1,0]");

    // b). Check that using the correct CAS succeeds.
    EXPECT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 "a",
                                 "",
                                 "3",
                                 SUBDOC_FLAG_NONE,
                                 mcbp::subdoc::doc_flag::None,
                                 resp.getCas()));
    EXPECT_SD_GET("a", "[0]", "3");
    validate_json_document("a", "[3,2,1,0]");

    // c). But using the wrong one fails.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 "a",
                                 "",
                                 "4",
                                 SUBDOC_FLAG_NONE,
                                 mcbp::subdoc::doc_flag::None,
                                 resp.getCas()),
            cb::mcbp::Status::KeyEexists,
            "");
    validate_json_document("a", "[3,2,1,0]");
    delete_object("a");

    // d). Check various other object types prepend successfully.
    store_document("b", "[]");
    for (const auto& value : JSON_VALUES) {
        EXPECT_SD_OK(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocArrayPushFirst, "b", "", value));
        EXPECT_SD_GET("b", "[0]", value);
    }
    delete_object("b");

    // e). Check we can prepend multiple values at once.
    store_document("c", "[]");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, "c", "", "0,1"));
    validate_json_document("c", "[0,1]");
    EXPECT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 "c",
                                 "",
                                 "\"two\",3.141,{\"four\":4}"));
    validate_json_document("c", R"(["two",3.141,{"four":4},0,1])");
    delete_object("c");

    // f). Check MKDIR_P flag works.
    store_document("d", "{}");
    EXPECT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 "d",
                                 "foo",
                                 "0",
                                 SUBDOC_FLAG_MKDIR_P));
    delete_object("d");
}

TEST_P(SubdocTestappTest, SubdocArrayPushFirst_Nested) {
    // Operations on a nested array.
    // a). Begin with an empty nested array, prepend to it.
    store_document("a", "[[]]");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, "a", "", "1"));
    EXPECT_SD_GET("a", "[0]", "1");
    EXPECT_SD_GET("a", "[1]", "[]");
    validate_json_document("a", "[1,[]]");

    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, "a", "", "2"));
    EXPECT_SD_GET("a", "[0]", "2");
    validate_json_document("a", "[2,1,[]]");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocArrayAddUnique_Simple) {
    // Start with an array with a single element.
    store_document("a", "[]");

    // a). Add an element which doesn't already exist.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayAddUnique, "a", "", "0"));
    validate_json_document("a", "[0]");

    // b). Add an element which does already exist.
    EXPECT_SD_ERR(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayAddUnique, "a", "", "0"),
            cb::mcbp::Status::SubdocPathEexists);
    validate_json_document("a", "[0]");
    delete_object("a");

    // c). Larger array, add an element which already exists.
    std::string array("[0,1,2,3,4,5,6,7,8,9]");
    store_document("b", array);
    EXPECT_SD_ERR(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayAddUnique, "b", "", "6"),
            cb::mcbp::Status::SubdocPathEexists);
    validate_json_document("b", array);

    // d). Check that all permitted types of values can be added:
    const std::vector<std::string> valid_unique_values({
        "\"string\"",
        "10",
        "1.0",
        "true",
        "false",
        "null"});
    for (const auto& v : valid_unique_values) {
        EXPECT_SD_OK(BinprotSubdocCommand(
                cb::mcbp::ClientOpcode::SubdocArrayAddUnique, "b", "", v));
    }
    // ... and attempting to add a second time returns EEXISTS
    for (const auto& v : valid_unique_values) {
        EXPECT_SD_ERR(BinprotSubdocCommand(
                              cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                              "b",
                              "",
                              v),
                      cb::mcbp::Status::SubdocPathEexists);
    }

#if 0 // TODO: According to the spec this shouldn't be permitted, however it
      // currently works...
    // f). Check it is not permitted to add non-primitive types (arrays, objects).
    const std::vector<std::string> invalid_unique_values({
        "{\"foo\": \"bar\"}",
        "[0,1,2]"});
    for (const auto& v : invalid_unique_values) {
        EXPECT_SUBDOC_CMD(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                    "b", "", v),
                          cb::mcbp::Status::SubdocPathMismatch, "");
    }
#endif
    delete_object("b");

    // g). Attempts to add_unique to a array with non-primitive values should
    // fail.
    store_document("c", R"([{"a":"b"}])");
    EXPECT_SD_ERR(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayAddUnique, "c", "", "1"),
            cb::mcbp::Status::SubdocPathMismatch);
    delete_object("c");

    store_document("d", "[[1,2]]");
    EXPECT_SD_ERR(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayAddUnique, "d", "", "3"),
            cb::mcbp::Status::SubdocPathMismatch);
    delete_object("d");
}

TEST_P(SubdocTestappTest, SubdocArrayInsert_Simple) {
    // Start with an empty array.
    store_document("a", "[]");

    // a). Attempt to insert at position 0 should succeed.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayInsert, "a", "[0]", "2"));
    validate_json_document("a", "[2]");

    // b). Second insert at zero should succeed and shuffle existing element
    // down.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayInsert, "a", "[0]", "0"));
    validate_json_document("a", "[0,2]");

    // c). Insert at position 1 should shuffle down elements after, leave alone
    // elements before.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayInsert, "a", "[1]", "1"));
    validate_json_document("a", "[0,1,2]");

    // d). Insert at len(array) should add to the end, without moving existing
    // elements.
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocArrayInsert, "a", "[3]", "3"));
    validate_json_document("a", "[0,1,2,3]");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocArrayInsert_Invalid) {
    // Start with an empty array.
    store_document("a", "[]");

    // a). Attempt to insert past the end of the (empty) array should fail.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayInsert, "a", "[1]", "0"),
            cb::mcbp::Status::SubdocPathEnoent,
            "");
    validate_json_document("a", "[]");

    // b). Insert at position '-1' is invalid.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 "a",
                                 "[-1]",
                                 "3"),
            cb::mcbp::Status::SubdocPathEinval,
            "");
    reconnect_to_server();
    validate_json_document("a", "[]");

    // c). MKDIR_P flag is not valid for ARRAY_INSERT
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 "a",
                                 "[0]",
                                 "1",
                                 SUBDOC_FLAG_MKDIR_P),
            cb::mcbp::Status::Einval,
            "");
    reconnect_to_server();
    validate_json_document("a", "[]");

    // d). A path larger than len(array) should fail.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayInsert, "a", "[1]", "1"),
            cb::mcbp::Status::SubdocPathEnoent,
            "");
    validate_json_document("a", "[]");

    // e). A path whose has component isn't an array subscript should fail.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 "a",
                                 "[0].foo",
                                 "1"),
            cb::mcbp::Status::SubdocPathEinval,
            "");
    reconnect_to_server();
    validate_json_document("a", "[]");

    delete_object("a");

    // f). Attempt to insert to a dict should fail.
    store_document("b", "{}");
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocArrayInsert, "b", "[0]", "0"),
            cb::mcbp::Status::SubdocPathMismatch,
            "");
    validate_json_document("b", "{}");
    delete_object("b");
}

TEST_P(SubdocTestappTest, SubdocGetCount) {
    // Start with an empty array.
    store_document("a", "[]");

    // Get size. Should be 0
    EXPECT_SD_VALEQ(BinprotSubdocCommand(
                            cb::mcbp::ClientOpcode::SubdocGetCount, "a", ""),
                    "0");

    // Store it again, giving it some size
    store_document("a", "[1,2,3]");
    EXPECT_SD_VALEQ(BinprotSubdocCommand(
                            cb::mcbp::ClientOpcode::SubdocGetCount, "a", ""),
                    "3");

    // Check for mismatch
    store_document("a", R"({"k":"v"})");
    EXPECT_SD_ERR(BinprotSubdocCommand(
                          cb::mcbp::ClientOpcode::SubdocGetCount, "a", "k"),
                  cb::mcbp::Status::SubdocPathMismatch);

    // Check for non-found
    EXPECT_SD_ERR(BinprotSubdocCommand(
                          cb::mcbp::ClientOpcode::SubdocGetCount, "a", "n"),
                  cb::mcbp::Status::SubdocPathEnoent);
    delete_object("a");
}

void SubdocTestappTest::test_subdoc_counter_simple() {
    store_document("a", "{}");

    // a). Check that empty document, empty path creates a new element.
    EXPECT_SD_VALEQ(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "1"),
            "1");
    auto result = fetch_value("a");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("{\"key\":1}", result.second);

    // b). Check we can now increment it further.
    EXPECT_SD_VALEQ(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "1"),
            "2");
    result = fetch_value("a");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("{\"key\":2}", result.second);

    // c). Decrement by 2; should go back to zero.
    EXPECT_SD_VALEQ(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "-2"),
            "0");
    result = fetch_value("a");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("{\"key\":0}", result.second);

    // d). Decrement by 1; should go negative.
    EXPECT_SD_VALEQ(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "-1"),
            "-1");
    result = fetch_value("a");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("{\"key\":-1}", result.second);

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocCounter_Simple) {
    test_subdoc_counter_simple();
}

TEST_P(SubdocTestappTest, SubdocCounter_Simple_MutationSeqno) {
    set_mutation_seqno_feature(true);
    test_subdoc_counter_simple();
    set_mutation_seqno_feature(false);
}

static const std::vector<std::string> NOT_INTEGER({"true",
                                                   "false",
                                                   "null",
                                                   "\"string\"",
                                                   "[0]",
                                                   R"({"foo": "bar"})",
                                                   "1.1"});

TEST_P(SubdocTestappTest, SubdocCounter_InvalidNotInt) {
    // Cannot increment things which are not integers.
    for (auto& val : NOT_INTEGER) {
        const std::string doc("{\"key\":" + val + "}");
        store_document("a", doc);
        EXPECT_SD_ERR(
                BinprotSubdocCommand(
                        cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "1"),
                cb::mcbp::Status::SubdocPathMismatch);
        auto result = fetch_value("a");
        EXPECT_EQ(cb::mcbp::Status::Success, result.first);
        EXPECT_EQ(doc, result.second);
        delete_object("a");
    }
}

TEST_P(SubdocTestappTest, SubdocCounter_InvalidERange) {
    // Cannot increment things which are not representable as int64_t.
    const auto int64_max = std::numeric_limits<int64_t>::max();

    const std::vector<std::string> unrepresentable({
        std::to_string(uint64_t(int64_max) + 1),
        "-" + std::to_string(uint64_t(int64_max) + 2),
    });
    for (auto& val : unrepresentable) {
        const std::string doc("{\"key\":" + val + "}");
        store_document("b", doc);
        EXPECT_SUBDOC_CMD(
                BinprotSubdocCommand(
                        cb::mcbp::ClientOpcode::SubdocCounter, "b", "key", "1"),
                cb::mcbp::Status::SubdocNumErange,
                "");
        auto result = fetch_value("b");
        EXPECT_EQ(cb::mcbp::Status::Success, result.first);
        EXPECT_EQ(doc, result.second);
        delete_object("b");
    }
}

TEST_P(SubdocTestappTest, SubdocCounter_Limits) {
    // a). Attempting to increment value one less than int64_t::MAX by one
    //     should succeed.
    const int64_t max = std::numeric_limits<int64_t>::max();

    store_document("a", "{\"key\":" + std::to_string(max - 1) + "}");
    EXPECT_SD_VALEQ(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "1"),
            std::to_string(max));

    auto result = fetch_value("a");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("{\"key\":" + std::to_string(max) + "}", result.second);

    // b). A further increment by one should fail.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "1"),
            cb::mcbp::Status::SubdocValueCantinsert,
            "");

    delete_object("a");

    // c). Same with int64_t::min() and decrement.
    const int64_t min = std::numeric_limits<int64_t>::min();

    store_document("b", "{\"key\":" + std::to_string(min + 1) + "}");
    EXPECT_SD_VALEQ(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "b", "key", "-1"),
            std::to_string(min));

    result = fetch_value("b");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("{\"key\":" + std::to_string(min) + "}", result.second);

    // b). A further decrement by one should fail.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "b", "key", "-1"),
            cb::mcbp::Status::SubdocValueCantinsert,
            "");

    delete_object("b");
}

TEST_P(SubdocTestappTest, SubdocCounter_InvalidIncr) {
    // Cannot increment by a non-numeric value.
    const std::string doc("{\"key\":10}");
    store_document("a", doc);

    for (auto& incr : NOT_INTEGER) {
        EXPECT_SUBDOC_CMD(
                BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocCounter,
                                     "a",
                                     "key",
                                     incr),
                cb::mcbp::Status::SubdocDeltaEinval,
                "");
        auto result = fetch_value("a");
        EXPECT_EQ(cb::mcbp::Status::Success, result.first)
                << " using increment '" << incr << "'";
        EXPECT_EQ(doc, result.second);
    }

    // Cannot increment by zero.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocCounter, "a", "key", "0"),
            cb::mcbp::Status::SubdocDeltaEinval,
            "");
    auto result = fetch_value("a");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ(doc, result.second);

    delete_object("a");
}

// Test handling of the internal auto-retry when a CAS mismatch occurs due
// to the underlying document changing between subdoc reading the initial value
// and trying to write the new value (after applying the subdoc modification).
TEST_P(SubdocTestappTest, SubdocCASAutoRetry) {
    // Store a simple dict value to operate on.
    store_document("a", "{}");

    // 1. Setup ewouldblock_engine - make the first three store commands return
    // EXISTS.
    ewouldblock_engine_configure(
            cb::engine_errc::success, // not used for this mode
            EWBEngineMode::CasMismatch,
            3);

    // Issue a DICT_ADD without an explicit CAS. We should have an auto-retry
    // occur (and the command succeed).
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDictAdd, "a", "key1", "1"));

    // 2. Now retry with MAXIMUM_ATTEMPTS-1 CAS mismatches - this should still
    // succeed.
    ewouldblock_engine_configure(
            cb::engine_errc::success, // not used for this mode
            EWBEngineMode::CasMismatch,
            99);
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDictAdd, "a", "key2", "2"));

    // 3. Now with MAXIMUM_ATTEMPTS CAS mismatches - this should return TMPFAIL.
    ewouldblock_engine_configure(
            cb::engine_errc::success, // not used for this mode
            EWBEngineMode::CasMismatch,
            100);
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocDictAdd, "a", "key3", "3"),
            cb::mcbp::Status::Etmpfail,
            "");
    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocMkdoc_Array) {
    // Create new document (array)
    ASSERT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 "a",
                                 "",
                                 "0",
                                 SUBDOC_FLAG_NONE,
                                 mcbp::subdoc::doc_flag::Mkdoc,
                                 0));
    EXPECT_SD_GET("a", "[0]", "0");

    // Flag doesn't do anything if doc already exists
    ASSERT_SD_OK(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "a",
                                 "",
                                 "1",
                                 SUBDOC_FLAG_NONE,
                                 mcbp::subdoc::doc_flag::Mkdoc,
                                 0));
    EXPECT_SD_GET("a", "[1]", "1");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocMkdoc_Dict) {
    // Create new document (dictionary)
    ASSERT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                      "a",
                                      "foo",
                                      "1",
                                      SUBDOC_FLAG_NONE,
                                      mcbp::subdoc::doc_flag::Mkdoc,
                                      0));
    EXPECT_SD_GET("a", "foo", "1");

    // Flag doesn't do anything if doc already exists
    ASSERT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                      "a",
                                      "bar",
                                      "2",
                                      SUBDOC_FLAG_NONE,
                                      mcbp::subdoc::doc_flag::Mkdoc,
                                      0));
    EXPECT_SD_GET("a", "bar", "2");

    // Flag still has MKDIR_P semantics if needed
    ASSERT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                      "a",
                                      "nested.path",
                                      "3",
                                      SUBDOC_FLAG_NONE,
                                      mcbp::subdoc::doc_flag::Mkdoc,
                                      0));
    EXPECT_SD_GET("a", "nested.path", "3");

    delete_object("a");
}

TEST_P(SubdocTestappTest, SubdocMkdoc_Counter) {
    // Counter should also work (with a path) + MKDIR_P
    ASSERT_SD_VALEQ(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocCounter,
                                         "a",
                                         "counter.path",
                                         "42",
                                         SUBDOC_FLAG_NONE,
                                         mcbp::subdoc::doc_flag::Mkdoc,
                                         0),
                    "42");
    // Repeat should be OK
    ASSERT_SD_VALEQ(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocCounter,
                                         "a",
                                         "counter.path",
                                         "42",
                                         SUBDOC_FLAG_NONE,
                                         mcbp::subdoc::doc_flag::Mkdoc,
                                         0),
                    "84");
    delete_object("a");
}

// Test operation of setting document expiry for single-path commands.
TEST_P(SubdocTestappTest, SubdocExpiry_Single) {
    // Create two documents; one to be used for an explicit expiry time and one
    // for an explicit 0s (i.e. never) expiry.
    uint32_t expiryTime = 5;
    store_document("ephemeral", "[\"a\"]");
    store_document("permanent", "[\"a\"]");

    // Expiry not permitted for SUBDOC_GET operations.
    BinprotSubdocCommand get(
            cb::mcbp::ClientOpcode::SubdocGet, "ephemeral", "[0]");
    get.setExpiry(666);
    EXPECT_SD_ERR(get, cb::mcbp::Status::Einval);
    reconnect_to_server();

    // Perform a REPLACE operation, setting a expiry of 1s.
    BinprotSubdocCommand replace(
            cb::mcbp::ClientOpcode::SubdocReplace, "ephemeral", "[0]", "\"b\"");
    EXPECT_SD_OK(replace.setExpiry(expiryTime));

    // Try to read the document immediately - should exist.
    auto result = fetch_value("ephemeral");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("[\"b\"]", result.second);

    // Perform a REPLACE, explicitly encoding an expiry of 0s.
    BinprotSubdocCommand replace2(
            cb::mcbp::ClientOpcode::SubdocReplace, "permanent", "[0]", "\"b\"");
    EXPECT_SD_OK(replace2.setExpiry(0));

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

/*
 * Test for MB-32364, checking that the single path mutation does not ignore
 * the expiry if docflags are included in extras.
 */
TEST_P(SubdocTestappTest, SubdocExpiry_DocflagUpsertSingle) {
    uint32_t expiryTime = 5;
    BinprotSubdocCommand upsert1(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                 "doc_flag",
                                 "foo",
                                 "\"a\"",
                                 SUBDOC_FLAG_NONE,
                                 mcbp::subdoc::doc_flag::Mkdoc);
    upsert1.setExpiry(expiryTime);
    EXPECT_SUBDOC_CMD(upsert1, cb::mcbp::Status::Success, "");

    // Try to read the doc_flag document immediately - should exist.
    auto result = fetch_value("doc_flag");
    EXPECT_EQ(cb::mcbp::Status::Success, result.first);
    EXPECT_EQ("{\"foo\":\"a\"}", result.second);

    // Move memcached time forward to expire the ephemeral document
    adjust_memcached_clock(
            (expiryTime * 2),
            cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);

    // Try to read the doc_flag document - shouldn't exist.
    result = fetch_value("doc_flag");
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, result.first);

    // Cleanup - Reset memcached clock
    adjust_memcached_clock(
            0, cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);
}

// Test handling of not-my-vbucket for a SUBDOC_GET
TEST_P(SubdocTestappTest, SubdocGet_NotMyVbucket) {
    store_document("array", "[0]");

    // Make the next engine operation (get) return NOT_MY_VBUCKET.
    ewouldblock_engine_configure(
            cb::engine_errc::not_my_vbucket, EWBEngineMode::Next_N, 1);

    // Should fail with NOT-MY-VBUCKET, and a non-zero length body including the
    // cluster config.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocGet, "array", "[0]"),
            cb::mcbp::Status::NotMyVbucket,
            "");

    // Second attempt should succced (as only next 1 engine op was set to fail).
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(
                    cb::mcbp::ClientOpcode::SubdocGet, "array", "[0]"),
            cb::mcbp::Status::Success,
            "0");

    delete_object("array");
}

// Test handling of not-my-vbucket for a SUBDOC_DICT_ADD
TEST_P(SubdocTestappTest, SubdocArrayPushLast_NotMyVbucket) {
    store_document("array", "[0]");

    // Configure the ewouldblock_engine to inject fake NOT-MY-VBUCKET failure
    // for the 3rd call (i.e. the 1st engine->store() attempt). We only expect 6 calls
    // total, so also make anything after that fail.
    ewouldblock_engine_configure({ewb::Passthrough,
                                  ewb::Passthrough,
                                  cb::engine_errc::not_my_vbucket,
                                  ewb::Passthrough,
                                  ewb::Passthrough,
                                  ewb::Passthrough});

    // Should fail with NOT-MY-VBUCKET, and a non-zero length body including the
    // cluster config.
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "array",
                                 "",
                                 "1"),
            cb::mcbp::Status::NotMyVbucket,
            "");

    // Second attempt should succced (as only next 1 engine op was set to fail).
    EXPECT_SUBDOC_CMD(
            BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "array",
                                 "",
                                 "1"),
            cb::mcbp::Status::Success,
            "");

    // Cleanup.
    ewouldblock_engine_disable();
    delete_object("array");
}

// Test that flags are preserved by subdoc mutation operations.
TEST_P(SubdocTestappTest, SubdocFlags) {
    const char array[] = "[0]";
    const uint32_t flags = 0xcafebabe;
    store_object_w_datatype("array", array, flags, 0, cb::mcbp::Datatype::Raw);

    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocReplace, "array", "[0]", "1"));

    validate_json_document("array", "[1]");
    validate_flags("array", flags);

    delete_object("array");
}

// Test that locked items are properly handled
TEST_P(SubdocTestappTest, SubdocLockedItem) {
    store_document("item", "{}");

    // Lock the object
    auto doc = getConnection().get_and_lock("item", Vbid(0), 10);

    // Construct the subdoc command
    BinprotSubdocCommand sd_cmd(
            cb::mcbp::ClientOpcode::SubdocDictUpsert, "item", "p", "true");
    sd_cmd.setCas(0);
    EXPECT_SUBDOC_CMD(sd_cmd, cb::mcbp::Status::Etmpfail, "");

    // Set the CAS to -1
    sd_cmd.setCas(-1);
    EXPECT_SUBDOC_CMD(sd_cmd, cb::mcbp::Status::Etmpfail, "");

    // Set our "normal" CAS back (
    sd_cmd.setCas(doc.info.cas);
    EXPECT_SUBDOC_CMD(sd_cmd, cb::mcbp::Status::Success, "");

    validate_json_document("item", "{\"p\":true}");

    delete_object("item");
}


enum class SubdocCmdType {
    Lookup,
    Mutation
};

static const SubdocStatTraits LOOKUP_TRAITS { "cmd_subdoc_lookup",
                                              "bytes_subdoc_lookup_total",
                                              "bytes_subdoc_lookup_extracted" };

static const SubdocStatTraits MUTATION_TRAITS { "cmd_subdoc_mutation",
                                                "bytes_subdoc_mutation_total",
                                                "bytes_subdoc_mutation_inserted" };

void SubdocTestappTest::test_subdoc_stats_command(cb::mcbp::ClientOpcode cmd,
                                                  SubdocStatTraits traits,
                                                  const std::string& doc,
                                                  const std::string& path,
                                                  const std::string& value,
                                                  const std::string& fragment,
                                                  size_t expected_total_len,
                                                  size_t expected_subset_len) {
    store_document("doc", doc);

    // Get initial stats
    auto stats = request_stats();
    auto count_before = extract_single_stat(stats, traits.count_name);
    auto bytes_before_total = extract_single_stat(stats, traits.bytes_total_name);
    auto bytes_before_subset = extract_single_stat(stats, traits.bytes_extracted_subset);

    // Perform the operation
    EXPECT_SUBDOC_CMD(BinprotSubdocCommand(cmd, "doc", path, value),
                      cb::mcbp::Status::Success,
                      fragment);

    // Get subsequent stats, check stat increased by one.
    stats = request_stats();
    auto count_after = extract_single_stat(stats, traits.count_name);
    auto bytes_after_total = extract_single_stat(stats, traits.bytes_total_name);
    auto bytes_after_subset = extract_single_stat(stats, traits.bytes_extracted_subset);

    EXPECT_EQ(1, count_after - count_before);
    EXPECT_EQ(expected_total_len, bytes_after_total - bytes_before_total);
    EXPECT_EQ(expected_subset_len, bytes_after_subset - bytes_before_subset);

    delete_object("doc");
}

TEST_P(SubdocTestappTest, SubdocStatsLookupGet) {
    std::string doc("[10,11,12,13,14,15,16,17,18,19]");
    std::string response("10");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocGet,
                              LOOKUP_TRAITS,
                              doc,
                              "[0]",
                              "",
                              response,
                              doc.size(),
                              response.size());
}
TEST_P(SubdocTestappTest, SubdocStatsLookupExists) {
    std::string doc("[10,11,12,13,14,15,16,17,18,19]");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocExists,
                              LOOKUP_TRAITS,
                              doc,
                              "[0]",
                              "",
                              "",
                              doc.size(),
                              0);
}
TEST_P(SubdocTestappTest, SubdocStatsDictAdd) {
    std::string input(R"({"foo":1,"bar":2})");
    std::string path("baz");
    std::string fragment("3");
    std::string result(R"({"foo":1,"bar":2,"baz":3})");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocDictAdd,
                              MUTATION_TRAITS,
                              input,
                              path,
                              fragment,
                              "",
                              result.size(),
                              fragment.size());
}
TEST_P(SubdocTestappTest, SubdocStatsDictUpsert) {
    std::string input(R"({"foo":1,"bar":2})");
    std::string path("bar");
    std::string fragment("3");
    std::string result(R"({"foo":1,"bar":3})");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                              MUTATION_TRAITS,
                              input,
                              path,
                              fragment,
                              "",
                              result.size(),
                              fragment.size());
}
TEST_P(SubdocTestappTest, SubdocStatsDelete) {
    std::string input(R"({"foo":1,"bar":2,"baz":3})");
    std::string path("baz");
    std::string result(R"({"foo":1,"bar":2})");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocDelete,
                              MUTATION_TRAITS,
                              input,
                              path,
                              "",
                              "",
                              result.size(),
                              0);
}
TEST_P(SubdocTestappTest, SubdocStatsReplace) {
    std::string input(R"({"foo":1,"bar":2})");
    std::string path("bar");
    std::string fragment("3");
    std::string result(R"({"foo":1,"bar":3})");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocReplace,
                              MUTATION_TRAITS,
                              input,
                              path,
                              fragment,
                              "",
                              result.size(),
                              fragment.size());
}
TEST_P(SubdocTestappTest, SubdocStatsArrayPushLast) {
    std::string input("[10,11,12,13,14,15,16,17,18,19]");
    std::string fragment("20");
    std::string result("[10,11,12,13,14,15,16,17,18,19,20]");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                              MUTATION_TRAITS,
                              input,
                              "",
                              fragment,
                              "",
                              result.size(),
                              fragment.size());
}
TEST_P(SubdocTestappTest, SubdocStatsArrayPushFirst) {
    std::string input("[10,11,12,13,14,15,16,17,18,19]");
    std::string fragment("9");
    std::string result("[9,10,11,12,13,14,15,16,17,18,19]");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                              MUTATION_TRAITS,
                              input,
                              "",
                              fragment,
                              "",
                              result.size(),
                              fragment.size());
}
TEST_P(SubdocTestappTest, SubdocStatsArrayInsert) {
    std::string input("[9,11,12,13,14,15,16,17,18,19]");
    std::string path("[0]");
    std::string fragment("10");
    std::string result("[9,10,11,12,13,14,15,16,17,18,19]");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                              MUTATION_TRAITS,
                              input,
                              path,
                              fragment,
                              "",
                              result.size(),
                              fragment.size());
}
TEST_P(SubdocTestappTest, SubdocStatsArrayAddUnique) {
    std::string input("[10,11,12,13,14,15,16,17,18,19]");
    std::string fragment("20");
    std::string result("[10,11,12,13,14,15,16,17,18,19,20]");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                              MUTATION_TRAITS,
                              input,
                              "",
                              fragment,
                              "",
                              result.size(),
                              fragment.size());
}
TEST_P(SubdocTestappTest, SubdocStatsCounter) {
    std::string input(R"({"foo":1,"bar":2})");
    std::string path("bar");
    std::string fragment("1");
    std::string result(R"({"foo":1,"bar":3})");
    test_subdoc_stats_command(cb::mcbp::ClientOpcode::SubdocCounter,
                              MUTATION_TRAITS,
                              input,
                              path,
                              fragment,
                              "3",
                              result.size(),
                              fragment.size());
}

TEST_P(SubdocTestappTest, SubdocUTF8PathTest) {
    // Check that using UTF8 characters in the path works, which it should

    const char dict[] = "{ \"kéy1\": 1 }";
    store_document("dict", dict);
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDictAdd, "dict", "kéy2👏", "56"));
    EXPECT_SD_GET("dict", "kéy2👏", "56");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "dict", "kéy2👏", ""));
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDictUpsert, "dict", "kéy2👏", "44"));
    EXPECT_SD_GET("dict", "kéy2👏", "44");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDictUpsert, "dict", "kéy2👏", "99"));
    EXPECT_SD_GET("dict", "kéy2👏", "99");
    validate_json_document("dict", R"({ "kéy1": 1 ,"kéy2👏":99})");
}

TEST_P(SubdocTestappTest, SubdocUTF8ValTest) {
    // Check that using UTF8 characters in the value works, which it should

    const char dict[] =
            R"({ "key1": "Ẇ̴̷̦͔͖͚̝̟̋̽ͪ̾ͤ̈́ͯͮͮ̀͗̌ͭ̾͜h̨̥̞͖̬̠͍͖̘̹͎͌̇̂̃ͯͣ͗̆̌̑ͨ̍̊ͪ̆̾̆̚͟͞ȧ̛̰̞̗̞̬̣̹͎̰̝͍͈̮̖̘̫̤̟͆̈́̒͗ͦ̋̓̌̊̋͝ͅţ͒ͮ͋̋̔̽ͥ̂ͭ̒̉̔̃ͫ̌̆̆҉̹͙̟̩̖̩̹̳̜͚̜̜ ͎̲͕̺̔̿̀͒̈́̏̌ͬͫ͒͂ͩͦ̀͝â̢̡̘̫̮̞̩̰̎ͨ̾ͤ̈́͑̉̈ͧ͆̃ͩ͆̚͡ ̧̢̛̙͔̰̹̲̱͔̤̝͖̥͚͓̲̪̯̟̖̏͒̽ͬ̂ͫͩͭ͋̏͊̽͗͊̀ͭ̋͘c̵̴͐̉̇͂̋ͬ̇̃͊ͨ͗̆̄̊́͏̡̡̫̦̦͉̼̙̜͉̯̮̪̫͍̩̼̘̫̻͍o̸̷͕̭̼̺̤͖͚̯̪̥̘̪̼̝̩̮͕̥̟̐͒̏ͭͦͮ̒ͧ̔̉̅̂͜͢͡o̷̢̡͕̟͓̺͚̟̱̜̻͇̘͍̤͓̲ͣͫ̾͛͗̅̐̏͑͆͌̀͜ͅͅͅl̴̡̙̹͖̈̄̌͒ͣ͒̅̏̕ ͛̐̿͋ͦ͛͌̄ͫ̒ͪ͊̀ͤ̀̿͏̶̡҉҉̤͎͖v̸̵̱͇̲͎̩͚̩͈̙̜̳̞̭̯̩̻̮̪ͯ̋̔͗̃̊ͬͮ̄̃͛̂̒̍͘͘a̦̝͇̙̬̬̰̪͙̗̟͙̝̬͛͂͑ͣ̓͑̏ͤ̑̀̚̚͘l͊̔́͋̋ͫ̈́̿̈̉̀͏̡͍͇̲̙̺̮͠uͬ̄̋̔ͪͧͥ͛ͭ̏̅ͫ͊̚͏̧͈̠̱͇͉̦̫͎͠ę̸͔̯̭̤͕̱͈̖͖̯̭̞͈͖ͨ̑̌̓̈ͮ͂̆̀͟ͅ" })";
    store_document("dict", dict);
    EXPECT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictAdd,
                                      "dict",
                                      "key2",
                                      "\"Ẇ̴̷̦͔͖͚̝̟̋̽ͪ̾ͤ̈́ͯͮͮ̀͗̌ͭ̾͜h̨̥̞͖̬̠͍͖̘̹͎͌̇̂̃ͯͣ͗̆̌̑ͨ̍̊ͪ̆̾̆̚͟͞ȧ̛̰̞̗̞̬̣̹͎̰̝͍͈̮̖̘̫̤̟͆̈́̒͗ͦ̋̓̌̊̋͝ͅţ͒ͮ͋̋̔̽ͥ̂ͭ̒̉̔̃ͫ̌̆̆҉̹͙̟̩̖̩̹̳̜͚̜̜ ͎̲͕̺̔̿̀͒̈́̏̌ͬͫ͒͂ͩͦ̀͝â̢̡̘̫̮̞̩̰̎ͨ̾ͤ̈́͑̉̈ͧ͆̃ͩ͆̚͡ "
                                      "̧̢̛̙͔̰̹̲̱͔̤̝͖̥͚͓̲̪̯̟̖̏͒̽ͬ̂ͫͩͭ͋̏͊̽͗͊̀ͭ̋͘c̵̴͐̉̇͂̋ͬ̇̃͊ͨ͗̆̄̊́͏̡̡̫̦̦͉̼̙̜͉̯̮̪̫͍̩̼̘̫̻͍o̸̷͕̭̼̺̤͖͚̯̪̥̘̪̼̝̩̮͕̥̟̐͒̏ͭͦͮ̒ͧ̔̉̅̂͜͢͡o̷̢̡͕̟͓̺͚̟̱̜̻͇̘͍̤͓̲ͣͫ̾͛͗̅̐̏͑͆͌̀͜ͅͅͅl̴̡̙̹͖̈̄̌͒ͣ͒̅̏̕"
                                      " "
                                      "͛̐̿͋ͦ͛͌̄ͫ̒ͪ͊̀ͤ̀̿͏̶̡҉҉̤͎͖v̸̵̱͇̲͎̩͚̩͈̙̜̳̞̭̯̩̻̮̪ͯ̋̔͗̃̊ͬͮ̄̃͛̂̒̍͘͘a̦̝͇̙̬̬̰̪͙̗̟͙̝̬͛͂͑ͣ̓͑̏ͤ̑̀̚̚͘l͊̔́͋̋ͫ̈́̿̈̉̀͏̡͍͇̲̙̺̮͠uͬ̄̋̔ͪͧͥ͛ͭ̏̅ͫ͊̚͏̧͈̠̱͇͉̦̫͎͠ę̸͔̯̭̤͕̱͈̖͖̯̭̞͈͖ͨ̑̌̓̈ͮ͂̆̀͟"
                                      "ͅ"
                                      "\""));
    EXPECT_SD_GET("dict", "key2", "\"Ẇ̴̷̦͔͖͚̝̟̋̽ͪ̾ͤ̈́ͯͮͮ̀͗̌ͭ̾͜h̨̥̞͖̬̠͍͖̘̹͎͌̇̂̃ͯͣ͗̆̌̑ͨ̍̊ͪ̆̾̆̚͟͞ȧ̛̰̞̗̞̬̣̹͎̰̝͍͈̮̖̘̫̤̟͆̈́̒͗ͦ̋̓̌̊̋͝ͅţ͒ͮ͋̋̔̽ͥ̂ͭ̒̉̔̃ͫ̌̆̆҉̹͙̟̩̖̩̹̳̜͚̜̜ ͎̲͕̺̔̿̀͒̈́̏̌ͬͫ͒͂ͩͦ̀͝â̢̡̘̫̮̞̩̰̎ͨ̾ͤ̈́͑̉̈ͧ͆̃ͩ͆̚͡ ̧̢̛̙͔̰̹̲̱͔̤̝͖̥͚͓̲̪̯̟̖̏͒̽ͬ̂ͫͩͭ͋̏͊̽͗͊̀ͭ̋͘c̵̴͐̉̇͂̋ͬ̇̃͊ͨ͗̆̄̊́͏̡̡̫̦̦͉̼̙̜͉̯̮̪̫͍̩̼̘̫̻͍o̸̷͕̭̼̺̤͖͚̯̪̥̘̪̼̝̩̮͕̥̟̐͒̏ͭͦͮ̒ͧ̔̉̅̂͜͢͡o̷̢̡͕̟͓̺͚̟̱̜̻͇̘͍̤͓̲ͣͫ̾͛͗̅̐̏͑͆͌̀͜ͅͅͅl̴̡̙̹͖̈̄̌͒ͣ͒̅̏̕ ͛̐̿͋ͦ͛͌̄ͫ̒ͪ͊̀ͤ̀̿͏̶̡҉҉̤͎͖v̸̵̱͇̲͎̩͚̩͈̙̜̳̞̭̯̩̻̮̪ͯ̋̔͗̃̊ͬͮ̄̃͛̂̒̍͘͘a̦̝͇̙̬̬̰̪͙̗̟͙̝̬͛͂͑ͣ̓͑̏ͤ̑̀̚̚͘l͊̔́͋̋ͫ̈́̿̈̉̀͏̡͍͇̲̙̺̮͠uͬ̄̋̔ͪͧͥ͛ͭ̏̅ͫ͊̚͏̧͈̠̱͇͉̦̫͎͠ę̸͔̯̭̤͕̱͈̖͖̯̭̞͈͖ͨ̑̌̓̈ͮ͂̆̀͟ͅ\"");
    EXPECT_SD_OK(BinprotSubdocCommand(
            cb::mcbp::ClientOpcode::SubdocDelete, "dict", "key2", ""));
    validate_json_document(
            "dict",
            R"({ "key1": "Ẇ̴̷̦͔͖͚̝̟̋̽ͪ̾ͤ̈́ͯͮͮ̀͗̌ͭ̾͜h̨̥̞͖̬̠͍͖̘̹͎͌̇̂̃ͯͣ͗̆̌̑ͨ̍̊ͪ̆̾̆̚͟͞ȧ̛̰̞̗̞̬̣̹͎̰̝͍͈̮̖̘̫̤̟͆̈́̒͗ͦ̋̓̌̊̋͝ͅţ͒ͮ͋̋̔̽ͥ̂ͭ̒̉̔̃ͫ̌̆̆҉̹͙̟̩̖̩̹̳̜͚̜̜ ͎̲͕̺̔̿̀͒̈́̏̌ͬͫ͒͂ͩͦ̀͝â̢̡̘̫̮̞̩̰̎ͨ̾ͤ̈́͑̉̈ͧ͆̃ͩ͆̚͡ ̧̢̛̙͔̰̹̲̱͔̤̝͖̥͚͓̲̪̯̟̖̏͒̽ͬ̂ͫͩͭ͋̏͊̽͗͊̀ͭ̋͘c̵̴͐̉̇͂̋ͬ̇̃͊ͨ͗̆̄̊́͏̡̡̫̦̦͉̼̙̜͉̯̮̪̫͍̩̼̘̫̻͍o̸̷͕̭̼̺̤͖͚̯̪̥̘̪̼̝̩̮͕̥̟̐͒̏ͭͦͮ̒ͧ̔̉̅̂͜͢͡o̷̢̡͕̟͓̺͚̟̱̜̻͇̘͍̤͓̲ͣͫ̾͛͗̅̐̏͑͆͌̀͜ͅͅͅl̴̡̙̹͖̈̄̌͒ͣ͒̅̏̕ ͛̐̿͋ͦ͛͌̄ͫ̒ͪ͊̀ͤ̀̿͏̶̡҉҉̤͎͖v̸̵̱͇̲͎̩͚̩͈̙̜̳̞̭̯̩̻̮̪ͯ̋̔͗̃̊ͬͮ̄̃͛̂̒̍͘͘a̦̝͇̙̬̬̰̪͙̗̟͙̝̬͛͂͑ͣ̓͑̏ͤ̑̀̚̚͘l͊̔́͋̋ͫ̈́̿̈̉̀͏̡͍͇̲̙̺̮͠uͬ̄̋̔ͪͧͥ͛ͭ̏̅ͫ͊̚͏̧͈̠̱͇͉̦̫͎͠ę̸͔̯̭̤͕̱͈̖͖̯̭̞͈͖ͨ̑̌̓̈ͮ͂̆̀͟ͅ" })");
}

// MB-30278: Perform a DICT_ADD followed by SD_GET to a path with backticks in
// them; to verify the path caching of un-escaped components is correctly reset
// between calls.
TEST_P(SubdocTestappTest, MB_30278_SubdocBacktickLookup) {
    // Test DICT_ADD where the element key contains a literal backtick.
    store_document("doc", "{}");
    // Add a key with literal backtick (which we escape by passing
    // double-backtick)
    EXPECT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictAdd,
                                      "doc",
                                      "key``",
                                      "\"value\""));
    EXPECT_SD_GET("doc", "key``", "\"value\"");
    validate_json_document("doc", R"({"key`":"value"})");
}

// MB-30278: Perform two DICT_ADD calls to paths with backticks in them;
// to verify the path caching of un-escaped components is correctly reset
// between calls.
TEST_P(SubdocTestappTest, MB_30278_SubdocBacktickDictAdd) {
    store_document("doc", "{}");
    EXPECT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictAdd,
                                      "doc",
                                      "key``",
                                      "\"value\""));
    EXPECT_SD_OK(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictAdd,
                                      "doc",
                                      "key2``",
                                      "\"value2\""));
    validate_json_document("doc", R"({"key`":"value","key2`":"value2"})");
}

// MB-34367: If a multi-mutation with durability requirements times out,
// ensure the status is reported correctly.
TEST_P(SubdocTestappTest, SubdocDurabilityTimeout) {
    /// Need XERORR given we are using sync_write_ambiguous
    set_xerror_feature(true);

    // Setup ewouldblock_engine to simulate a timeout - first 3 calls should
    // succeed, bucket_store should return ewouldblock and 'pending' IO return
    // sync_write_ambiguous.
    ewouldblock_engine_configure({cb::engine_errc(-1),
                                  cb::engine_errc(-1),
                                  cb::engine_errc::would_block,
                                  cb::engine_errc::sync_write_ambiguous});
    EXPECT_SD_ERR(BinprotSubdocCommand(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                       "durability_timeout",
                                       "element",
                                       "4",
                                       SUBDOC_FLAG_NONE,
                                       mcbp::subdoc::doc_flag::Mkdoc),
                  cb::mcbp::Status::SyncWriteAmbiguous);
}

INSTANTIATE_TEST_SUITE_P(
        Subdoc,
        SubdocTestappTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No)),
        McdTestappTest::PrintToStringCombinedName);

// Tests how a single worker handles multiple "concurrent" connections
// performing operations.
class WorkerConcurrencyTest : public TestappTest {
public:
protected:
    void SetUp() override {
        TestappTest::SetUp();
        reconnect_to_server();
        // Set ewouldblock_engine test harness to default mode.
        ewouldblock_engine_configure(cb::engine_errc::would_block,
                                     EWBEngineMode::First,
                                     /*unused*/ 0);
    }

public:
    static void SetUpTestCase() {
        memcached_cfg = generate_config();
        // Change the number of worker threads to one so we guarantee that
        // multiple connections are handled by a single worker.
        memcached_cfg["threads"] = 1;
        start_memcached_server();

        if (HasFailure()) {
            std::cerr << "Error in WorkerConcurrencyTest::SetUpTestCase, "
                         "terminating process"
                      << std::endl;
            exit(EXIT_FAILURE);
        } else {
            CreateTestBucket();
        }
    }
};

TEST_F(WorkerConcurrencyTest, SubdocArrayPushLast_Concurrent) {
    // Concurrently add to two different array documents, using two connections.

    // Setup the initial empty objects.
    store_document("a", "[]");
    store_document("b", "[]");

    // Create an additional second connection to memcached.
    SOCKET* current_sock = &sock;
    SOCKET sock1 = *current_sock;
    SOCKET sock2 = connect_to_server_plain();
    ASSERT_NE(sock2, INVALID_SOCKET);
    sock = sock1;

    const size_t push_count = 100;
    std::vector<uint8_t> send_buf;

    // Build pipeline for the even commands.
    std::string expected_a;
    for (unsigned int i = 0; i < push_count; i += 2) {
        expected_a += std::to_string(i) + ",";
        BinprotSubdocCommand cmd(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "a",
                                 "",
                                 std::to_string(i));
        std::vector<uint8_t> cmd_buf;
        cmd.encode(cmd_buf);
        send_buf.insert(send_buf.end(), cmd_buf.begin(), cmd_buf.end());
    }
    *current_sock = sock1;
    safe_send(send_buf.data(), send_buf.size(), false);

    // .. and the odd commands.
    send_buf.clear();
    std::string expected_b;
    for (unsigned int i = 1; i < push_count; i += 2) {
        expected_b += std::to_string(i) + ",";
        BinprotSubdocCommand cmd(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 "b",
                                 "",
                                 std::to_string(i));
        std::vector<uint8_t> cmd_buf;
        cmd.encode(cmd_buf);
        send_buf.insert(send_buf.end(), cmd_buf.begin(), cmd_buf.end());
    }
    *current_sock = sock2;
    safe_send(send_buf.data(), send_buf.size(), false);

    // Fixup the expected values - remove the trailing comma and bookend with
    // [ ].
    expected_a.insert(0, "[");
    expected_a.replace(expected_a.size() - 1, 1, "]");
    expected_b.insert(0, "[");
    expected_b.replace(expected_b.size() - 1, 1, "]");

    // Consume all the responses we should be expecting back.
    for (unsigned int i = 0; i < push_count; i++) {
        sock = (i % 2) ? sock1 : sock2;
        recv_subdoc_response(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                             cb::mcbp::Status::Success,
                             "");
    }

    // Validate correct data was written.
    validate_json_document("a", expected_a);
    validate_json_document("b", expected_b);

    // Restore original socket; free second one.
    *current_sock = sock1;
    cb::net::closesocket(sock2);

    delete_object("a");
    delete_object("b");
}
