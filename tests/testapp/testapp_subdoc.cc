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

#include "testapp_subdoc.h"

#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include <cJSON.h>
#include <memcached/protocol_binary.h>
#include <memcached/util.h> // for memcached_protocol_errcode_2_text()

#include "../utilities/protocol2text.h"

/*
 * testapp testcases for sub-document API.
 */

// Maximum depth for a document (and path) is 32. Create documents
// that large and one bigger to test with.
const int MAX_SUBDOC_PATH_COMPONENTS = 32;

std::ostream& operator<<(std::ostream& os, const SubdocCmd& obj)
{
    os << "[cmd:" << memcached_opcode_2_text(obj.cmd)
       << " key:" << obj.key << " path:" << obj.path << " value:" << obj.value
       << " flags:" << obj.flags << " cas:" << obj.cas << "]";
    return os;
}

/* Encode the specified subdoc command into `buf`.
 *
 * @param lengrh The size of the encoded data
 */
void encode_subdoc_command(char* buf, size_t bufsz, const SubdocCmd& cmd,
                           ssize_t& length) {
    protocol_binary_request_subdocument* request = (protocol_binary_request_subdocument*)buf;
    ASSERT_GE(bufsz,
              (sizeof(*request) + cmd.key.size() + cmd.path.size() + cmd.value.size()));

    // Always need a key.
    ASSERT_FALSE(cmd.key.empty());

    // path is encoded in extras as a uint16_t.
    ASSERT_LT(cmd.path.size(), std::numeric_limits<uint16_t>::max());

    memset(request, 0, sizeof(*request));

    // Populate the header.
    protocol_binary_request_header* const header = &request->message.header;
    header->request.magic = PROTOCOL_BINARY_REQ;
    header->request.opcode = cmd.cmd;
    header->request.keylen = htons(cmd.key.size());
    header->request.extlen = sizeof(uint16_t) + sizeof(uint8_t);
    header->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    header->request.vbucket = 0;
    header->request.bodylen = htonl(header->request.extlen + cmd.key.size() +
                                    cmd.path.size() + cmd.value.size());
    header->request.opaque = 0xdeadbeef;
    header->request.cas = cmd.cas;

    // Add extras: flags, pathlen.
    request->message.extras.subdoc_flags = cmd.flags;
    request->message.extras.pathlen = htons(cmd.path.size());

    // Add Body: key; path; value if applicable.
    const off_t key_offset = sizeof(*header) + header->request.extlen;
    memcpy(buf + key_offset, cmd.key.data(), cmd.key.size());

    const off_t path_offset = key_offset + cmd.key.size();
    memcpy(buf + path_offset, cmd.path.data(), cmd.path.size());

    const off_t value_offset = path_offset + cmd.path.size();
    if (!cmd.value.empty()) {
        memcpy(buf + value_offset, cmd.value.data(), cmd.value.size());
    }

    const off_t encoded_bytes = value_offset + cmd.value.size();
    length = encoded_bytes;
}

/* Encodes and sends a sub-document command, without waiting for any response.
 */
void send_subdoc_cmd(const SubdocCmd& cmd) {
    union {
        protocol_binary_request_subdocument request;
        char bytes[2048];
    } send;

    ssize_t len = -1;
    encode_subdoc_command(send.bytes, sizeof(send.bytes), cmd, len);
    if (len == -1) {
        FAIL() << "Failed to encode subdoc command " << cmd;
    }

    safe_send(send.bytes, len, false);
}

uint64_t recv_subdoc_response(protocol_binary_command expected_cmd,
                              protocol_binary_response_status expected_status,
                              const std::string& expected_value) {
    union {
        protocol_binary_response_subdocument response;
        char bytes[1024];
    } receive;

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));

    validate_response_header((protocol_binary_response_no_extras*)&receive.response,
                             expected_cmd, expected_status);

    // TODO: Check extras for subdoc command and mutation / seqno (if enabled).

    const protocol_binary_response_header* header = &receive.response.message.header;
    const char* val_ptr = receive.bytes + sizeof(*header) +
                          header->response.extlen;
    const size_t vallen = header->response.bodylen + header->response.extlen;

    if (!expected_value.empty() &&
        (expected_cmd != PROTOCOL_BINARY_CMD_SUBDOC_EXISTS)) {
        const std::string val(val_ptr, val_ptr + vallen);
        EXPECT_EQ(expected_value, val);
    } else {
        // Expect zero length on success (on error the error message string is
        // returned).
        if (header->response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            EXPECT_EQ(0u, vallen);
        }
    }
    return header->response.cas;
}

uint64_t expect_subdoc_cmd(const SubdocCmd& cmd,
                           protocol_binary_response_status expected_status,
                           const std::string& expected_value) {
    send_subdoc_cmd(cmd);
    return recv_subdoc_response(cmd.cmd, expected_status, expected_value);
}

void store_object(const std::string& key,
                  const std::string& value,
                  bool JSON, bool compress) {
    const char* payload = value.c_str();
    size_t payload_len = value.size();
    char* deflated = NULL;
    if (compress) {
        payload_len = compress_document(payload, payload_len, &deflated);
        payload = deflated;
    }

    set_datatype_feature(true);
    cb_assert(store_object_w_datatype(key.c_str(), payload, payload_len,
                                      compress, JSON) == TEST_PASS);
    set_datatype_feature(false);
    if (compress) {
        free(deflated);
    }
}

// Non JSON document, optionally compressed. Subdoc commands should fail.
void test_subdoc_get_binary(bool compress, protocol_binary_command cmd) {
    const char not_JSON[] = "not; json";
    store_object("binary", not_JSON);

    // a). Check that access fails with DOC_NOTJSON
    expect_subdoc_cmd(SubdocCmd(cmd, "binary", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON, "");

    delete_object("binary");
}

TEST_P(McdTestappTest, SubdocGet_BinaryRaw) {
    test_subdoc_get_binary(/*compress*/false, PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
TEST_P(McdTestappTest, SubdocGet_BinaryCompressed) {
    test_subdoc_get_binary(/*compress*/true, PROTOCOL_BINARY_CMD_SUBDOC_GET);
}

TEST_P(McdTestappTest, SubdocExists_BinaryRaw) {
    test_subdoc_get_binary(/*compress*/false,
                           PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
TEST_P(McdTestappTest, SubdocExists_BinaryCompressed) {
    test_subdoc_get_binary(/*compress*/true,
                           PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// retrieve from a JSON document consisting of a toplevel array.
void test_subdoc_fetch_array_simple(bool compressed, protocol_binary_command cmd) {

    ASSERT_TRUE((cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
                (cmd == PROTOCOL_BINARY_CMD_SUBDOC_EXISTS));

    const char array[] = "[ 0, \"one\", 2.0 ]";
    store_object("array", array, /*JSON*/true, compressed);

    // a). Check successful access to each array element.
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "0");

    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "\"one\"");

    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[2]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2.0");

    // b). Check successful access to last element (using -1).
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[-1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2.0");

    // c). Check -2 treated as invalid index (only -1 permitted).
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[-2]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL, "");

    // d). Check failure accessing out-of-range index.
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[3]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[9999]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");

    // e). Check failure accessing array as dict.
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "missing_key"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[2].nothing_here"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");

    // f). Check path longer than SUBDOC_PATH_MAX_LENGTH is invalid.
    std::string too_long_path(1024 + 1, '.');
    expect_subdoc_cmd(SubdocCmd(cmd, "array", too_long_path),
                      PROTOCOL_BINARY_RESPONSE_EINVAL, "");

    // g). Check that incorrect flags (i.e. non-zero) is invalid.
    expect_subdoc_cmd(SubdocCmd(cmd, "array", "[0]", "", SUBDOC_FLAG_MKDIR_P),
                      PROTOCOL_BINARY_RESPONSE_EINVAL, "");

    delete_object("array");
}

TEST_P(McdTestappTest, SubdocGet_ArraySimpleRaw) {
    test_subdoc_fetch_array_simple(/*compressed*/false,
                                   PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
TEST_P(McdTestappTest, SubdocGet_ArraySimpleCompressed) {
    test_subdoc_fetch_array_simple(/*compressed*/true,
                                   PROTOCOL_BINARY_CMD_SUBDOC_GET);
}

TEST_P(McdTestappTest, SubdocExists_ArraySimpleRaw) {
    test_subdoc_fetch_array_simple(/*compressed*/false,
                                   PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
TEST_P(McdTestappTest, SubdocExists_ArraySimpleCompressed) {
    test_subdoc_fetch_array_simple(/*compressed*/true,
                                   PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// JSON document containing toplevel dict.
void test_subdoc_fetch_dict_simple(bool compressed,
                                   protocol_binary_command cmd) {

    ASSERT_TRUE((cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
                (cmd == PROTOCOL_BINARY_CMD_SUBDOC_EXISTS));

    const char dict[] = "{ \"int\": 1,"
                        "  \"string\": \"two\","
                        "  \"true\": true,"
                        "  \"false\": false }";
    store_object("dict", dict, /*JSON*/true, compressed);

    // a). Check successful access to each dict element.
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "int"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "1");
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "string"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "\"two\"");
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "true"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "true");
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "false"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "false");

    // b). Check failure accessing non-existent keys.
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "missing_key"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");

    // c). Check failure accessing object incorrectly (wrong type).
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "[-1]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "int.nothing_here"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");

    delete_object("dict");
}

TEST_P(McdTestappTest, SubdocGet_DictSimpleRaw) {
    test_subdoc_fetch_dict_simple(/*compressed*/false,
                                  PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
TEST_P(McdTestappTest, SubdocGet_DictSimpleCompressed) {
    test_subdoc_fetch_dict_simple(/*compressed*/true,
                                  PROTOCOL_BINARY_CMD_SUBDOC_GET);
}

TEST_P(McdTestappTest, SubdocExists_DictSimpleRaw) {
    test_subdoc_fetch_dict_simple(/*compressed*/false,
                                  PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
TEST_P(McdTestappTest, SubdocExists_DictSimpleCompressed) {
    test_subdoc_fetch_dict_simple(/*compressed*/true,
                                  PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// JSON document containing nested dictionary.
void test_subdoc_fetch_dict_nested(bool compressed,
                                   protocol_binary_command cmd) {

    ASSERT_TRUE((cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
                (cmd == PROTOCOL_BINARY_CMD_SUBDOC_EXISTS));

    // Getting a bit complex to do raw (with all the quote escaping so use
    // cJSON API.
    unique_cJSON_ptr dict(cJSON_CreateObject());
    cJSON* name = cJSON_CreateObject();
    cJSON_AddStringToObject(name, "title", "Mr");
    cJSON_AddStringToObject(name, "first", "Joseph");
    cJSON_AddStringToObject(name, "last", "Bloggs");
    cJSON_AddItemToObject(dict.get(), "name", name);

    cJSON* orders = cJSON_CreateArray();
    for (int i = 0; i < 10; i++) {
        cJSON* order = cJSON_CreateObject();
        std::string order_name("order_" + std::to_string(i));
        cJSON_AddStringToObject(order, "date", "2020-04-04T18:17:04Z");
        cJSON_AddNumberToObject(order, "count", i * 3);
        std::string desc("Cool project #" + std::to_string(i));
        cJSON_AddStringToObject(order, "description", desc.c_str());
        cJSON_AddItemToArray(orders, order);
    }
    cJSON_AddItemToObject(dict.get(), "orders", orders);

    char* dict_str = cJSON_PrintUnformatted(dict.get());

    // Store to Couchbase, optionally compressing first.
    store_object("dict2", dict_str, /*JSON*/true, compressed);
    cJSON_Free(dict_str);

    // a). Check successful access to individual nested components.
    expect_subdoc_cmd(SubdocCmd(cmd, "dict2", "name.title"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "\"Mr\"");
    expect_subdoc_cmd(SubdocCmd(cmd, "dict2", "name.first"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "\"Joseph\"");
    expect_subdoc_cmd(SubdocCmd(cmd, "dict2", "name.last"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "\"Bloggs\"");

    // b). Check successful access to a whole sub-dictionary.
    char* name_str = cJSON_PrintUnformatted(name);
    expect_subdoc_cmd(SubdocCmd(cmd, "dict2", "name"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, name_str);
    cJSON_Free(name_str);

    // c). Check successful access to a whole sub-array.
    char* orders_str = cJSON_PrintUnformatted(orders);
    expect_subdoc_cmd(SubdocCmd(cmd, "dict2", "orders"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, orders_str);
    cJSON_Free(orders_str);

    // d). Check access to dict in array.
    expect_subdoc_cmd(SubdocCmd(cmd, "dict2", "orders[0].date"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS,
                      "\"2020-04-04T18:17:04Z\"");

    delete_object("dict2");
}

TEST_P(McdTestappTest, SubdocGet_DictNestedRaw) {
    test_subdoc_fetch_dict_nested(/*compressed*/false,
                                  PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
TEST_P(McdTestappTest, SubdocGet_DictNestedCompressed) {
    test_subdoc_fetch_dict_nested(/*compressed*/true,
                                  PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
TEST_P(McdTestappTest, SubdocExists_DictNestedRaw) {
    test_subdoc_fetch_dict_nested(/*compressed*/false,
                                  PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
TEST_P(McdTestappTest, SubdocExists_DictNestedCompressed) {
    test_subdoc_fetch_dict_nested(/*compressed*/true,
                                  PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// Creates a nested dictionary with the specified number of levels.
// Caller is responsible for calling cJSON_Free() on the result when finished
//with.
static cJSON* make_nested_dict(int nlevels) {
    cJSON* child = cJSON_CreateObject();
    cJSON* parent;
    for (int depth = nlevels-1; depth > 0; depth--) {
        std::string name(std::to_string(depth));
        parent = cJSON_CreateObject();
        cJSON_AddItemToObject(parent, name.c_str(), child);
        child = parent;
    }
    return parent;
}

// Deeply nested JSON dictionary; verify limits on how deep documents can be.
void test_subdoc_fetch_dict_deep(protocol_binary_command cmd) {

    // a). Should be able to access a deeply nested document as long as the
    // path we ask for is no longer than MAX_SUBDOC_PATH_COMPONENTS.
    unique_cJSON_ptr max_dict(make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS));
    char* max_dict_str = cJSON_PrintUnformatted(max_dict.get());
    store_object("max_dict", max_dict_str);

    cJSON_Free(max_dict_str);

    std::string valid_max_path(std::to_string(1));
    for (int depth = 2; depth < MAX_SUBDOC_PATH_COMPONENTS; depth++) {
        valid_max_path += std::string(".") + std::to_string(depth);
    }
    expect_subdoc_cmd(SubdocCmd(cmd, "max_dict", valid_max_path),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "{}");

    delete_object("max_dict");

    // b). Accessing a deeper document should fail.
    unique_cJSON_ptr too_deep_dict(make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS + 1));
    char* too_deep_dict_str = cJSON_PrintUnformatted(too_deep_dict.get());
    store_object("too_deep_dict", too_deep_dict_str);
    cJSON_Free(too_deep_dict_str);

    std::string too_long_path(std::to_string(1));
    for (int depth = 2; depth < MAX_SUBDOC_PATH_COMPONENTS + 1; depth++) {
        too_long_path += std::string(".") + std::to_string(depth);
    }
    expect_subdoc_cmd(SubdocCmd(cmd, "too_deep_dict", too_long_path),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG, "");

    delete_object("too_deep_dict");
}

TEST_P(McdTestappTest, SubdocGet_DictDeep) {
    test_subdoc_fetch_dict_deep(PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
TEST_P(McdTestappTest, SubdocExists_DictDeep) {
    test_subdoc_fetch_dict_deep(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// Creates a nested array with the specified number of levels.
// Caller is responsible for calling cJSON_Free() on the result when finished
//with.
static cJSON* make_nested_array(int nlevels) {
    cJSON* child = cJSON_CreateArray();
    cJSON* parent;
    for (int depth = nlevels-1; depth > 0; depth--) {
        parent = cJSON_CreateArray();
        cJSON_AddItemToArray(parent, child);
        child = parent;
    }
    return parent;
}

std::string make_nested_array_path(int nlevels) {
    std::string path;
    for (int depth = 1; depth < nlevels; depth++) {
        path += "[0]";
    }
    return path;
}

// Deeply nested JSON array; verify limits on how deep documents can be.
void test_subdoc_fetch_array_deep(protocol_binary_command cmd) {

    // a). Should be able to access a deeply nested document as long as the
    // path we ask for is no longer than MAX_SUBDOC_PATH_COMPONENTS.

    unique_cJSON_ptr max_array(make_nested_array(MAX_SUBDOC_PATH_COMPONENTS));
    char* max_array_str = cJSON_PrintUnformatted(max_array.get());
    store_object("max_array", max_array_str);
    cJSON_Free(max_array_str);

    std::string valid_max_path(make_nested_array_path(MAX_SUBDOC_PATH_COMPONENTS));

    expect_subdoc_cmd(SubdocCmd(cmd, "max_array", valid_max_path),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "[]");
    delete_object("max_array");

    // b). Accessing a deeper array should fail.
    unique_cJSON_ptr too_deep_array(make_nested_array(MAX_SUBDOC_PATH_COMPONENTS + 1));
    char* too_deep_array_str = cJSON_PrintUnformatted(too_deep_array.get());
    store_object("too_deep_array", too_deep_array_str);
    cJSON_Free(too_deep_array_str);

    std::string too_long_path(make_nested_array_path(MAX_SUBDOC_PATH_COMPONENTS + 1));

    expect_subdoc_cmd(SubdocCmd(cmd, "too_deep_array", too_long_path),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG, "");
    delete_object("too_deep_array");
}

TEST_P(McdTestappTest, SubdocGet_ArrayDeep) {
    test_subdoc_fetch_array_deep(PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
TEST_P(McdTestappTest, SubdocExists_ArrayDeep) {
    test_subdoc_fetch_array_deep(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

/* Test adding to a JSON dictionary.
 * @param compress If true operate on compressed JSON documents.
 * @param cmd The binary protocol command to test. Permitted values are:
 *            - PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD
 *            - PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT
 */
void test_subdoc_dict_add_simple(bool compress, protocol_binary_command cmd) {
    ASSERT_TRUE((cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD) ||
                (cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT));

    const std::vector<std::pair<std::string, std::string>> key_vals({
            {"int", "2"},
            {"float", "2.0"},
            {"object", "{ \"foo\": \"bar\" }"},
            {"array", "[ \"a\", \"b\", \"c\"]"},
            {"true", "true"},
            {"false", "false"},
            {"null", "null"}});

    // a). Attempt to add to non-existent document should fail.
    expect_subdoc_cmd(SubdocCmd(cmd, "dict",
                                "int", "2"),
                      PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "");

    // b). Attempt to add to non-JSON document should return ENOT_JSON
    const char not_JSON[] = "not; valid, JSON";
    store_object("binary", not_JSON, /*JSON*/false, compress);
    expect_subdoc_cmd(SubdocCmd(cmd, "binary", "int", "2"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON, "");
    delete_object("binary");

    // Store a simple JSON document to work on.
    const char dict[] = "{ \"key1\": 1 }";
    store_object("dict", dict, /*JSON*/true, compress);

    // c). Addition of primitive types to the dict.
    for (const auto& kv : key_vals) {
        expect_subdoc_cmd(SubdocCmd(cmd, "dict", kv.first, kv.second),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "dict",
                                    kv.first),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, kv.second);
    }

    // d). Check that attempts to add keys which already exist fail for DICT_ADD,
    // and are permitted for DICT_UPSERT.
    for (const auto& kv : key_vals) {
        SubdocCmd sd_cmd(cmd, "dict", kv.first, kv.second);
        if (cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD) {
            expect_subdoc_cmd(sd_cmd,
                              PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS, "");
        } else { // DICT_UPSERT
            expect_subdoc_cmd(sd_cmd,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
            expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "dict",
                                        kv.first),
                              PROTOCOL_BINARY_RESPONSE_SUCCESS, kv.second);
        }
    }

    // e). Check that attempts to add keys with a missing intermediate
    // dict path fail.
    for (const auto& kv : key_vals) {
        auto key = "intermediate." + kv.first;
        expect_subdoc_cmd(SubdocCmd(cmd, "dict", key, kv.second),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    }

    // f). Check that attempts to add keys with missing intermediate
    // array path fail.
    for (const auto& kv : key_vals) {
        auto key = "intermediate_array[0]." + kv.first;
        expect_subdoc_cmd(SubdocCmd(cmd, "dict", key, kv.second),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    }

    // g). ... and they still fail even if MKDIR_P flag is specified (as
    // intermediate array paths are never automatically created).
    for (const auto& kv : key_vals) {
        auto key = "intermediate_array[0]." + kv.first;
        expect_subdoc_cmd(SubdocCmd(cmd, "dict", key, kv.second,
                                    SUBDOC_FLAG_MKDIR_P),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    }

    // h) However attempts to add keys with _dict_ intermediate paths should
    // succeed if the MKDIR_P flag is set.
    for (const auto& kv : key_vals) {
        auto key = "intermediate." + kv.first;
        expect_subdoc_cmd(SubdocCmd(cmd, "dict", key, kv.second,
                                    SUBDOC_FLAG_MKDIR_P),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "dict",
                                    key),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, kv.second);
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
        expect_subdoc_cmd(SubdocCmd(cmd, "dict", kv.first, kv.second),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT, "");
    }

    // j). Check CAS support - cmd with correct CAS should succeed.
    // Get the current CAS.
    uint64_t cas = expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                               "dict", "int"),
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    uint64_t new_cas = expect_subdoc_cmd(SubdocCmd(cmd, "dict", "new_int", "3",
                                                   protocol_binary_subdoc_flag(0),
                                                   cas),
                                         PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    EXPECT_NE(cas, new_cas);

    // k). CAS - cmd with old cas should fail.
    expect_subdoc_cmd(SubdocCmd(cmd, "dict", "new_int2", "4",
                                protocol_binary_subdoc_flag(0), cas),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "");

    // l). CAS - manually corrupted (off by one) cas should fail.
    expect_subdoc_cmd(SubdocCmd(cmd, "dict","new_int2", "4",
                                protocol_binary_subdoc_flag(0), new_cas + 1),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "");

    delete_object("dict");

    // m). Attempt to perform dict command on array should fail.
    store_object("array", "[1,2]", /*JSON*/true, compress);
    expect_subdoc_cmd(SubdocCmd(cmd, "array","foo", "\"bar\""),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    delete_object("array");
}

TEST_P(McdTestappTest, SubdocDictAdd_SimpleRaw) {
    test_subdoc_dict_add_simple(/*compress*/false,
                                PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}

TEST_P(McdTestappTest, SubdocDictAdd_SimpleCompressed) {
    test_subdoc_dict_add_simple(/*compress*/true,
                                PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}

TEST_P(McdTestappTest, SubdocDictUpsert_SimpleRaw) {
    test_subdoc_dict_add_simple(/*compress*/false,
                                PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}

TEST_P(McdTestappTest, SubdocDictUpsert_SimpleCompressed) {
    test_subdoc_dict_add_simple(/*compress*/true,
                                PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}

void McdTestappTest::test_subdoc_dict_add_cas(bool compress,
                                              protocol_binary_command cmd) {
    ASSERT_TRUE((cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD) ||
                (cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT));

    // Store a simple JSON document to work on.
    store_object("dict", "{}", /*JSON*/true, compress);

    // a). Check that a CAS mismatch internally (between reading the JSON
    // (doc to operate on and storing it), is correctly retried.
    // (Note: the auto-retry only occurs when there is no CAS specified by the
    // user).

    // Configure the ewouldblock_engine to inject fake CAS failure for the
    // 3rd call (i.e. the 1st engine->store() attempt). We only expect 6 calls
    // total, so also make anything after that fail.
    ewouldblock_engine_configure(ENGINE_KEY_EEXISTS, EWBEngineMode_SEQUENCE,
                                 0xffffffc4 /* <3 MSBytes all-ones>, 0b11,000,100 */);

    // .. Yet a client request should succeed, as internal CAS failure should
    // be retried.
    uint64_t new_cas = expect_subdoc_cmd(SubdocCmd(cmd, "dict","new_int3", "3"),
                                         PROTOCOL_BINARY_RESPONSE_SUCCESS, "");

    // b). Check that if the user specifies an explicit CAS, then a mismatch
    // isn't retried and EEXISTS is returned back to the user.

    // Setup ewouldblock_engine - first two calls succeed, 3rd (engine->store)
    // fails. Do not expect more than 3 calls so make any further calls error.
    ewouldblock_engine_configure(ENGINE_KEY_EEXISTS, EWBEngineMode_SEQUENCE,
                                 0xfffffffc /* <3 MSBytes all-ones>, 0b11,111,100 */);

    expect_subdoc_cmd(SubdocCmd(cmd, "dict","new_int4", "4",
                                protocol_binary_subdoc_flag(0), new_cas),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "");

    // Cleanup.
    ewouldblock_engine_disable();
    delete_object("dict");
}

TEST_P(McdTestappTest, SubdocDictAdd_CasRaw) {
    test_subdoc_dict_add_cas(/*compress*/false,
                             PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}
TEST_P(McdTestappTest, SubdocDictAdd_CasCompressed) {
    test_subdoc_dict_add_cas(/*compress*/true,
                             PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}
TEST_P(McdTestappTest, SubdocDictUpsert_CasRaw) {
    test_subdoc_dict_add_cas(/*compress*/false,
                             PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}
TEST_P(McdTestappTest, SubdocDictUpsert_CasCompressed) {
    test_subdoc_dict_add_cas(/*compress*/true,
                             PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}


void test_subdoc_dict_add_upsert_deep(protocol_binary_command cmd) {

    ASSERT_TRUE((cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD) ||
                (cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT));

    // a). Check that we can add elements to a document at the maximum nested
    // level.
    unique_cJSON_ptr one_less_max_dict(
            make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS - 1));
    char* one_less_max_dict_str = cJSON_PrintUnformatted(one_less_max_dict.get());
    store_object("dict", one_less_max_dict_str);
    cJSON_Free(one_less_max_dict_str);

    std::string one_less_max_path(std::to_string(1));
    for (int depth = 2; depth < MAX_SUBDOC_PATH_COMPONENTS - 1; depth++) {
        one_less_max_path += std::string(".") + std::to_string(depth);
    }
    // Check precondition - should have an empty dict we can access.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "dict",
                                one_less_max_path),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "{}");

    // a). Check we can add primitive elements to this path.
    const std::vector<std::pair<std::string, std::string>> primitive_key_vals({
            {"int", "2"},
            {"float", "2.0"},
            {"true", "true"},
            {"false", "false"},
            {"null", "null"}});
    for (const auto& kv : primitive_key_vals) {
        const auto key = one_less_max_path + "." + kv.first;
        expect_subdoc_cmd(SubdocCmd(cmd, "dict", key, kv.second),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "dict",
                                    key),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, kv.second);
    }

    delete_object("dict");
}

TEST_P(McdTestappTest, SubdocDictAdd_Deep) {
    test_subdoc_dict_add_upsert_deep(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}

TEST_P(McdTestappTest, SubdocDictUpsert_Deep) {
    test_subdoc_dict_add_upsert_deep(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}

void test_subdoc_delete_simple(bool compress) {

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
    store_object("dict", dict, /*JSON*/true, compress);

    // Attempts to delete non-existent elements should fail.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "dict",
                                "bad_key"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");

    for (unsigned int ii = 0; ii < 8; ii++) {
        // Assert we can access it initially:
        std::string path(std::to_string(ii));
        uint64_t cas = expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                                   "dict", path),
                                         PROTOCOL_BINARY_RESPONSE_SUCCESS, "");

        // Deleting with the wrong CAS should fail:
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "dict",
                                    path, "", protocol_binary_subdoc_flag(0),
                                    cas + 1),
                          PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, "dict",
                                    path),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");

        // Should be able to delete with no CAS specified.
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "dict",
                                    path),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        // ... and should no longer exist:
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, "dict",
                                    path),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    }

    // After deleting everything the dictionary should be empty.
    validate_object("dict", "{}");
    delete_object("dict");
}

TEST_P(McdTestappTest, SubdocDelete_SimpleRaw) {
    test_subdoc_delete_simple(/*compress*/false);
}

TEST_P(McdTestappTest, SubdocDelete_SimpleCompressed) {
    test_subdoc_delete_simple(/*compress*/true);
}

TEST_P(McdTestappTest, SubdocDelete_Array) {

    // Create an array, then test deleting elements.
    store_object("a", "[0,1,2,3,4]", /*JSON*/true, /*compress*/false);

    // Sanity check - 3rd element should be 2
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[2]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");

    // a). Attempts to delete out of range elements should fail.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[5]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");

    // b). Test deleting at end of array.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[4]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    //     3rd element should still be 2; last element should now be 3.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[2]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[-1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "3");
    validate_object("a", "[0,1,2,3]");

    // c). Test deleting at start of array; elements are shuffled down.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    //     3rd element should now be 3; last element should still be 3.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[2]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "3");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[-1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "3");
    validate_object("a", "[1,2,3]");

    // d). Test deleting of last element using [-1].
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[-1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    //     Last element should now be 2.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[-1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");
    validate_object("a", "[1,2]");

    // e). Delete remaining elements.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("a", "[2]");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    // Should have an empty array.
    validate_object("a", "[]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocDelete_ArrayNested) {
    // Nested array containing different objects.
    store_object("b", "[0,[10,20,[100]],{\"key\":\"value\"}]",
                 /*JSON*/true, /*compress*/false);

    // Sanity check - 2nd element should be "[10,20,[100]]"
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "b", "[1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "[10,20,[100]]");

    // a). Delete nested array element
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "b", "[1][2][0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "b", "[1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "[10,20,[]]");

    // b). Delete the (now empty) nested array.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "b", "[1][2]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "b", "[1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "[10,20]");

    // c). Delete the next level up array.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "b", "[1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    // element [1] should now be the dict.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "b", "[1]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "{\"key\":\"value\"}");

    delete_object("b");
}

const std::vector<std::string> JSON_VALUES({
    "1.1",
    "\"value\"",
    "{\"inner\":\"dict\"}",
    "[1,2]",
    "true",
    "false",
    "null"});

TEST_P(McdTestappTest, SubdocReplace_SimpleDict)
{
    // Simple dictionary, replace first element with various types.
    store_object("a", "{\"key\":0,\"key2\":1}", /*JSON*/true, /*compress*/false);

    // Sanity check - 'key' should be "0"
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "key"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "0");

    // Replace the initial key with each primitive type:
    for (const auto& replace : JSON_VALUES) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a", "key",
                                    replace),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "key"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, replace);
    }
    // Sanity-check the final document
    validate_object("a", "{\"key\":null,\"key2\":1}");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocReplace_SimpleArray)
{
    // Simple array, replace first element with various types.
    store_object("a", "[0,1]", /*JSON*/true, /*compress*/false);

    // Sanity check - [0] should be "0"
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "0");

    // Replace the first element with each primitive type:
    for (const auto& replace : JSON_VALUES) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a", "[0]",
                                    replace),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, replace);
    }
    // Sanity-check the final document
    validate_object("a", "[null,1]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocReplace_ArrayDeep)
{
    // Test replacing in deeply nested arrays.

    // Create an array at one less than the maximum depth and an associated path.
    unique_cJSON_ptr one_less_max(make_nested_array(MAX_SUBDOC_PATH_COMPONENTS));
    char* one_less_max_str = cJSON_PrintUnformatted(one_less_max.get());
    store_object("a", one_less_max_str);
    cJSON_Free(one_less_max_str);

    std::string valid_max_path(make_nested_array_path(MAX_SUBDOC_PATH_COMPONENTS));
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a",
                                valid_max_path),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "[]");

    // a). Should be able to replace an element at the max depth.
    std::string new_value("\"deep\"");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a",
                                valid_max_path, new_value),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");

    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a",
                                valid_max_path),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, new_value);

    // b). But adding a nested array (taking the document over the maximum
    // depth) should fail.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a",
                                valid_max_path, "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_ETOODEEP, "");


    // c). Replace the whole deep array with a single toplevel element.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a",
                                "[0]", "[]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a",
                                "[0]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "[]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayPushLast_Simple)
{
    // a). Empty array, append to it.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a",
                                "", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "0");
    validate_object("a", "[0]");

    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a",
                                "", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[1]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "1");
    validate_object("a", "[0,1]");

    uint64_t cas = expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                               "a", "", "2"),
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[2]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");
    validate_object("a", "[0,1,2]");

    // b). Check that using the correct CAS succeeds.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "a", "", "3", protocol_binary_subdoc_flag(0), cas),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[3]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "3");
    validate_object("a", "[0,1,2,3]");

    // c). But using the wrong one fails.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "a", "", "4", protocol_binary_subdoc_flag(0), cas),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "");
    validate_object("a", "[0,1,2,3]");
    delete_object("a");

    // d). Check various other object types append successfully.
    store_object("b", "[]", /*JSON*/true, /*compress*/false);
    int index = 0;
    for (const auto& value : JSON_VALUES) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                    "b", "", value),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        std::string path("[" + std::to_string(index) + "]");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "b", path),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, value);
        index++;
    }
    delete_object("b");

    // e). Check we can append multiple values at once.
    store_object("c", "[]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "c", "", "0,1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("c", "[0,1]");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "c", "", "\"two\",3.141,{\"four\":4}"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("c", "[0,1,\"two\",3.141,{\"four\":4}]");

    delete_object("c");

    // f). Check MKDIR_P flag works.
    store_object("d", "{}", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "d", "foo", "0", SUBDOC_FLAG_MKDIR_P),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    delete_object("d");
}

TEST_P(McdTestappTest, SubdocArrayPushLast_Nested)
{
    // Operations on a nested array,
    // a). Begin with an empty nested array, append to it.
    store_object("a", "[[]]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a",
                                "", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "[]");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[1]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "1");
    validate_object("a", "[[],1]");

    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a",
                                "", "2"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[2]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");
    validate_object("a", "[[],1,2]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayPushFirst_Simple)
{
    // a). Empty array, prepend to it.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, "a",
                                "", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "0");
    validate_object("a", "[0]");

    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, "a",
                                "", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "1");
    validate_object("a", "[1,0]");

    uint64_t cas = expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                               "a", "", "2"),
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");
    validate_object("a", "[2,1,0]");

    // b). Check that using the correct CAS succeeds.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                "a", "", "3", protocol_binary_subdoc_flag(0), cas),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "3");
    validate_object("a", "[3,2,1,0]");

    // c). But using the wrong one fails.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                "a", "", "4", protocol_binary_subdoc_flag(0), cas),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "");
    validate_object("a", "[3,2,1,0]");
    delete_object("a");

    // d). Check various other object types prepend successfully.
    store_object("b", "[]", /*JSON*/true, /*compress*/false);
    for (const auto& value : JSON_VALUES) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                    "b", "", value),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "b", "[0]"),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, value);
    }
    delete_object("b");

    // e). Check we can prepend multiple values at once.
    store_object("c", "[]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                "c", "", "0,1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("c", "[0,1]");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                "c", "", "\"two\",3.141,{\"four\":4}"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("c", "[\"two\",3.141,{\"four\":4},0,1]");
    delete_object("c");

    // f). Check MKDIR_P flag works.
    store_object("d", "{}", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                "d", "foo", "0", SUBDOC_FLAG_MKDIR_P),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    delete_object("d");
}

TEST_P(McdTestappTest, SubdocArrayPushFirst_Nested)
{
    // Operations on a nested array.
    // a). Begin with an empty nested array, prepend to it.
    store_object("a", "[[]]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, "a",
                                "", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "1");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[1]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "[]");
    validate_object("a", "[1,[]]");

    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, "a",
                                "", "2"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "a", "[0]"),
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");
    validate_object("a", "[2,1,[]]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayAddUnique_Simple)
{
    // Start with an array with a single element.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);

    // a). Add an element which doesn't already exist.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                "a", "", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("a", "[0]");

    // b). Add an element which does already exist.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                "a", "", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS, "");
    validate_object("a", "[0]");
    delete_object("a");

    // c). Larger array, add an element which already exists.
    std::string array("[0,1,2,3,4,5,6,7,8,9]");
    store_object("b", array, /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                "b", "", "6"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS, "");
    validate_object("b", array.c_str());

    // d). Check that all permitted types of values can be added:
    const std::vector<std::string> valid_unique_values({
        "\"string\"",
        "10",
        "1.0",
        "true",
        "false",
        "null"});
    for (const auto& v : valid_unique_values) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                    "b", "", v),
                          PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }
    // ... and attempting to add a second time returns EEXISTS
    for (const auto& v : valid_unique_values) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                    "b", "", v),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS, "");
    }

#if 0 // TODO: According to the spec this shouldn't be permitted, however it
      // currently works...
    // f). Check it is not permitted to add non-primitive types (arrays, objects).
    const std::vector<std::string> invalid_unique_values({
        "{\"foo\": \"bar\"}",
        "[0,1,2]"});
    for (const auto& v : invalid_unique_values) {
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                    "b", "", v),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    }
#endif
    delete_object("b");

    // g). Attempts to add_unique to a array with non-primitive values should
    // fail.
    store_object("c", "[{\"a\":\"b\"}]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                "c", "", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    delete_object("c");

    store_object("d", "[[1,2]]", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                "d", "", "3"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    delete_object("d");
}

TEST_P(McdTestappTest, SubdocArrayInsert_Simple)
{
    // Start with an empty array.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);

    // a). Attempt to insert at position 0 should succeed.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[0]", "2"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("a", "[2]");

    // b). Second insert at zero should succeed and shuffle existing element
    // down.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[0]", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("a", "[0,2]");

    // c). Insert at position 1 should shuffle down elements after, leave alone
    // elements before.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[1]", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("a", "[0,1,2]");

    // d). Insert at len(array) should add to the end, without moving existing
    // elements.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[3]", "3"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    validate_object("a", "[0,1,2,3]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayInsert_Invalid)
{
    // Start with an empty array.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);

    // a). Attempt to insert past the end of the (empty) array should fail.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[1]", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    validate_object("a", "[]");

#if 0 // 2015-05-28 Incorrectly passes - need to report to MarkN
    // b). Insert at position '-1' is invalid.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[-1]", "3"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    validate_object("a", "[]");
#endif

    // c). MKDIR_P flag is not valid for ARRAY_INSERT
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[0]", "1", SUBDOC_FLAG_MKDIR_P),
                      PROTOCOL_BINARY_RESPONSE_EINVAL, "");
    validate_object("a", "[]");

    // d). A path larger than len(array) should fail.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[1]", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    validate_object("a", "[]");
    delete_object("a");

    // e). Attempt to insert to a dict should fail.
    store_object("b", "{}", /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "b", "[0]", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    validate_object("b", "{}");
    delete_object("b");
}

TEST_P(McdTestappTest, SubdocCounter_Simplet)
{
    store_object("a", "{}", /*JSON*/true, /*compress*/false);

    // a). Check that empty document, empty path creates a new element.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "1");
    auto result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":1}", result.second);

    // b). Check we can now increment it further.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "2");
    result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":2}", result.second);

    // c). Decrement by 2; should go back to zero.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "-2"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "0");
    result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":0}", result.second);

    // d). Decrement by 1; should go negative.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "-1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "-1");
    result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":-1}", result.second);

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocCounter_InvalidNotInt)
{
    // Cannot increment things which are not integers.
    const std::vector<std::string> not_integer({
        "true",
        "false",
        "null",
        "\"string\"",
        "[0]",
        "{\"foo\": \"bar\"}",
        "1.1"
    });
    for (auto& val : not_integer) {
        const std::string doc("{\"key\":" + val + "}");
        store_object("a", doc, /*JSON*/true, /*compress*/false);
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                    "a", "key", "1"),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
        auto result = fetch_value("a");
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
        EXPECT_EQ(doc, result.second);
        delete_object("a");
    }
}

TEST_P(McdTestappTest, SubdocCounter_InvalidERange)
{
    // Cannot increment things which are not representable as int64_t.
    const auto int64_max = std::numeric_limits<int64_t>::max();

    const std::vector<std::string> unrepresentable({
        std::to_string(uint64_t(int64_max) + 1),
        "-" + std::to_string(uint64_t(int64_max) + 2),
    });
    for (auto& val : unrepresentable) {
        const std::string doc("{\"key\":" + val + "}");
        store_object("b", doc, /*JSON*/true, /*compress*/false);
        expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                    "b", "key", "1"),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_NUM_ERANGE, "");
        auto result = fetch_value("b");
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
        EXPECT_EQ(doc, result.second);
        delete_object("b");
    }
}

TEST_P(McdTestappTest, SubdocCounter_Limits)
{
    // a). Attempting to increment value one less than int64_t::MAX by one
    //     should succeed.
    const int64_t max = std::numeric_limits<int64_t>::max();

    store_object("a", "{\"key\":" + std::to_string(max - 1) + "}",
                 /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, std::to_string(max));

    auto result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":" + std::to_string(max) + "}", result.second);

    // b). A further increment by one should fail.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DELTA_ERANGE, "");

    delete_object("a");

    // c). Same with int64_t::min() and decrement.
    const int64_t min = std::numeric_limits<int64_t>::min();

    store_object("b", "{\"key\":" + std::to_string(min + 1) + "}",
                 /*JSON*/true, /*compress*/false);
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "b", "key", "-1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, std::to_string(min));

    result = fetch_value("b");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":" + std::to_string(min) + "}", result.second);

    // b). A further decrement by one should fail.
    expect_subdoc_cmd(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "b", "key", "-1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DELTA_ERANGE, "");

    delete_object("b");
}

// Tests how a single worker handles multiple "concurrent" connections
// performing operations.
class WorkerConcurrencyTest : public TestappTest {
public:
    static void SetUpTestCase() {
        memcached_cfg.reset(generate_config(/*disable SSL*/0));
        // Change the number of worker threads to one so we guarantee that
        // multiple connections are handled by a single worker.
        cJSON_AddNumberToObject(memcached_cfg.get(), "threads", 1);
        start_memcached_server(memcached_cfg.get());

        if (HasFailure()) {
            server_pid = reinterpret_cast<pid_t>(-1);
        } else {
            CreateTestBucket();
        }
    }

    static unique_cJSON_ptr memcached_cfg;
};

unique_cJSON_ptr WorkerConcurrencyTest::memcached_cfg;


TEST_F(WorkerConcurrencyTest, SubdocArrayPushLast_Concurrent) {
    // Concurrently add to two different array documents, using two connections.

    // Setup the initial empty objects.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);
    store_object("b", "[]", /*JSON*/true, /*compress*/false);

    // Create an additional second connection to memcached.
    SOCKET* current_sock = &sock;
    SOCKET sock1 = *current_sock;
    SOCKET sock2 = connect_to_server_plain(port, false);
    ASSERT_NE(sock2, INVALID_SOCKET);
    sock = sock1;

    const size_t push_count = 100;

    char send_buf[1024 * 10];
    char* ptr = send_buf;
    ssize_t len = -1;

    // Build pipeline for the even commands.
    std::string expected_a;
    for (unsigned int i = 0; i < push_count; i += 2) {
        expected_a += std::to_string(i) + ",";

        encode_subdoc_command(ptr, sizeof(send_buf) - (ptr - send_buf),
                SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                        "a", "", std::to_string(i)),
                len);
        ASSERT_NE(len, -1);
        ptr += len;
    }
    *current_sock = sock1;
    safe_send(send_buf, ptr - send_buf, false);

    // .. and the odd commands.
    ptr = send_buf;
    std::string expected_b;
    for (unsigned int i = 1; i < push_count; i += 2) {
        expected_b += std::to_string(i) + ",";

        encode_subdoc_command(ptr, sizeof(send_buf) - (ptr - send_buf),
                SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                        "b", "", std::to_string(i)),
                len);
        ASSERT_NE(len, -1);
        ptr += len;
    }
    *current_sock = sock2;
    safe_send(send_buf, ptr - send_buf, false);

    // Fixup the expected values - remove the trailing comma and bookend with
    // [ ].
    expected_a.insert(0, "[");
    expected_a.replace(expected_a.size() - 1, 1, "]");
    expected_b.insert(0, "[");
    expected_b.replace(expected_b.size() - 1, 1, "]");

    // Consume all the responses we should be expecting back.
    for (unsigned int i = 0; i < push_count; i++) {
        sock = (i % 2) ? sock1 : sock2;
        recv_subdoc_response(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                PROTOCOL_BINARY_RESPONSE_SUCCESS, "");
    }

    // Validate correct data was written.
    validate_object("a", expected_a);
    validate_object("b", expected_b);

    // Restore original socket; free second one.
    *current_sock = sock1;
    closesocket(sock2);

    delete_object("a");
    delete_object("b");
}
