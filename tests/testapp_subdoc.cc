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
#include <memory>
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


// helper class for use with std::unique_ptr in managing cJSON* objects.
struct cJSONDeleter {
  void operator()(cJSON* j) { cJSON_Delete(j); }
};

typedef std::unique_ptr<cJSON, cJSONDeleter> unique_cJSON_ptr;

// Checks that a and b are equal; if not then assert.
#define check_equal(a, b) check_equal_impl((a), (b), #a, #b, __FILE__, __LINE__)
// Checks that a and b are not equal; asserts if not the case.
#define check_ne(a, b) check_not_equal_impl((a), (b), #a, #b, __FILE__, __LINE__)

// Overload of check_equal_impl for std::string
static void check_equal_impl(const std::string& a_value, const std::string& b_value,
                             const char* a_name, const char* b_name,
                             const char* file, int line) {
    if (a_value != b_value) {
        fprintf(stderr, "Check '%s == %s' failed - '%s == %s' at %s:%d\n",
                a_name, b_name, a_value.c_str(), b_value.c_str(), file, line);
        abort();
    }
}

// Overload of check_not_equal_impl for uint64_t
static void check_not_equal_impl(uint64_t a_value, uint64_t b_value,
                             const char* a_name, const char* b_name,
                             const char* file, int line) {
    if (a_value == b_value) {
        fprintf(stderr, "Check '%s != %s' failed - '%" PRId64 " == %" PRId64 "' at %s:%d\n",
                a_name, b_name, a_value, b_value, file, line);
        abort();
    }
}

// Class representing a subdoc command; to assist in constructing / encoding one.
struct SubdocCmd {
    // Always need at least a cmd, key and path.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_)
      : cmd(cmd_), key(key_), path(path_), value(""), flags(), cas() {}

    // Constructor including a value.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_)
      : cmd(cmd_), key(key_), path(path_), value(value_), flags(), cas() {}

    // Constructor additionally including flags.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_,
                       protocol_binary_subdoc_flag flags_)
      : cmd(cmd_), key(key_), path(path_), value(value_), flags(flags_),
        cas() {}

    // Constructor additionally including CAS.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_,
                       protocol_binary_subdoc_flag flags_, uint64_t cas_)
      : cmd(cmd_), key(key_), path(path_), value(value_), flags(flags_),
        cas(cas_) {}

    protocol_binary_command cmd;
    std::string key;
    std::string path;
    std::string value;
    protocol_binary_subdoc_flag flags;
    uint64_t cas;
};

/* Encode the specified subdoc command into `buf`.
 *
 * @return the size of the encoded data.
 */
static off_t encode_subdoc_command(char* buf, size_t bufsz,
                                   const SubdocCmd& cmd) {
    protocol_binary_request_subdocument* request = (protocol_binary_request_subdocument*)buf;
    cb_assert(bufsz >= (sizeof(*request) + cmd.key.size() + cmd.path.size()
                        + cmd.value.size()));

    // Always need a key.
    cb_assert(cmd.key.empty() == false);

    // Always need a path.
    cb_assert(cmd.path.empty() == false);

    // path is encoded in extras as a uint16_t.
    cb_assert(cmd.path.size() < std::numeric_limits<uint16_t>::max());

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
    return encoded_bytes;
}

/* Encodes and sends a sub-document command with the given parameters, receives
 * the response and validates that the status matches the expected one.
 * If expected_value is non-empty, also verifies that the response value equals
 * expectd_value.
 * @return CAS value (if applicable for the command, else zero).
 */
static uint64_t expect_subdoc_cmd(const SubdocCmd& cmd,
                                  protocol_binary_response_status expected_status,
                                  const std::string& expected_value) {
    union {
        protocol_binary_request_subdocument request;
        char bytes[2048];
    } send;
    union {
        protocol_binary_response_subdocument response;
        char bytes[1024];
    } receive;

    size_t len = encode_subdoc_command(send.bytes, sizeof(send.bytes),
                                       cmd);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));

    validate_response_header((protocol_binary_response_no_extras*)&receive.response,
                             cmd.cmd, expected_status);

    // TODO: Check extras for subdoc command and mutation / seqno (if enabled).

    const protocol_binary_response_header* header = &receive.response.message.header;
    const char* val_ptr = receive.bytes + sizeof(*header) +
                          header->response.extlen;
    const size_t vallen = header->response.bodylen + header->response.extlen;

    if (!expected_value.empty() &&
        (cmd.cmd != PROTOCOL_BINARY_CMD_SUBDOC_EXISTS)) {
        const std::string val(val_ptr, val_ptr + vallen);
        check_equal(val, expected_value);
    } else {
        // Expect zero length on success (on error the error message string is
        // returned).
        if (header->response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            cb_assert(vallen == 0);
        }
    }
    return header->response.cas;
}

/* Stores the given document.
 * @param key Document key
 * @param value Document value
 * @param json If true store as JSON, if false as binary
 * @param compress If true compress the provided value before storing.
 */
static void store_object(const std::string& key, const std::string& value,
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
static enum test_return test_subdoc_get_binary(bool compress,
                                               protocol_binary_command cmd) {
    const char not_JSON[] = "not; json";
    store_object("binary", not_JSON);

    // a). Check that access fails with DOC_NOTJSON
    expect_subdoc_cmd(SubdocCmd(cmd, "binary", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON, "");

    delete_object("binary");

    return TEST_PASS;
}

enum test_return test_subdoc_get_binary_raw() {
    return test_subdoc_get_binary(/*compress*/false,
                                  PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
enum test_return test_subdoc_get_binary_compressed() {
    return test_subdoc_get_binary(/*compress*/true,
                                  PROTOCOL_BINARY_CMD_SUBDOC_GET);
}

enum test_return test_subdoc_exists_binary_raw() {
    return test_subdoc_get_binary(/*compress*/false,
                                  PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
enum test_return test_subdoc_exists_binary_compressed() {
    return test_subdoc_get_binary(/*compress*/true,
                                  PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// retrieve from a JSON document consisting of a toplevel array.
static enum test_return
test_subdoc_fetch_array_simple(bool compressed, protocol_binary_command cmd) {

    cb_assert((cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
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
    SubdocCmd bad_flags(cmd, "array", "[0]");
    bad_flags.flags = SUBDOC_FLAG_MKDIR_P;
    expect_subdoc_cmd(bad_flags, PROTOCOL_BINARY_RESPONSE_EINVAL, "");

    delete_object("array");

    return TEST_PASS;
}

enum test_return test_subdoc_get_array_simple_raw() {
    return test_subdoc_fetch_array_simple(/*compressed*/false,
                                          PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
enum test_return test_subdoc_get_array_simple_compressed() {
    return test_subdoc_fetch_array_simple(/*compressed*/true,
                                          PROTOCOL_BINARY_CMD_SUBDOC_GET);
}

enum test_return test_subdoc_exists_array_simple_raw() {
    return test_subdoc_fetch_array_simple(/*compressed*/false,
                                         PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
enum test_return test_subdoc_exists_array_simple_compressed() {
    return test_subdoc_fetch_array_simple(/*compressed*/true,
                                         PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// JSON document containing toplevel dict.
static enum test_return test_subdoc_fetch_dict_simple(bool compressed,
                                                      protocol_binary_command cmd) {

    cb_assert((cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
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

    return TEST_PASS;
}

enum test_return test_subdoc_get_dict_simple_raw() {
    return test_subdoc_fetch_dict_simple(/*compressed*/false,
                                       PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
enum test_return test_subdoc_get_dict_simple_compressed() {
    return test_subdoc_fetch_dict_simple(/*compressed*/true,
                                       PROTOCOL_BINARY_CMD_SUBDOC_GET);
}

enum test_return test_subdoc_exists_dict_simple_raw() {
    return test_subdoc_fetch_dict_simple(/*compressed*/false,
                                       PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
enum test_return test_subdoc_exists_dict_simple_compressed() {
    return test_subdoc_fetch_dict_simple(/*compressed*/true,
                                       PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

// JSON document containing nested dictionary.
static enum test_return test_subdoc_fetch_dict_nested(bool compressed,
                                                      protocol_binary_command cmd) {

    cb_assert((cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
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

    return TEST_PASS;
}

enum test_return test_subdoc_get_dict_nested_raw() {
    return test_subdoc_fetch_dict_nested(/*compressed*/false,
                                       PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
enum test_return test_subdoc_get_dict_nested_compressed() {
    return test_subdoc_fetch_dict_nested(/*compressed*/true,
                                       PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
enum test_return test_subdoc_exists_dict_nested_raw() {
    return test_subdoc_fetch_dict_nested(/*compressed*/false,
                                       PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}
enum test_return test_subdoc_exists_dict_nested_compressed() {
    return test_subdoc_fetch_dict_nested(/*compressed*/true,
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
static enum test_return
test_subdoc_fetch_dict_deep(protocol_binary_command cmd) {

    // a). Should be able to access a deeply nested document as long as the
    // path we ask for is no longer than MAX_SUBDOC_PATH_COMPONENTS.
    unique_cJSON_ptr max_dict(make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS));
    char* max_dict_str = cJSON_PrintUnformatted(max_dict.get());
    cb_assert(store_object("max_dict", max_dict_str) == TEST_PASS);
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
    cb_assert(store_object("too_deep_dict", too_deep_dict_str) == TEST_PASS);
    cJSON_Free(too_deep_dict_str);

    std::string too_long_path(std::to_string(1));
    for (int depth = 2; depth < MAX_SUBDOC_PATH_COMPONENTS + 1; depth++) {
        too_long_path += std::string(".") + std::to_string(depth);
    }
    expect_subdoc_cmd(SubdocCmd(cmd, "too_deep_dict", too_long_path),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG, "");

    delete_object("too_deep_dict");

    return TEST_PASS;
}

enum test_return test_subdoc_get_dict_deep() {
    return test_subdoc_fetch_dict_deep(PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
enum test_return test_subdoc_exists_dict_deep() {
    return test_subdoc_fetch_dict_deep(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
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

// Deeply nested JSON array; verify limits on how deep documents can be.
static enum test_return
test_subdoc_fetch_array_deep(protocol_binary_command cmd) {

    // a). Should be able to access a deeply nested document as long as the
    // path we ask for is no longer than MAX_SUBDOC_PATH_COMPONENTS.

    unique_cJSON_ptr max_array(make_nested_array(MAX_SUBDOC_PATH_COMPONENTS));
    char* max_array_str = cJSON_PrintUnformatted(max_array.get());
    cb_assert(store_object("max_array", max_array_str) == TEST_PASS);
    cJSON_Free(max_array_str);

    std::string valid_max_path;
    for (int depth = 1; depth < MAX_SUBDOC_PATH_COMPONENTS; depth++) {
        valid_max_path += "[0]";
    }
    expect_subdoc_cmd(SubdocCmd(cmd, "max_array", valid_max_path),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "[]");
    delete_object("max_array");

    // b). Accessing a deeper array should fail.
    unique_cJSON_ptr too_deep_array(make_nested_array(MAX_SUBDOC_PATH_COMPONENTS + 1));
    char* too_deep_array_str = cJSON_PrintUnformatted(too_deep_array.get());
    cb_assert(store_object("too_deep_array", too_deep_array_str) == TEST_PASS);
    cJSON_Free(too_deep_array_str);

    std::string too_long_path;
    for (int depth = 1; depth < MAX_SUBDOC_PATH_COMPONENTS + 1; depth++) {
        too_long_path += "[0]";
    }
    expect_subdoc_cmd(SubdocCmd(cmd, "too_deep_array", too_long_path),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG, "");
    delete_object("too_deep_array");

    return TEST_PASS;
}

enum test_return test_subdoc_get_array_deep() {
    return test_subdoc_fetch_array_deep(PROTOCOL_BINARY_CMD_SUBDOC_GET);
}
enum test_return test_subdoc_exists_array_deep() {
    return test_subdoc_fetch_array_deep(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS);
}

/* Test adding to a JSON dictionary.
 * @param compress If true operate on compressed JSON documents.
 * @param cmd The binary protocol command to test. Permitted values are:
 *            - PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD
 *            - PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT
 */
static enum test_return test_subdoc_dict_add_simple(bool compress,
                                                    protocol_binary_command cmd) {
    cb_assert((cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD) ||
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
    check_ne(cas, new_cas);

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

    return TEST_PASS;
}

enum test_return test_subdoc_dict_add_simple_raw() {
    return test_subdoc_dict_add_simple(/*compress*/false,
                                       PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}

enum test_return test_subdoc_dict_add_simple_compressed() {
    return test_subdoc_dict_add_simple(/*compress*/true,
                                       PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}

enum test_return test_subdoc_dict_upsert_simple_raw() {
    return test_subdoc_dict_add_simple(/*compress*/false,
                                       PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}

enum test_return test_subdoc_dict_upsert_simple_compressed() {
    return test_subdoc_dict_add_simple(/*compress*/true,
                                       PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}

static enum test_return test_subdoc_dict_add_upsert_deep(protocol_binary_command cmd) {

    cb_assert((cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD) ||
              (cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT));

    // a). Check that we can add elements to a document at the maximum nested
    // level.
    unique_cJSON_ptr one_less_max_dict(
            make_nested_dict(MAX_SUBDOC_PATH_COMPONENTS - 1));
    char* one_less_max_dict_str = cJSON_PrintUnformatted(one_less_max_dict.get());
    cb_assert(store_object("dict", one_less_max_dict_str) == TEST_PASS);
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

    return TEST_PASS;
}

enum test_return test_subdoc_dict_add_deep() {
    return test_subdoc_dict_add_upsert_deep(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
}

enum test_return test_subdoc_dict_upsert_deep() {
    return test_subdoc_dict_add_upsert_deep(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT);
}

enum test_return test_subdoc_delete_simple(bool compress) {

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

    return TEST_PASS;
}

enum test_return test_subdoc_delete_simple_raw() {
    return test_subdoc_delete_simple(/*compress*/false);
}

enum test_return test_subdoc_delete_simple_compressed() {
    return test_subdoc_delete_simple(/*compress*/true);
}

enum test_return test_subdoc_delete_array() {

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

    return TEST_PASS;
}

enum test_return test_subdoc_delete_array_nested() {
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

    return TEST_PASS;
}
