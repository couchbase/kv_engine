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
 * testapp testcases for sub-document API - single path.
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

void SubdocCmd::encode(std::vector<char>& buf) const
{
    ASSERT_FALSE(key.empty()) << "Need a key";
    ASSERT_LT(path.size(), std::numeric_limits<uint16_t>::max()) << "Path too long";

    protocol_binary_request_subdocument request;

    // Expiry (optional) is encoded in extras. Only include if non-zero or
    // if explicit encoding of zero was requested.
    bool include_expiry = (expiry != 0 || has_expiry);
    size_t encoded_expiry_len = include_expiry ? sizeof(uint32_t) : 0;

    memset(&request, 0, sizeof(request));

    // Populate the header.
    protocol_binary_request_header* const header = &request.message.header;
    header->request.magic = PROTOCOL_BINARY_REQ;
    header->request.opcode = cmd;
    header->request.keylen = htons(key.size());
    header->request.extlen = sizeof(uint16_t) + sizeof(uint8_t) +
                             encoded_expiry_len;
    header->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    header->request.vbucket = 0;
    header->request.bodylen = htonl(header->request.extlen + key.size() +
                                    path.size() + value.size());
    header->request.opaque = 0xdeadbeef;
    header->request.cas = cas;

    // Add extras: pathlen, flags, optional expiry
    request.message.extras.pathlen = htons(path.size());
    request.message.extras.subdoc_flags = flags;
    buf.insert(buf.end(), request.bytes, &request.bytes[0] + sizeof request.bytes);

    if (include_expiry) {
        // As expiry is optional (and immediately follows subdoc_flags,
        // i.e. unaligned) there's no field in the struct; so use low-level
        // memcpy to populate it.
        uint32_t encoded_expiry = htonl(expiry);
        char *expbuf = reinterpret_cast<char*>(&encoded_expiry);
        buf.insert(buf.end(), expbuf, expbuf + sizeof encoded_expiry);
    }

    // Add Body: key; path; value if applicable.
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), path.begin(), path.end());
    buf.insert(buf.end(), value.begin(), value.end());
}

/* Encodes and sends a sub-document command, without waiting for any response.
 */
void send_subdoc_cmd(const SubdocCmd& cmd) {
    std::vector<char> buf;
    cmd.encode(buf);
    safe_send(buf.data(), buf.size(), false);
}

static void recv_subdoc_response(const SubdocCmd& cmd,
                                 protocol_binary_response_status err,
                                 SubdocSingleResponse& resp)
{
    resp.clear();
    std::vector<char> buf;
    buf.resize(1024);
    if (!safe_recv_packet(buf.data(), buf.size())) {
        ADD_FAILURE() << "Failed to recv subdoc response";
        return;
    }

    protocol_binary_response_no_extras tempResponse;
    memcpy(&tempResponse, buf.data(), sizeof tempResponse);
    mcbp_validate_response_header(&tempResponse, cmd.getOp(), err);

    // safe_recv_packet does ntohl already!
    buf.resize(sizeof (protocol_binary_response_header) +
               tempResponse.message.header.response.bodylen);
    resp.assign(std::move(buf));
}

uint64_t recv_subdoc_response(protocol_binary_command expected_cmd,
                              protocol_binary_response_status expected_status,
                              const std::string& expected_value) {
    union {
        protocol_binary_response_subdocument response;
        char bytes[1024];
    } receive;

    if (!safe_recv_packet(receive.bytes, sizeof(receive.bytes))) {
        ADD_FAILURE() << "Failed to recv subdoc response";
        return -1;
    }

    mcbp_validate_response_header(
        (protocol_binary_response_no_extras*)&receive.response,
        expected_cmd, expected_status);

    const protocol_binary_response_header* header = &receive.response.message.header;

    const char* val_ptr = receive.bytes + sizeof(*header) +
                          header->response.extlen;
    const size_t vallen = header->response.bodylen - header->response.extlen;

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

// Overload for multi-lookup responses
uint64_t recv_subdoc_response(protocol_binary_command expected_cmd,
                              protocol_binary_response_status expected_status,
                              const std::vector<SubdocMultiLookupResult>& expected_results) {
    union {
        protocol_binary_response_subdocument response;
        char bytes[1024];
    } receive;

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));

    mcbp_validate_response_header(
        (protocol_binary_response_no_extras*)&receive.response,
        expected_cmd, expected_status);

    // Decode body and check against expected_results
    const auto& header = receive.response.message.header;
    const char* val_ptr = receive.bytes + sizeof(header) +
                          header.response.extlen;
    const size_t vallen = header.response.bodylen + header.response.extlen;

    size_t offset = 0;
    for (unsigned int ii = 0; ii < expected_results.size(); ii++) {
        const size_t result_header_len = sizeof(uint16_t) + sizeof(uint32_t);
        if (offset + result_header_len > vallen) {
            ADD_FAILURE() << "Remaining value length too short for expected result header";
            return -1;
        }

        const auto& exp_result = expected_results[ii];
        const char* result_header = val_ptr + offset;
        uint16_t status = ntohs(*reinterpret_cast<const uint16_t*>(result_header));
        EXPECT_EQ(exp_result.first, protocol_binary_response_status(status))
            << "Lookup result[" << ii << "]: status different";

        uint32_t result_len = ntohl(*reinterpret_cast<const uint32_t*>
                (result_header + sizeof(uint16_t)));
        EXPECT_EQ(exp_result.second.size(), result_len)
            << "Lookup result[" << ii << "]: length different";

        if (offset + result_header_len + result_len > vallen) {
            ADD_FAILURE() << "Remaining value length too short for expected result value";
            return -1;
        }

        std::string result_value(result_header + result_header_len, result_len);
        EXPECT_EQ(exp_result.second, result_value)
            << "Lookup result[" << ii << "]: value differs";

        offset += result_header_len + result_len;
    }

    return header.response.cas;
}

// Allow GTest to print out std::vectors as part of EXPECT/ ASSERT error
// messages.
namespace std {
template <typename T>
std::ostream& operator<< (std::ostream& os, const std::vector<T>& v)
{
    os << '[';
    for (auto& e : v) {
        os << " " << e;
    }
    os << ']';
    return os;
}

// Specialization for uint8_t to print as hex.
template <>
std::ostream& operator<< (std::ostream& os, const std::vector<uint8_t>& v)
{
    os << '[' << std::hex;
    for (auto& e : v) {
        os << " " << std::setw(2) << std::setfill('0') << (e & 0xff);
    }
    os << ']';
    return os;
}
} // namespace std;

// Overload for multi-mutation responses
uint64_t recv_subdoc_response(protocol_binary_command expected_cmd,
                              protocol_binary_response_status expected_status,
                              const std::vector<SubdocMultiMutationResult>& expected_results) {
    union {
        protocol_binary_response_subdocument response;
        char bytes[1024];
    } receive;

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));

    mcbp_validate_response_header(
        (protocol_binary_response_no_extras*)&receive.response,
        expected_cmd, expected_status);

    // TODO: Check extras for subdoc command and mutation / seqno (if enabled).

    // Decode body and check against expected_results
    const auto& header = receive.response.message.header;
    const char* val_ptr = receive.bytes + sizeof(header) +
                          header.response.extlen;
    const size_t vallen = header.response.bodylen - header.response.extlen;
    std::string value(val_ptr, val_ptr + vallen);

    if (expected_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        if (enabled_hello_features.count(PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO) > 0) {
            EXPECT_EQ(16, header.response.extlen);
        } else {
            EXPECT_EQ(0u, header.response.extlen);
        }

        for (const auto& result : expected_results) {
            // Should always have at least 7 bytes in result -
            // index, status, resultlen.
            EXPECT_GE(value.size(),
                      sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint32_t));

            // Extract fields from result spec and validate.
            uint8_t actual_index = *reinterpret_cast<uint8_t*>(&value[0]);
            value.erase(value.begin());
            EXPECT_EQ(result.index, actual_index);

            uint16_t actual_status = ntohs(*reinterpret_cast<uint16_t*>(&value[0]));
            value.erase(value.begin(), value.begin() + 2);
            EXPECT_EQ(result.status, actual_status);

            uint32_t actual_resultlen = ntohl(*reinterpret_cast<uint32_t*>(&value[0]));
            value.erase(value.begin(), value.begin() + 4);
            EXPECT_EQ(result.result.size(), actual_resultlen);

            std::string actual_result = value.substr(0, actual_resultlen);
            value.erase(value.begin(), value.begin() + actual_resultlen);
            EXPECT_EQ(result.result, actual_result);
        }
        // Should have consumed all of the value.
        EXPECT_EQ(0u, value.size());

    } else if (expected_status == PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE) {
        // Specific path failed - should have a 3-byte body containing
        // specific status and index of first failing spec.
        EXPECT_EQ(3, vallen) << "Incorrect value:'" << std::string(val_ptr, vallen) << '"';
        uint8_t actual_fail_index = *val_ptr;
        uint16_t actual_fail_spec_status =
                ntohs(*reinterpret_cast<const uint16_t*>(val_ptr + sizeof(actual_fail_index)));
        EXPECT_EQ(1, expected_results.size());
        EXPECT_EQ(expected_results[0].index, actual_fail_index);
        EXPECT_EQ(expected_results[0].status, actual_fail_spec_status);
    } else {
        // Top-level error - should have zero body.
        EXPECT_EQ(0u, vallen);
    }

    return header.response.cas;
}

::testing::AssertionResult subdoc_verify_cmd(const SubdocCmd& cmd,
                                             protocol_binary_response_status err,
                                             const std::string& value,
                                             SubdocSingleResponse& resp)
{
    using ::testing::AssertionSuccess;
    using ::testing::AssertionFailure;

    send_subdoc_cmd(cmd);
    recv_subdoc_response(cmd, err, resp);
    if (::testing::Test::HasFailure()) {
        return AssertionFailure();
    }

    if (!value.empty() && cmd.getOp() != PROTOCOL_BINARY_CMD_SUBDOC_EXISTS) {
        if (value != resp.getValue()) {
            return AssertionFailure()
                    << "Value mismatch for " << cmd << std::endl
                    << "  Expected: " << value << std::endl
                    << "  Got: " << resp.getValue() << std::endl;
        }
    }
    return AssertionSuccess();
}

// Overload for multi-lookup commands.
uint64_t expect_subdoc_cmd(const SubdocMultiLookupCmd& cmd,
                           protocol_binary_response_status expected_status,
                           const std::vector<SubdocMultiLookupResult>& expected_results) {
    std::vector<char> payload = cmd.encode();
    safe_send(payload.data(), payload.size(), false);

    return recv_subdoc_response(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
                                expected_status, expected_results);
}

// Overload for multi-mutation commands.
uint64_t expect_subdoc_cmd(const SubdocMultiMutationCmd& cmd,
                           protocol_binary_response_status expected_status,
                           const std::vector<SubdocMultiMutationResult>& expected_results) {
    std::vector<char> payload = cmd.encode();
    safe_send(payload.data(), payload.size(), false);

    return recv_subdoc_response(cmd.command, expected_status,
                                expected_results);
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

// Ensure the execution of the operation is successful
#define EXPECT_SD_OK(cmd) EXPECT_PRED_FORMAT1(subdoc_pred_ok, cmd)
#define ASSERT_SD_OK(cmd) ASSERT_PRED_FORMAT1(subdoc_pred_ok, cmd)

// Ensure the execution of the operation returns an error
#define EXPECT_SD_ERR(cmd, err) EXPECT_PRED_FORMAT2(subdoc_pred_errcode, cmd, err)

// Ensure the returned value is equal to val
#define EXPECT_SD_VALEQ(cmd, val) EXPECT_PRED_FORMAT2(subdoc_pred_value, cmd, val)
#define ASSERT_SD_VALEQ(cmd, val) ASSERT_PRED_FORMAT2(subdoc_pred_value, cmd, val)

// Ensure the path p in the document k is equal to v
#define EXPECT_SD_GET(k, p, v) EXPECT_SD_VALEQ(\
    SubdocCmd().setOp(PROTOCOL_BINARY_CMD_SUBDOC_GET).setKey(k).setPath(p), v)

// Ensure that the given error AND value are returned
#define EXPECT_SUBDOC_CMD(cmd, err, val) EXPECT_PRED_FORMAT3(subdoc_pred_compat, cmd, err, val)

// Ensure the given error and value are returned. resp is used as an 'out'
// value.
#define EXPECT_SUBDOC_CMD_RESP(cmd, err, val, resp) EXPECT_PRED_FORMAT4(subdoc_pred_full, cmd, err, val, resp)

// Non JSON document, optionally compressed. Subdoc commands should fail.
void test_subdoc_get_binary(bool compress, protocol_binary_command cmd) {
    const char not_JSON[] = "not; json";
    store_object("binary", not_JSON);

    // a). Check that access fails with DOC_NOTJSON
    EXPECT_SD_ERR(SubdocCmd(cmd, "binary", "[0]"), PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON);

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
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "array", "[0]"), "0");
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "array", "[1]"), "\"one\"");
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "array", "[2]"), "2.0");

    // b). Check successful access to last element (using -1).
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "array", "[-1]"), "2.0");

    // c). Check -2 treated as invalid index (only -1 permitted).
    EXPECT_SD_ERR(SubdocCmd(cmd, "array", "[-2]"),
        PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL);
    reconnect_to_server();

    // d). Check failure accessing out-of-range index.
    EXPECT_SD_ERR(SubdocCmd(cmd, "array", "[3]"),
        PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);
    EXPECT_SD_ERR(SubdocCmd(cmd, "array", "[9999]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);

    // e). Check failure accessing array as dict.
    EXPECT_SD_ERR(SubdocCmd(cmd, "array", "missing_key"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
    EXPECT_SD_ERR(SubdocCmd(cmd, "array", "[2].nothing_here"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);

    // f). Check path longer than SUBDOC_PATH_MAX_LENGTH is invalid.
    std::string too_long_path(1024 + 1, '.');
    EXPECT_SD_ERR(SubdocCmd(cmd, "array", too_long_path),
                  PROTOCOL_BINARY_RESPONSE_EINVAL);
    reconnect_to_server();

    // g). Check that incorrect flags (i.e. non-zero) is invalid.
    EXPECT_SD_ERR(SubdocCmd(cmd, "array", "[0]", "", SUBDOC_FLAG_MKDIR_P),
                  PROTOCOL_BINARY_RESPONSE_EINVAL);
    reconnect_to_server();

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
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict", "int"), "1");
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict", "string"), "\"two\"");
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict", "true"), "true");
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict", "false"), "false");

    // b). Check failure accessing non-existent keys.
    EXPECT_SD_ERR(SubdocCmd(cmd, "dict", "missing_key"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);

    // c). Check failure accessing object incorrectly (wrong type).
    EXPECT_SD_ERR(SubdocCmd(cmd, "dict", "[0]"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
    EXPECT_SD_ERR(SubdocCmd(cmd, "dict", "[-1]"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
    EXPECT_SD_ERR(SubdocCmd(cmd, "dict", "int.nothing_here"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);

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
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict2", "name.title"), "\"Mr\"");
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict2", "name.first"), "\"Joseph\"");
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict2", "name.last"), "\"Bloggs\"");

    // b). Check successful access to a whole sub-dictionary.
    char* name_str = cJSON_PrintUnformatted(name);
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict2", "name"), name_str);
    cJSON_Free(name_str);

    // c). Check successful access to a whole sub-array.
    char* orders_str = cJSON_PrintUnformatted(orders);
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict2", "orders"), orders_str);
    cJSON_Free(orders_str);

    // d). Check access to dict in array.
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "dict2", "orders[0].date"),
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
    cJSON* parent = nullptr;
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
    EXPECT_SD_VALEQ(SubdocCmd(cmd, "max_dict", valid_max_path), "{}");

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
    EXPECT_SD_ERR(SubdocCmd(cmd, "too_deep_dict", too_long_path),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG);

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
    cJSON* parent = nullptr;
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

    EXPECT_SD_VALEQ(SubdocCmd(cmd, "max_array", valid_max_path), "[]");
    delete_object("max_array");

    // b). Accessing a deeper array should fail.
    unique_cJSON_ptr too_deep_array(make_nested_array(MAX_SUBDOC_PATH_COMPONENTS + 1));
    char* too_deep_array_str = cJSON_PrintUnformatted(too_deep_array.get());
    store_object("too_deep_array", too_deep_array_str);
    cJSON_Free(too_deep_array_str);

    std::string too_long_path(make_nested_array_path(MAX_SUBDOC_PATH_COMPONENTS + 1));

    EXPECT_SD_ERR(SubdocCmd(cmd, "too_deep_array", too_long_path),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG);
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
    EXPECT_SD_ERR(SubdocCmd(cmd, "dict", "int", "2"),
                      PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    // b). Attempt to add to non-JSON document should return ENOT_JSON
    const char not_JSON[] = "not; valid, JSON";
    store_object("binary", not_JSON, /*JSON*/false, compress);
    EXPECT_SD_ERR(SubdocCmd(cmd, "binary", "int", "2"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON);
    delete_object("binary");

    // Store a simple JSON document to work on.
    const char dict[] = "{ \"key1\": 1 }";
    store_object("dict", dict, /*JSON*/true, compress);

    // c). Addition of primitive types to the dict.
    for (const auto& kv : key_vals) {
        EXPECT_SD_OK(SubdocCmd(cmd, "dict", kv.first, kv.second));
        EXPECT_SD_GET("dict", kv.first, kv.second);
    }

    // d). Check that attempts to add keys which already exist fail for DICT_ADD,
    // and are permitted for DICT_UPSERT.
    for (const auto& kv : key_vals) {
        SubdocCmd sd_cmd(cmd, "dict", kv.first, kv.second);
        if (cmd == PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD) {
            EXPECT_SD_ERR(sd_cmd, PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS);
        } else { // DICT_UPSERT
            EXPECT_SD_OK(sd_cmd);
            EXPECT_SD_GET("dict", kv.first, kv.second);
        }
    }

    // e). Check that attempts to add keys with a missing intermediate
    // dict path fail.
    for (const auto& kv : key_vals) {
        auto key = "intermediate." + kv.first;
        EXPECT_SD_ERR(SubdocCmd(cmd, "dict", key, kv.second),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);
    }

    // f). Check that attempts to add keys with missing intermediate
    // array path fail.
    for (const auto& kv : key_vals) {
        auto key = "intermediate_array[0]." + kv.first;
        EXPECT_SD_ERR(SubdocCmd(cmd, "dict", key, kv.second),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);
    }

    // g). ... and they still fail even if MKDIR_P flag is specified (as
    // intermediate array paths are never automatically created).
    for (const auto& kv : key_vals) {
        auto key = "intermediate_array[0]." + kv.first;
        EXPECT_SD_ERR(SubdocCmd(cmd, "dict", key, kv.second, SUBDOC_FLAG_MKDIR_P),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);
    }

    // h) However attempts to add keys with _dict_ intermediate paths should
    // succeed if the MKDIR_P flag is set.
    for (const auto& kv : key_vals) {
        auto key = "intermediate." + kv.first;
        EXPECT_SD_OK(SubdocCmd(cmd, "dict", key, kv.second, SUBDOC_FLAG_MKDIR_P));
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
        EXPECT_SD_ERR(SubdocCmd(cmd, "dict", kv.first, kv.second),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT);
    }

    // j). Check CAS support - cmd with correct CAS should succeed.
    // Get the current CAS.
    SubdocSingleResponse resp;
    EXPECT_SUBDOC_CMD_RESP(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, "dict", "int"),
        PROTOCOL_BINARY_RESPONSE_SUCCESS, "", resp);
    uint64_t cas = resp.getCas();
    EXPECT_NE(0, cas);
    EXPECT_SUBDOC_CMD_RESP(SubdocCmd(cmd, "dict", "new_int", "3", SUBDOC_FLAG_NONE, cas),
        PROTOCOL_BINARY_RESPONSE_SUCCESS, "", resp);

    uint64_t new_cas = resp.getCas();
    EXPECT_NE(cas, new_cas);

    // k). CAS - cmd with old cas should fail.
    EXPECT_SD_ERR(SubdocCmd(cmd, "dict", "new_int2", "4", SUBDOC_FLAG_NONE, cas),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    // l). CAS - manually corrupted (off by one) cas should fail.
    EXPECT_SD_ERR(SubdocCmd(cmd, "dict","new_int2", "4",
                            SUBDOC_FLAG_NONE, new_cas + 1),
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    delete_object("dict");

    // m). Attempt to perform dict command on array should fail.
    store_object("array", "[1,2]", /*JSON*/true, compress);
    EXPECT_SD_ERR(SubdocCmd(cmd, "array","foo", "\"bar\""),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
    delete_object("array");

    // n). Check that attempts to add keys to a valid JSON fragment which is
    // not in a container fail. (We cannot operate on non-dict or array JSON
    // objects).
    store_object("dict", "\"string\"", /*JSON*/true, compress);
    for (const auto& kv : key_vals) {
        EXPECT_SD_ERR(SubdocCmd(cmd, "dict", kv.first, kv.second),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON);
    }
    delete_object("dict");
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

// Test FEATURE_MUTATION_SEQNO support.
TEST_P(McdTestappTest, SubdocDictAdd_SimpleRaw_MutationSeqno) {
    set_mutation_seqno_feature(true);
    test_subdoc_dict_add_simple(/*compress*/false,
                                PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
    set_mutation_seqno_feature(false);
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
    ewouldblock_engine_configure(ENGINE_KEY_EEXISTS, EWBEngineMode::Sequence,
                                 0xffffffc4 /* <3 MSBytes all-ones>, 0b11,000,100 */);

    // .. Yet a client request should succeed, as internal CAS failure should
    // be retried.
    SubdocSingleResponse resp;
    EXPECT_SUBDOC_CMD_RESP(SubdocCmd(cmd, "dict","new_int3", "3"),
        PROTOCOL_BINARY_RESPONSE_SUCCESS, "", resp);
    uint64_t new_cas = resp.getCas();

    // b). Check that if the user specifies an explicit CAS, then a mismatch
    // isn't retried and EEXISTS is returned back to the user.

    // Setup ewouldblock_engine - first two calls succeed, 3rd (engine->store)
    // fails. Do not expect more than 3 calls so make any further calls error.
    ewouldblock_engine_configure(ENGINE_KEY_EEXISTS, EWBEngineMode::Sequence,
                                 0xfffffffc /* <3 MSBytes all-ones>, 0b11,111,100 */);

    EXPECT_SD_ERR(SubdocCmd(cmd, "dict","new_int4", "4", SUBDOC_FLAG_NONE, new_cas),
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

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
        EXPECT_SD_OK(SubdocCmd(cmd, "dict", key, kv.second));
        EXPECT_SD_GET("dict", key, kv.second);
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
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "dict", "bad_key"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);

    for (unsigned int ii = 0; ii < 8; ii++) {
        // Assert we can access it initially:
        std::string path(std::to_string(ii));
        SubdocSingleResponse resp;
        EXPECT_SUBDOC_CMD_RESP(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, "dict", path),
                                         PROTOCOL_BINARY_RESPONSE_SUCCESS, "",
                                         resp);
        uint64_t cas = resp.getCas();

        // Deleting with the wrong CAS should fail:
        EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "dict",
                                path, "", SUBDOC_FLAG_NONE, cas + 1),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

        EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, "dict",path));

        // Should be able to delete with no CAS specified.
        EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "dict", path));

        // ... and should no longer exist:
        EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, "dict", path),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);
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
    EXPECT_SD_GET("a", "[2]", "2");

    // a). Attempts to delete out of range elements should fail.
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[5]"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);

    // b). Test deleting at end of array.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[4]"));
    //     3rd element should still be 2; last element should now be 3.
    EXPECT_SD_GET("a", "[2]", "2");
    EXPECT_SD_GET("a", "[-1]", "3");
    validate_object("a", "[0,1,2,3]");

    // c). Test deleting at start of array; elements are shuffled down.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[0]"));
    //     3rd element should now be 3; last element should still be 3.
    EXPECT_SD_GET("a", "[2]", "3");
    EXPECT_SD_GET("a", "[-1]", "3");
    validate_object("a", "[1,2,3]");

    // d). Test deleting of last element using [-1].
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[-1]"));
    //     Last element should now be 2.
    EXPECT_SD_GET("a", "[-1]", "2");
    validate_object("a", "[1,2]");

    // e). Delete remaining elements.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[0]"));
    validate_object("a", "[2]");
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "a", "[0]"));
    // Should have an empty array.
    validate_object("a", "[]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocDelete_ArrayNested) {
    // Nested array containing different objects.
    store_object("b", "[0,[10,20,[100]],{\"key\":\"value\"}]",
                 /*JSON*/true, /*compress*/false);

    // Sanity check - 2nd element should be "[10,20,[100]]"
    EXPECT_SD_GET("b", "[1]", "[10,20,[100]]");

    // a). Delete nested array element
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "b", "[1][2][0]"));
    EXPECT_SD_GET("b", "[1]", "[10,20,[]]");

    // b). Delete the (now empty) nested array.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "b", "[1][2]"));
    EXPECT_SD_GET("b", "[1]", "[10,20]");

    // c). Delete the next level up array.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, "b", "[1]"));
    // element [1] should now be the dict.
    EXPECT_SD_GET("b", "[1]", "{\"key\":\"value\"}");

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
    EXPECT_SD_GET("a", "key", "0");

    // Replace the initial key with each primitive type:
    for (const auto& replace : JSON_VALUES) {
        EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                               "a", "key", replace));
        EXPECT_SD_GET("a", "key", replace);
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
    EXPECT_SD_GET("a", "[0]", "0");

    // Replace the first element with each primitive type:
    for (const auto& replace : JSON_VALUES) {
        EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a", "[0]",
                                    replace));
        EXPECT_SD_GET("a", "[0]", replace);
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
    EXPECT_SD_GET("a", valid_max_path, "[]");

    // a). Should be able to replace an element at the max depth.
    std::string new_value("\"deep\"");
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a",
        valid_max_path, new_value));
    EXPECT_SD_GET("a", valid_max_path, new_value);

    // b). But adding a nested array (taking the document over the maximum
    // depth) should fail.
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a",
                           valid_max_path, "[0]"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_ETOODEEP);


    // c). Replace the whole deep array with a single toplevel element.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "a", "[0]", "[]"));
    EXPECT_SD_GET("a", "[0]", "[]");
    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayPushLast_Simple)
{
    // a). Empty array, append to it.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST)
        .setKey("a").setPath("").setValue("0"));
    EXPECT_SD_GET("a", "[0]", "0");
    validate_object("a", "[0]");

    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a",
                           "", "1"));
    EXPECT_SD_GET("a", "[1]", "1");
    validate_object("a", "[0,1]");

    SubdocSingleResponse resp;
    EXPECT_SUBDOC_CMD_RESP(
        SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST)
        .setKey("a").setValue("2"), PROTOCOL_BINARY_RESPONSE_SUCCESS, "", resp);
    EXPECT_SD_GET("a", "[2]", "2");
    validate_object("a", "[0,1,2]");

    // b). Check that using the correct CAS succeeds.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "a", "", "3", SUBDOC_FLAG_NONE, resp.getCas()));
    EXPECT_SD_GET("a", "[3]", "3"),
    validate_object("a", "[0,1,2,3]");

    // c). But using the wrong one fails.
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                            "a", "", "4", SUBDOC_FLAG_NONE, resp.getCas()),
                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    validate_object("a", "[0,1,2,3]");
    delete_object("a");

    // d). Check various other object types append successfully.
    store_object("b", "[]", /*JSON*/true, /*compress*/false);
    int index = 0;
    for (const auto& value : JSON_VALUES) {
        EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                    "b", "", value));
        std::string path("[" + std::to_string(index) + "]");
        EXPECT_SD_GET("b", path, value);
        index++;
    }
    delete_object("b");

    // e). Check we can append multiple values at once.
    store_object("c", "[]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "c", "", "0,1"));
    validate_object("c", "[0,1]");
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "c", "", "\"two\",3.141,{\"four\":4}"));
    validate_object("c", "[0,1,\"two\",3.141,{\"four\":4}]");

    delete_object("c");

    // f). Check MKDIR_P flag works.
    store_object("d", "{}", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "d", "foo", "0", SUBDOC_FLAG_MKDIR_P));
    delete_object("d");
}

TEST_P(McdTestappTest, SubdocArrayPushLast_Nested)
{
    // Operations on a nested array,
    // a). Begin with an empty nested array, append to it.
    store_object("a", "[[]]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a",
                 "", "1"));
    EXPECT_SD_GET("a", "[0]", "[]");
    EXPECT_SD_GET("a", "[1]", "1");
    validate_object("a", "[[],1]");

    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a",
                 "", "2"));
    EXPECT_SD_GET("a", "[2]", "2");
    validate_object("a", "[[],1,2]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayPushFirst_Simple)
{
    // a). Empty array, prepend to it.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, "a", "", "0"));
    EXPECT_SD_GET("a", "[0]", "0");
    validate_object("a", "[0]");

    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, "a", "", "1"));
    EXPECT_SD_GET("a", "[0]", "1");
    validate_object("a", "[1,0]");

    SubdocSingleResponse resp;
    EXPECT_SUBDOC_CMD_RESP(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                               "a", "", "2"),
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS, "", resp);
    EXPECT_SD_GET("a", "[0]", "2");
    validate_object("a", "[2,1,0]");

    // b). Check that using the correct CAS succeeds.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                           "a", "", "3", SUBDOC_FLAG_NONE, resp.getCas()));
    EXPECT_SD_GET("a", "[0]", "3");
    validate_object("a", "[3,2,1,0]");

    // c). But using the wrong one fails.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                "a", "", "4", SUBDOC_FLAG_NONE, resp.getCas()),
                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, "");
    validate_object("a", "[3,2,1,0]");
    delete_object("a");

    // d). Check various other object types prepend successfully.
    store_object("b", "[]", /*JSON*/true, /*compress*/false);
    for (const auto& value : JSON_VALUES) {
        EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                                    "b", "", value));
        EXPECT_SD_GET("b", "[0]", value);
    }
    delete_object("b");

    // e). Check we can prepend multiple values at once.
    store_object("c", "[]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                           "c", "", "0,1"));
    validate_object("c", "[0,1]");
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                           "c", "", "\"two\",3.141,{\"four\":4}"));
    validate_object("c", "[\"two\",3.141,{\"four\":4},0,1]");
    delete_object("c");

    // f). Check MKDIR_P flag works.
    store_object("d", "{}", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                           "d", "foo", "0", SUBDOC_FLAG_MKDIR_P));
    delete_object("d");
}

TEST_P(McdTestappTest, SubdocArrayPushFirst_Nested)
{
    // Operations on a nested array.
    // a). Begin with an empty nested array, prepend to it.
    store_object("a", "[[]]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, "a",
                           "", "1"));
    EXPECT_SD_GET("a", "[0]", "1");
    EXPECT_SD_GET("a", "[1]", "[]");
    validate_object("a", "[1,[]]");

    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                 "a","", "2"));
    EXPECT_SD_GET("a", "[0]", "2");
    validate_object("a", "[2,1,[]]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayAddUnique_Simple)
{
    // Start with an array with a single element.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);

    // a). Add an element which doesn't already exist.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                           "a", "", "0"));
    validate_object("a", "[0]");

    // b). Add an element which does already exist.
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                            "a", "", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS);
    validate_object("a", "[0]");
    delete_object("a");

    // c). Larger array, add an element which already exists.
    std::string array("[0,1,2,3,4,5,6,7,8,9]");
    store_object("b", array, /*JSON*/true, /*compress*/false);
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                            "b", "", "6"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS);
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
        EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                               "b", "", v));
    }
    // ... and attempting to add a second time returns EEXISTS
    for (const auto& v : valid_unique_values) {
        EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                "b", "", v),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS);
    }

#if 0 // TODO: According to the spec this shouldn't be permitted, however it
      // currently works...
    // f). Check it is not permitted to add non-primitive types (arrays, objects).
    const std::vector<std::string> invalid_unique_values({
        "{\"foo\": \"bar\"}",
        "[0,1,2]"});
    for (const auto& v : invalid_unique_values) {
        EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                                    "b", "", v),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    }
#endif
    delete_object("b");

    // g). Attempts to add_unique to a array with non-primitive values should
    // fail.
    store_object("c", "[{\"a\":\"b\"}]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                            "c", "", "1"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
    delete_object("c");

    store_object("d", "[[1,2]]", /*JSON*/true, /*compress*/false);
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                            "d", "", "3"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
    delete_object("d");
}

TEST_P(McdTestappTest, SubdocArrayInsert_Simple)
{
    // Start with an empty array.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);

    // a). Attempt to insert at position 0 should succeed.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[0]", "2"));
    validate_object("a", "[2]");

    // b). Second insert at zero should succeed and shuffle existing element
    // down.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[0]", "0"));
    validate_object("a", "[0,2]");

    // c). Insert at position 1 should shuffle down elements after, leave alone
    // elements before.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                           "a", "[1]", "1"));
    validate_object("a", "[0,1,2]");

    // d). Insert at len(array) should add to the end, without moving existing
    // elements.
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[3]", "3"));
    validate_object("a", "[0,1,2,3]");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocArrayInsert_Invalid)
{
    // Start with an empty array.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);

    // a). Attempt to insert past the end of the (empty) array should fail.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[1]", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    validate_object("a", "[]");

    // b). Insert at position '-1' is invalid.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[-1]", "3"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL, "");
    reconnect_to_server();
    validate_object("a", "[]");

    // c). MKDIR_P flag is not valid for ARRAY_INSERT
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[0]", "1", SUBDOC_FLAG_MKDIR_P),
                      PROTOCOL_BINARY_RESPONSE_EINVAL, "");
    reconnect_to_server();
    validate_object("a", "[]");

    // d). A path larger than len(array) should fail.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[1]", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, "");
    validate_object("a", "[]");

    // e). A path whose has component isn't an array subscript should fail.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "a", "[0].foo", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL, "");
    reconnect_to_server();
    validate_object("a", "[]");

    delete_object("a");

    // f). Attempt to insert to a dict should fail.
    store_object("b", "{}", /*JSON*/true, /*compress*/false);
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                                "b", "[0]", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH, "");
    validate_object("b", "{}");
    delete_object("b");
}

TEST_P(McdTestappTest, SubdocGetCount)
{
    // Start with an empty array.
    store_object("a", "[]", /*JSON*/true, /*compress*/false);

    // Get size. Should be 0
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT, "a", ""), "0");

    // Store it again, giving it some size
    store_object("a", "[1,2,3]", true, false);
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT, "a", ""), "3");

    // Check for mismatch
    store_object("a", "{\"k\":\"v\"}", true, false);
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT, "a", "k"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);

    // Check for non-found
    EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT, "a", "n"),
                  PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);
    delete_object("a");
}

void test_subdoc_counter_simple() {
    store_object("a", "{}", /*JSON*/true, /*compress*/false);

    // a). Check that empty document, empty path creates a new element.
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              "a", "key", "1"), "1");
    auto result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":1}", result.second);

    // b). Check we can now increment it further.
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              "a", "key", "1"), "2");
    result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":2}", result.second);

    // c). Decrement by 2; should go back to zero.
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "-2"), "0");
    result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":0}", result.second);

    // d). Decrement by 1; should go negative.
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "-1"), "-1");
    result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":-1}", result.second);

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocCounter_Simple) {
    test_subdoc_counter_simple();
}

TEST_P(McdTestappTest, SubdocCounter_Simple_MutationSeqno) {
    set_mutation_seqno_feature(true);
    test_subdoc_counter_simple();
    set_mutation_seqno_feature(false);
}

static const std::vector<std::string> NOT_INTEGER({
    "true",
    "false",
    "null",
    "\"string\"",
    "[0]",
    "{\"foo\": \"bar\"}",
    "1.1"
});

TEST_P(McdTestappTest, SubdocCounter_InvalidNotInt)
{
    // Cannot increment things which are not integers.
    for (auto& val : NOT_INTEGER) {
        const std::string doc("{\"key\":" + val + "}");
        store_object("a", doc, /*JSON*/true, /*compress*/false);
        EXPECT_SD_ERR(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                    "a", "key", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
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
        EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
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
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "1"), std::to_string(max));

    auto result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":" + std::to_string(max) + "}", result.second);

    // b). A further increment by one should fail.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT, "");

    delete_object("a");

    // c). Same with int64_t::min() and decrement.
    const int64_t min = std::numeric_limits<int64_t>::min();

    store_object("b", "{\"key\":" + std::to_string(min + 1) + "}",
                 /*JSON*/true, /*compress*/false);
    EXPECT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              "b", "key", "-1"), std::to_string(min));

    result = fetch_value("b");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("{\"key\":" + std::to_string(min) + "}", result.second);

    // b). A further decrement by one should fail.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "b", "key", "-1"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT, "");

    delete_object("b");
}

TEST_P(McdTestappTest, SubdocCounter_InvalidIncr)
{
    // Cannot increment by a non-numeric value.
    const std::string doc("{\"key\":10}");
    store_object("a", doc, /*JSON*/true, /*compress*/false);

    for (auto& incr : NOT_INTEGER) {
        EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                    "a", "key", incr),
                          PROTOCOL_BINARY_RESPONSE_SUBDOC_DELTA_EINVAL, "");
        auto result = fetch_value("a");
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first)
            << " using increment '" << incr << "'";
        EXPECT_EQ(doc, result.second);
    }

    // Cannot increment by zero.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                                "a", "key", "0"),
                      PROTOCOL_BINARY_RESPONSE_SUBDOC_DELTA_EINVAL, "");
    auto result = fetch_value("a");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ(doc, result.second);

    delete_object("a");
}

// Test handling of the internal auto-retry when a CAS mismatch occurs due
// to the underlying document changing between subdoc reading the initial value
// and trying to write the new value (after applying the subdoc modification).
TEST_P(McdTestappTest, SubdocCASAutoRetry)
{
    // Store a simple dict value to operate on.
    store_object("a", "{}");

    // 1. Setup ewouldblock_engine - make the first three store commands return
    // EXISTS.
    ewouldblock_engine_configure(ENGINE_SUCCESS, // not used for this mode
                                 EWBEngineMode::CasMismatch,
                                 3);

    // Issue a DICT_ADD without an explicit CAS. We should have an auto-retry
    // occur (and the command succeed).
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                                "a", "key1", "1"));

    // 2. Now retry with MAXIMUM_ATTEMPTS-1 CAS mismatches - this should still
    // succeed.
    ewouldblock_engine_configure(ENGINE_SUCCESS, // not used for this mode
                                 EWBEngineMode::CasMismatch, 99);
    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                           "a", "key2", "2"));

    // 3. Now with MAXIMUM_ATTEMPTS CAS mismatches - this should return TMPFAIL.
    ewouldblock_engine_configure(ENGINE_SUCCESS, // not used for this mode
                                 EWBEngineMode::CasMismatch,
                                 100);
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                                "a", "key3", "3"),
                      PROTOCOL_BINARY_RESPONSE_ETMPFAIL, "");
    delete_object("a");
}

TEST_P(McdTestappTest, SubdocMkdoc_Array)
{
    // Create new document (array)
    ASSERT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                           "a", "", "0", SUBDOC_FLAG_MKDOC));
    EXPECT_SD_GET("a", "[0]", "0");

    // Flag doesn't do anything if doc already exists
    ASSERT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                           "a", "", "1", SUBDOC_FLAG_MKDOC));
    EXPECT_SD_GET("a", "[1]", "1");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocMkdoc_Dict) {
    // Create new document (dictionary)
    ASSERT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                           "a", "foo", "1", SUBDOC_FLAG_MKDOC));
    EXPECT_SD_GET("a", "foo", "1");

    // Flag doesn't do anything if doc already exists
    ASSERT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                           "a", "bar", "2", SUBDOC_FLAG_MKDOC));
    EXPECT_SD_GET("a", "bar", "2");

    // Flag still has MKDIR_P semantics if needed
    ASSERT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                           "a", "nested.path", "3", SUBDOC_FLAG_MKDOC));
    EXPECT_SD_GET("a", "nested.path", "3");

    delete_object("a");
}

TEST_P(McdTestappTest, SubdocMkdoc_Counter) {
    // Counter should also work (with a path) + MKDIR_P
    ASSERT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              "a", "counter.path", "42", SUBDOC_FLAG_MKDOC), "42");
    // Repeat should be OK
    ASSERT_SD_VALEQ(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                              "a", "counter.path", "42", SUBDOC_FLAG_MKDOC), "84");
    delete_object("a");
}

// Test operation of setting document expiry for single-path commands.
TEST_P(McdTestappTest, SubdocExpiry_Single)
{
    // Create two documents; one to be used for an exlicit 1s expiry and one
    // for an explicit 0s (i.e. never) expiry.
    store_object("ephemeral", "[\"a\"]");
    store_object("permanent", "[\"a\"]");

    // Expiry not permitted for SUBDOC_GET operations.
    SubdocCmd get(PROTOCOL_BINARY_CMD_SUBDOC_GET, "ephemeral", "[0]");
    get.setExpiry(666);
    EXPECT_SD_ERR(get, PROTOCOL_BINARY_RESPONSE_EINVAL);
    reconnect_to_server();

    // Perform a REPLACE operation, setting a expiry of 1s.
    SubdocCmd replace(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "ephemeral", "[0]",
                      "\"b\"");
    EXPECT_SD_OK(replace.setExpiry(1));

    // Try to read the document immediately - should exist.
    auto result = fetch_value("ephemeral");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, result.first);
    EXPECT_EQ("[\"b\"]", result.second);

    // Perform a REPLACE, explicitly encoding an expiry of 0s.
    SubdocCmd replace2(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "permanent", "[0]",
                      "\"b\"");
    EXPECT_SD_OK(replace2.setExpiry(0));

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

// Test handling of not-my-vbucket for a SUBDOC_GET
TEST_P(McdTestappTest, SubdocGet_NotMyVbucket)
{
    const char array[] = "[0]";
    store_object("array", array);

    // Make the next engine operation (get) return NOT_MY_VBUCKET.
    ewouldblock_engine_configure(ENGINE_NOT_MY_VBUCKET, EWBEngineMode::Next_N,
                                 1);

    // Should fail with NOT-MY-VBUCKET, and a non-zero length body including the
    // cluster config.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "array", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "");

    // Second attempt should succced (as only next 1 engine op was set to fail).
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_GET, "array", "[0]"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "0");

    delete_object("array");
}

// Test handling of not-my-vbucket for a SUBDOC_DICT_ADD
TEST_P(McdTestappTest, SubdocArrayPushLast_NotMyVbucket)
{
    const char array[] = "[0]";
    store_object("array", array);

    // Configure the ewouldblock_engine to inject fake NOT-MY-VBUCKET failure
    // for the 3rd call (i.e. the 1st engine->store() attempt). We only expect 6 calls
    // total, so also make anything after that fail.
    ewouldblock_engine_configure(ENGINE_NOT_MY_VBUCKET, EWBEngineMode::Sequence,
                                 0xffffffc4 /* <3 MSBytes all-ones>, 0b11,000,100 */);

    // Should fail with NOT-MY-VBUCKET, and a non-zero length body including the
    // cluster config.
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "array", "", "1"),
                      PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "");

    // Second attempt should succced (as only next 1 engine op was set to fail).
    EXPECT_SUBDOC_CMD(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                "array", "", "1"),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, "");

    // Cleanup.
    ewouldblock_engine_disable();
    delete_object("array");
}

// Test that flags are preserved by subdoc mutation operations.
TEST_P(McdTestappTest, SubdocFlags)
{
    const char array[] = "[0]";
    const uint32_t flags = 0xcafebabe;
    store_object_with_flags("array", array, flags);

    EXPECT_SD_OK(SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, "array",
                           "[0]", "1"));

    validate_object("array", "[1]");
    validate_flags("array", 0xcafebabe);

    delete_object("array");
}


enum class SubdocCmdType {
    Lookup,
    Mutation
};

struct SubdocStatTraits {
    const char* count_name;
    const char* bytes_total_name;
    const char* bytes_extracted_subset;
};

static const SubdocStatTraits LOOKUP_TRAITS { "cmd_subdoc_lookup",
                                              "bytes_subdoc_lookup_total",
                                              "bytes_subdoc_lookup_extracted" };

static const SubdocStatTraits MUTATION_TRAITS { "cmd_subdoc_mutation",
                                                "bytes_subdoc_mutation_total",
                                                "bytes_subdoc_mutation_inserted" };

static void test_subdoc_stats_command(protocol_binary_command cmd,
                                      SubdocStatTraits traits,
                                      const std::string& doc,
                                      const std::string& path,
                                      const std::string& value,
                                      const std::string& fragment,
                                      size_t expected_total_len,
                                      size_t expected_subset_len) {
    store_object("doc", doc.c_str());

    // Get initial stats
    auto stats = request_stats();
    auto count_before = extract_single_stat(stats, traits.count_name);
    auto bytes_before_total = extract_single_stat(stats, traits.bytes_total_name);
    auto bytes_before_subset = extract_single_stat(stats, traits.bytes_extracted_subset);

    // Perform the operation
    EXPECT_SUBDOC_CMD(SubdocCmd(cmd, "doc", path, value),
                      PROTOCOL_BINARY_RESPONSE_SUCCESS, fragment);

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

TEST_P(McdTestappTest, SubdocStatsLookupGet) {
    std::string doc("[10,11,12,13,14,15,16,17,18,19]");
    std::string response("10");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_GET, LOOKUP_TRAITS,
                              doc, "[0]", "", response,
                              doc.size(), response.size());
}
TEST_P(McdTestappTest, SubdocStatsLookupExists) {
    std::string doc("[10,11,12,13,14,15,16,17,18,19]");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, LOOKUP_TRAITS,
                              doc, "[0]", "", "", doc.size(), 0);
}
TEST_P(McdTestappTest, SubdocStatsDictAdd) {
    std::string input("{\"foo\":1,\"bar\":2}");
    std::string path("baz");
    std::string fragment("3");
    std::string result("{\"foo\":1,\"bar\":2,\"baz\":3}");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                              MUTATION_TRAITS, input, path, fragment, "",
                              result.size(), fragment.size());
}
TEST_P(McdTestappTest, SubdocStatsDictUpsert) {
    std::string input("{\"foo\":1,\"bar\":2}");
    std::string path("bar");
    std::string fragment("3");
    std::string result("{\"foo\":1,\"bar\":3}");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                              MUTATION_TRAITS, input, path, fragment, "",
                              result.size(), fragment.size());
}
TEST_P(McdTestappTest, SubdocStatsDelete) {
    std::string input("{\"foo\":1,\"bar\":2,\"baz\":3}");
    std::string path("baz");
    std::string result("{\"foo\":1,\"bar\":2}");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                               MUTATION_TRAITS, input, path, "", "",
                               result.size(), 0);
}
TEST_P(McdTestappTest, SubdocStatsReplace) {
    std::string input("{\"foo\":1,\"bar\":2}");
    std::string path("bar");
    std::string fragment("3");
    std::string result("{\"foo\":1,\"bar\":3}");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                              MUTATION_TRAITS, input, path, fragment, "",
                              result.size(), fragment.size());
}
TEST_P(McdTestappTest, SubdocStatsArrayPushLast) {
    std::string input("[10,11,12,13,14,15,16,17,18,19]");
    std::string fragment("20");
    std::string result("[10,11,12,13,14,15,16,17,18,19,20]");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                               MUTATION_TRAITS, input, "", fragment, "",
                               result.size(), fragment.size());
}
TEST_P(McdTestappTest, SubdocStatsArrayPushFirst) {
    std::string input("[10,11,12,13,14,15,16,17,18,19]");
    std::string fragment("9");
    std::string result("[9,10,11,12,13,14,15,16,17,18,19]");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                               MUTATION_TRAITS, input, "", fragment, "",
                               result.size(), fragment.size());
}
TEST_P(McdTestappTest, SubdocStatsArrayInsert) {
    std::string input("[9,11,12,13,14,15,16,17,18,19]");
    std::string path("[0]");
    std::string fragment("10");
    std::string result("[9,10,11,12,13,14,15,16,17,18,19]");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                               MUTATION_TRAITS, input, path, fragment, "",
                               result.size(), fragment.size());
}
TEST_P(McdTestappTest, SubdocStatsArrayAddUnique) {
    std::string input("[10,11,12,13,14,15,16,17,18,19]");
    std::string fragment("20");
    std::string result("[10,11,12,13,14,15,16,17,18,19,20]");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                               MUTATION_TRAITS, input, "", fragment, "",
                               result.size(), fragment.size());
}
TEST_P(McdTestappTest, SubdocStatsCounter) {
    std::string input("{\"foo\":1,\"bar\":2}");
    std::string path("bar");
    std::string fragment("1");
    std::string result("{\"foo\":1,\"bar\":3}");
    test_subdoc_stats_command(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                               MUTATION_TRAITS, input, path, fragment, "3",
                               result.size(), fragment.size());
}


// Tests how a single worker handles multiple "concurrent" connections
// performing operations.
class WorkerConcurrencyTest : public TestappTest {
public:
    static void SetUpTestCase() {
        memcached_cfg.reset(generate_config(0));
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
    SOCKET sock2 = connect_to_server_plain(port);
    ASSERT_NE(sock2, INVALID_SOCKET);
    sock = sock1;

    const size_t push_count = 100;
    std::vector<char> send_buf;

    // Build pipeline for the even commands.
    std::string expected_a;
    for (unsigned int i = 0; i < push_count; i += 2) {
        expected_a += std::to_string(i) + ",";
        auto cmd = SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, "a", "", std::to_string(i));
        cmd.encode(send_buf);
    }
    *current_sock = sock1;
    safe_send(send_buf.data(), send_buf.size(), false);

    // .. and the odd commands.
    send_buf.clear();
    std::string expected_b;
    for (unsigned int i = 1; i < push_count; i += 2) {
        expected_b += std::to_string(i) + ",";
        auto cmd = SubdocCmd(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                             "b", "", std::to_string(i));
        cmd.encode(send_buf);
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
