/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
 * testapp testcases for sub-document API.
 */

#pragma once

#include "testapp.h"

#include "utilities/subdoc_encoder.h"

#include <memory>

// Class representing a subdoc command; to assist in constructing / encoding one.
class SubdocCmd : public TestCmdT<SubdocCmd> {
public:
    SubdocCmd() : TestCmdT() {}

    explicit SubdocCmd(protocol_binary_command cmd_) {
        setOp(cmd_);
    }

    // Old-style constructors
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_)
      : SubdocCmd(cmd_, key_, path_, "", SUBDOC_FLAG_NONE, 0) {}

    // Constructor including a value.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_)
      : SubdocCmd(cmd_, key_, path_, value_, SUBDOC_FLAG_NONE, 0) {}

    // Constructor additionally including flags.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_,
                       protocol_binary_subdoc_flag flags_)
    : SubdocCmd(cmd_, key_, path_, value_, flags_, 0) {}

    // Constructor additionally including CAS.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_,
                       protocol_binary_subdoc_flag flags_, uint64_t cas_)
        : TestCmdT() {
        setOp(cmd_);
        setKey(key_);
        setPath(path_);
        setValue(value_);
        setFlags(flags_);
        setCas(cas_);
    }

    SubdocCmd& setPath(const std::string& path_) {
        path = path_;
        return *this;
    }

    SubdocCmd& setValue(const std::string& value_) {
        value = value_;
        return *this;
    }

    SubdocCmd& setFlags(protocol_binary_subdoc_flag flags_) {
        flags = flags_;
        return *this;
    }

    virtual void encode(std::vector<char>& buf) const override;
    using TestCmd::encode;

    std::string path;
    std::string value;
    protocol_binary_subdoc_flag flags = SUBDOC_FLAG_NONE;
};

class SubdocSingleResponse : public TestResponse {
public:
    explicit SubdocSingleResponse(std::vector<char>&& buf) {
        // Base constructor will call its own version of assign(), which is a
        // bad thing.
        assign(std::move(buf));
    }

    SubdocSingleResponse() : TestResponse() {}

    const std::string& getValue() {
        return value;
    }

    virtual void assign(std::vector<char>&& srcbuf) {
        TestResponse::assign(std::move(srcbuf));
        if (getBodylen() - getExtlen() > 0) {
            value.assign(raw_packet.data() +
                         sizeof (protocol_binary_response_header) + getExtlen(),
                         raw_packet.data() + raw_packet.size());
        }
    }

    virtual void clear() {
        TestResponse::clear();
        value.clear();
    }

private:
    std::string value;
};

typedef std::pair<protocol_binary_response_status,
                  std::string> SubdocMultiLookupResult;

/* The result of an operation on a particular path performed in a
 * multi-mutation.
 */
struct SubdocMultiMutationResult {

    SubdocMultiMutationResult(uint8_t index_,
                              protocol_binary_response_status status_)
    : index(index_),
      status(status_),
      result() {}

    SubdocMultiMutationResult(uint8_t index_,
                              protocol_binary_response_status status_,
                              const std::string& result_)
    : index(index_),
      status(status_),
      result(result_) {}

    uint8_t index;
    protocol_binary_response_status status;
    std::string result;
};

/* Encodes and sends a sub-document command with the given parameters, receives
 * the response and validates that the status matches the expected one.
 * If expected_value is non-empty, also verifies that the response value equals
 * expectd_value.
 * @return CAS value (if applicable for the command, else zero).
 */

/**
 * Encodes and sends the subdocument command `cmd`. It will check that the
 * status matches the provided status. If the status is successful, it will
 * compare the value (`expected_value`) against the returned value.
 *
 * It will return an AssertionResult (successful or failed) depending on whether
 * all tests passed.
 *
 * @param cmd Command to send
 * @param expected_status Ensure the status matches this value
 * @param expected_value Ensure the value matches this value (can be empty)
 * @param[out] resp Response object. Will be populated with response information
 * @return
 */
::testing::AssertionResult subdoc_verify_cmd(const SubdocCmd& cmd,
                                             protocol_binary_response_status expected_status,
                                             const std::string& expected_value,
                                             SubdocSingleResponse& resp);

// Overload which creates a temporary response object
inline ::testing::AssertionResult subdoc_verify_cmd(const SubdocCmd& cmd,
                                                    protocol_binary_response_status err = PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                                    const std::string& value = "") {
    SubdocSingleResponse resp;
    return subdoc_verify_cmd(cmd, err, value, resp);
}

// Gtest helper
template<typename T>
::testing::AssertionResult subdoc_pred_ok(const char *, const T& cmd) {
    return subdoc_verify_cmd(cmd);
}

// Gtest helper
template<typename T>
::testing::AssertionResult subdoc_pred_value(const char *, const char *,
                                             const T& cmd,
                                             const std::string& value) {
    return subdoc_verify_cmd(cmd, PROTOCOL_BINARY_RESPONSE_SUCCESS, value);
}

// Gtest helper
template<typename T>
::testing::AssertionResult subdoc_pred_errcode(const char *, const char *,
                                               const T& cmd,
                                               protocol_binary_response_status expected) {
    return subdoc_verify_cmd(cmd, expected);
}

// Gtest helper
template<typename T>
::testing::AssertionResult subdoc_pred_compat(const char *, const char *,
                                              const char *, const T& cmd,
                                              protocol_binary_response_status err,
                                              const std::string& value) {
    return subdoc_verify_cmd(cmd, err, value);
}

// Gtest helper
template<typename Tc, typename Tr>
::testing::AssertionResult subdoc_pred_full(const char *, const char *,
                                            const char *, const char *,
                                            const Tc& cmd,
                                            protocol_binary_response_status err,
                                            const std::string& value,
                                            Tr& resp) {
    return subdoc_verify_cmd(cmd, err, value, resp);
}

uint64_t expect_subdoc_cmd(const SubdocMultiLookupCmd& cmd,
                           protocol_binary_response_status expected_status,
                           const std::vector<SubdocMultiLookupResult>& expected_results);

uint64_t expect_subdoc_cmd(const SubdocMultiMutationCmd& cmd,
                           protocol_binary_response_status expected_status,
                           const std::vector<SubdocMultiMutationResult>& expected_results);


void store_object(const std::string& key,
                  const std::string& value,
                  bool JSON, bool compress);
