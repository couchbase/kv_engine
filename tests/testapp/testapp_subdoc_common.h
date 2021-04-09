/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Common functions / classes for sub-document API tests.
 */

#pragma once

#include "testapp.h"

#include "subdoc_encoder.h"

#include <memory>

// Properties of a particular subdoc statistic set.
struct SubdocStatTraits {
    const char* count_name;
    const char* bytes_total_name;
    const char* bytes_extracted_subset;
};

typedef std::pair<cb::mcbp::Status, std::string> SubdocMultiLookupResult;

/**
 * Subclass of McdTestappTest - adds helpers for various generic testcases,
 * and functions to assist GTest in checking the results of tests.
 */
class SubdocTestappTest : public McdTestappTest {
protected:
    /* Helpers for individual testcases */

    void test_subdoc_counter_simple();

    void test_subdoc_dict_add_cas(bool compress, cb::mcbp::ClientOpcode cmd);

    void test_subdoc_dict_add_simple(bool compress, cb::mcbp::ClientOpcode cmd);

    void test_subdoc_dict_add_upsert_deep(cb::mcbp::ClientOpcode cmd);

    void test_subdoc_delete_simple(bool compress);

    void test_subdoc_fetch_array_deep(cb::mcbp::ClientOpcode cmd);

    void test_subdoc_fetch_array_simple(bool compressed,
                                        cb::mcbp::ClientOpcode cmd);

    void test_subdoc_fetch_dict_deep(cb::mcbp::ClientOpcode cmd);

    void test_subdoc_fetch_dict_nested(bool compressed,
                                       cb::mcbp::ClientOpcode cmd);

    void test_subdoc_fetch_dict_simple(bool compressed,
                                       cb::mcbp::ClientOpcode cmd);

    void test_subdoc_get_binary(bool compress,
                                cb::mcbp::ClientOpcode cmd,
                                MemcachedConnection& conn);

    void test_subdoc_stats_command(cb::mcbp::ClientOpcode cmd,
                                   SubdocStatTraits traits,
                                   const std::string& doc,
                                   const std::string& path,
                                   const std::string& value,
                                   const std::string& fragment,
                                   size_t expected_total_len,
                                   size_t expected_subset_len);

    /**
     * Encodes and sends the subdocument command `cmd`. It will check that the
     * status matches the provided status. If the status is successful, it will
     * compare the value (`expected_value`) against the returned value.
     *
     * It will return an AssertionResult (successful or failed) depending on
     * whether
     * all tests passed.
     *
     * @param cmd Command to send
     * @param expected_status Ensure the status matches this value
     * @param expected_value Ensure the value matches this value (can be empty)
     * @param[out] resp Response object. Will be populated with response
     * information
     * @return
     */
    ::testing::AssertionResult subdoc_verify_cmd(
            const BinprotSubdocCommand& cmd,
            cb::mcbp::Status expected_status,
            const std::string& expected_value,
            BinprotSubdocResponse& resp);

    /// Overload which creates a temporary response object
    ::testing::AssertionResult subdoc_verify_cmd(
            const BinprotSubdocCommand& cmd,
            cb::mcbp::Status err = cb::mcbp::Status::Success,
            const std::string& value = "") {
        BinprotSubdocResponse resp;
        return subdoc_verify_cmd(cmd, err, value, resp);
    }

    // Gtest helper
    template <typename T>
    ::testing::AssertionResult subdoc_pred_ok(const char*, const T& cmd) {
        return subdoc_verify_cmd(cmd);
    }

    // Gtest helper
    template <typename T>
    ::testing::AssertionResult subdoc_pred_value(const char*,
                                                 const char*,
                                                 const T& cmd,
                                                 const std::string& value) {
        return subdoc_verify_cmd(cmd, cb::mcbp::Status::Success, value);
    }

    // Gtest helper, used by subdoc tests
    template <typename T>
    ::testing::AssertionResult subdoc_pred_errcode(const char*,
                                                   const char*,
                                                   const T& cmd,
                                                   cb::mcbp::Status expected) {
        BinprotSubdocResponse resp;
        return subdoc_verify_cmd(cmd, expected, {}, resp);
    }

    // Gtest helper
    template <typename T>
    ::testing::AssertionResult subdoc_pred_compat(const char*,
                                                  const char*,
                                                  const char*,
                                                  const T& cmd,
                                                  cb::mcbp::Status err,
                                                  const std::string& value) {
        return subdoc_verify_cmd(cmd, err, value);
    }

    // Gtest helper
    template <typename Tc, typename Tr>
    ::testing::AssertionResult subdoc_pred_full(const char*,
                                                const char*,
                                                const char*,
                                                const char*,
                                                const Tc& cmd,
                                                cb::mcbp::Status err,
                                                const std::string& value,
                                                Tr& resp) {
        return subdoc_verify_cmd(cmd, err, value, resp);
    }
};

/* The result of an operation on a particular path performed in a
 * multi-mutation.
 */
struct SubdocMultiMutationResult {
    SubdocMultiMutationResult(uint8_t index_, cb::mcbp::Status status_)
        : index(index_), status(status_), result() {
    }

    SubdocMultiMutationResult(uint8_t index_,
                              cb::mcbp::Status status_,
                              const std::string& result_)
        : index(index_), status(status_), result(result_) {
    }

    uint8_t index;
    cb::mcbp::Status status;
    std::string result;
};

uint64_t expect_subdoc_cmd(
        const SubdocMultiLookupCmd& cmd,
        cb::mcbp::Status expected_status,
        const std::vector<SubdocMultiLookupResult>& expected_results);

uint64_t expect_subdoc_cmd(
        const SubdocMultiMutationCmd& cmd,
        cb::mcbp::Status expected_status,
        const std::vector<SubdocMultiMutationResult>& expected_results);
