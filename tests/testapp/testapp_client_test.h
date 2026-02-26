/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "testapp.h"
#include <memcached/durability_spec.h>
#include <algorithm>
#include <optional>

/**
 * Test fixture for testapp tests; parameterised on the TransportProtocol (IPv4,
 * Ipv6); (Plain, SSL).
 */
class TestappClientTest
    : public TestappTest,
      public ::testing::WithParamInterface<TransportProtocols> {
public:
    static void SetUpTestCase();

    /**
     * Get the value for a given command counter
     *
     * @param name The name of the command counter
     * @return Its value
     * @throws std::runtime_error if the counter isn't found
     */
    size_t get_cmd_counter(std::string_view name);

    bool isTlsEnabled() const override;

    /**
     * Populate the bucket with data until we have less than the provided
     * present of resident data in the active vbucket (we're only using vb0)
     * by using the key pattern: "mykey-<number>".
     *
     * @returns the number of keys inserted
     */
    size_t populateData(double limit = 90.0);

    /**
     * Rerun the access scanner task for the current bucket being tested
     * and wait for it to complete.
     */
    void rerunAccessScanner(int num_shards);

    /**
     * Get the current access scanner task stats for the current bucket being
     * tested
     *
     * @return the JSON object returned for AccessScanner from the 'stats tasks'
     */
    nlohmann::json getAccessScannerStats();

    /**
     * Wait until the access scanner task is in "snoozed" state for the
     * current bucket being tested;
     * (and the access scanner visitor tasks have completed).
     *
     * @return the number of runs the access scanner has done
     */
    int waitForSnoozedAccessScanner();

    /**
     * Get the number of access scanner runs and skips
     * @return a pair with the first entry being the number of runs
     *        and the second entry being the number of skips
     */
    std::pair<int, int> getNumAccessScannerCounts();

    int getAggregatedAccessScannerCounts() {
        auto [runs, skips] = getNumAccessScannerCounts();
        return runs + skips;
    }

    /**
     * Get the number of shards in the current bucket being tested.
     * @return the number of shards
     */
    int getNumShards();

    /**
     * Verify that the expected set of access log files exist. If encrypted
     * is set to true; the files should be encrypted and have an .cef suffix.
     * Otherwise they should be unencrypted and have no suffix.
     *
     * @param shards list of all shards - true if they are expected to have
     * access logs. shards.size should be the total number of shards in the
     * bucket. The function will iterate for shards.size and use the true/false
     * value for applying the checks
     * @param encrypted see above
     */
    void verifyAccessLogFiles(const std::vector<bool>& shards, bool encrypted);

    /**
     * Verify that there are no access log files for the bucket
     * @param num_shards the number of shards in the bucket
     */
    void verifyNoAccessLogFiles(int num_shards);
};

enum class XattrSupport { Yes, No };
std::ostream& operator<<(std::ostream& os, const XattrSupport& xattrSupport);
std::string to_string(const XattrSupport& xattrSupport);

/**
 * Test fixture for Extended Attribute (XATTR) tests.
 * Parameterized on:
 * - TransportProtocol (IPv4, Ipv6); (Plain, SSL);
 * - XATTR On/Off
 * - Client JSON On/Off.
 * - Client SNAPPY On/Off.
 */
class TestappXattrClientTest : public TestappTest,
                               public ::testing::WithParamInterface<
                                       ::testing::tuple<TransportProtocols,
                                                        XattrSupport,
                                                        ClientJSONSupport,
                                                        ClientSnappySupport>> {
public:
    static void SetUpTestCase();

protected:
    TestappXattrClientTest() = default;

    void SetUp() override;

    /**
     * Create an extended attribute
     *
     * @param path the full path to the attribute (including the key)
     * @param value The value to store
     * @param macro is this a macro for expansion or not
     * @param expectedStatus optional status if success is not expected
     */
    void runCreateXattr(std::string path,
                        std::string value,
                        bool macro,
                        cb::mcbp::Status expectedStatus);
    void createXattr(const std::string& path,
                     const std::string& value,
                     bool macro = false);

    /**
     * Get an extended attribute
     *
     * @param path the full path to the attribute to fetch
     * @param deleted allow get from deleted documents
     * @param expectedStatus optional status if success is not expected
     * @return the value stored for the key (it is expected to be there!)
     */
    BinprotSubdocResponse runGetXattr(std::string path,
                                      bool deleted,
                                      cb::mcbp::Status expectedStatus);

    BinprotSubdocResponse getXattr(const std::string& path,
                                   bool deleted = false);

    bool isTlsEnabled() const override;
    ClientJSONSupport hasJSONSupport() const override;
    ClientSnappySupport hasSnappySupport() const override;

    // What response datatype do we expect for documents which are JSON?
    // Will be JSON only if the client successfully negotiated JSON feature.
    cb::mcbp::Datatype expectedJSONDatatype() const;

    /**
     * What response datatype do we expect for documents which are JSON and
     * were stored as Snappy if client supports it?
     * Will be:
     * - JSON only if the client successfully negotiated JSON feature
     *   without Snappy.
     * - JSON+Snappy if client negotiated both JSON and Snappy (and
     *   sent a compressed document).
     */
    cb::mcbp::Datatype expectedJSONSnappyDatatype() const;

    /**
     * Helper function to check datatype is what we expect for this test config;
     * and if datatype says JSON, validate the value /is/ JSON.
     */
    static ::testing::AssertionResult hasCorrectDatatype(
            const Document& doc, cb::mcbp::Datatype expectedType);

    static ::testing::AssertionResult hasCorrectDatatype(
            cb::mcbp::Datatype expectedType,
            cb::mcbp::Datatype actualType,
            std::string_view value);

    /**
     * Replaces document `name` with a document containing the given
     * body and XATTRs.
     *
     * @param xattrList list of XATTR key / value pairs to store.
     * @param compressValue Should the value be compress before being sent?
     */
    void setBodyAndXattr(
            const std::string& startValue,
            std::initializer_list<std::pair<const std::string, std::string>>
                    xattrList,
            bool compressValue);

    /**
     * Replaces document `name` with a document containing the given
     * body and XATTRs.
     * If Snappy support is available (hasSnappySupport), will store as a
     * compressed document.
     * @param xattrList list of XATTR key / value pairs to store.
     */
    void setBodyAndXattr(
            const std::string& value,
            std::initializer_list<std::pair<const std::string, std::string>>
                    xattrList);

    /// Perform the specified subdoc command; returning the response.
    BinprotSubdocResponse subdoc(
            cb::mcbp::ClientOpcode opcode,
            const std::string& key,
            const std::string& path,
            const std::string& value = {},
            cb::mcbp::subdoc::PathFlag flag = {},
            cb::mcbp::subdoc::DocFlag docFlag = cb::mcbp::subdoc::DocFlag::None,
            const std::optional<cb::durability::Requirements>& durReqs = {});

    /// Perform the specified subdoc multi-mutation command; returning the
    /// response.
    static BinprotSubdocResponse subdocMultiMutation(
            const BinprotSubdocMultiMutationCommand& cmd);

    cb::mcbp::Status xattr_upsert(const std::string& path,
                                  const std::string& value);

protected:
    Document document;
    cb::mcbp::Status xattrOperationStatus = cb::mcbp::Status::Success;
};

struct PrintToStringCombinedName {
    std::string operator()(const ::testing::TestParamInfo<
                           ::testing::tuple<TransportProtocols,
                                            XattrSupport,
                                            ClientJSONSupport,
                                            ClientSnappySupport>>& info) const;
};
