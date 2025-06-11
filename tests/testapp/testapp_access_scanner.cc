/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"

#include <cbcrypto/file_reader.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/random.h>
#include <platform/uuid.h>

class AccessScannerTest : public TestappClientTest {
public:
    int getNumAccessScannerSkips() {
        int ret = 0;
        userConnection->stats(
                [&ret](auto& k, auto& v) {
                    if (k == "ep_num_access_scanner_skips") {
                        ret = std::stoi(v);
                    }
                },
                "");
        return ret;
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         AccessScannerTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(AccessScannerTest, EmptyDatabase) {
    waitForSnoozedAccessScanner();
    auto shards = getNumShards();
    ASSERT_NE(0, shards);
    auto skipped = getNumAccessScannerSkips();
    ASSERT_EQ(0, skipped);
    rerunAccessScanner();
    skipped = getNumAccessScannerSkips();
    EXPECT_EQ(shards, skipped);

    // Rerun the access scanner, it should skip again as there is no data
    // and update the stats
    rerunAccessScanner();
    skipped = getNumAccessScannerSkips();
    EXPECT_EQ(2 * shards, skipped);

    // There shall be no access log files created
    auto dbpath = mcd_env->getTestBucket().getDbPath();
    std::error_code ec;
    for (const auto& p :
         std::filesystem::directory_iterator(dbpath / bucketName, ec)) {
        EXPECT_EQ(std::string::npos,
                  p.path().filename().string().find("access_log"))
                << "Expected no access log files in "
                << (dbpath / bucketName).string()
                << ", but found: " << p.path().filename().string();
    }
}

TEST_P(AccessScannerTest, DatabaseContaingData) {
    waitForSnoozedAccessScanner();
    auto shards = getNumShards();
    ASSERT_NE(0, shards);
    auto skipped = getNumAccessScannerSkips();

    size_t num_docs = populateData();
    rerunAccessScanner();

    EXPECT_EQ(skipped, getNumAccessScannerSkips());

    verifyAccessLogFiles(shards, true, false);

    // Rerun the access scanner. It should not skip anything and rotate
    // the current files to .old.cef and create new files
    rerunAccessScanner();
    EXPECT_EQ(skipped, getNumAccessScannerSkips());

    verifyAccessLogFiles(shards, true, true);

    // Delete the documents.. that should make us "resident" again
    // and rerunning the access scanner should delete the access log files
    for (std::size_t ii = 0; ii < num_docs; ++ii) {
        userConnection->remove(fmt::format("mykey-{}", ii), Vbid(0));
    }
    rerunAccessScanner();
    EXPECT_EQ(skipped + shards, getNumAccessScannerSkips());
    verifyNoAccessLogFiles(shards);
}
