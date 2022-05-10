/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <protocol/connection/client_mcbp_commands.h>

#include <xattr/blob.h>
#include <xattr/utils.h>

#include <mcbp/codec/range_scan_continue_codec.h>
#include <memcached/range_scan_id.h>
#include <platform/base64.h>

#include <unordered_set>

class RangeScanTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        if (!mcd_env->getTestBucket().supportsRangeScans()) {
            GTEST_SKIP();
        }
        TestappXattrClientTest::SetUp();
        auto mInfo = storeTestKeys();

        start = cb::base64::encode("user", false);
        end = cb::base64::encode("user\xFF", false);

        // if snappy evict so values comes from disk and we can validate snappy
        if (::testing::get<3>(GetParam()) == ClientSnappySupport::Yes) {
            adminConnection->executeInBucket(
                    bucketName, [this](auto& connection) {
                        for (const auto& key : userKeys) {
                            connection.evict(key, Vbid(0));
                        }
                    });
        }

        // Setup to scan for user prefixed docs. Utilise wait_for_seqno so the
        // tests should be stable (have data ready to scan)
        config = {{"range", {{"start", start}, {"end", end}}},
                  {"snapshot_requirements",
                   {{"seqno", mInfo.seqno},
                    {"vb_uuid", mInfo.vbucketuuid},
                    {"timeout_ms", 120000}}}};
    }

    // All successful scans are scanning for these keys
    std::unordered_set<std::string> userKeys = {"user-alan",
                                                "useralan",
                                                "user.claire",
                                                "user::zoe",
                                                "user:aaaaaaaa",
                                                "users"};
    // Other keys that get stored in the bucket
    std::vector<std::string> otherKeys = {
            "useq", "uses", "abcd", "uuu", "uuuu", "xyz"};

    std::unordered_set<std::string> sequential = {"0", "1", "2", "3"};

    MutationInfo storeTestKeys() {
        for (const auto& key : userKeys) {
            store_document(key, key);
        }

        for (const auto& key : otherKeys) {
            store_document(key, key);
        }

        for (const auto& key : sequential) {
            store_document(key, key);
        }

        Document doc;
        doc.value = "persist me";
        doc.info.id = "final";
        return userConnection->mutate(doc, Vbid(0), MutationType::Set);
    }

    size_t drainKeyResponse(
            const BinprotResponse& response,
            const std::unordered_set<std::string> expectedKeySet);

    size_t drainItemResponse(
            const BinprotResponse& response,
            const std::unordered_set<std::string> expectedKeySet);

    size_t drainScan(cb::rangescan::Id id,
                     bool keyScan,
                     size_t itemLimit,
                     const std::unordered_set<std::string> expectedKeySet);

    std::string start;
    std::string end;
    nlohmann::json config;
    std::unordered_set<std::string> allKeys;
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        RangeScanTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

// Create one range scan which we leave, this gives test coverage of shutdown
// whilst we have a snapshot open (have seen crashes/destruct issues)
TEST_P(RangeScanTest, CreateAndLeave) {
    BinprotRangeScanCreate create(Vbid(0), config);
    userConnection->sendCommand(create);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

TEST_P(RangeScanTest, CreateInvalid) {
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::RangeScanCreate);
    userConnection->sendCommand(cmd);
    BinprotResponse resp;
    userConnection->recvResponse(resp);
    // No value, so invalid
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // Not JSON
    cmd.setValue("...");
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    userConnection->sendCommand(cmd);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // JSON but no datatype
    cmd.setValue(config.dump());
    cmd.setDatatype(cb::mcbp::Datatype::Raw);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(RangeScanTest, CreateCancel) {
    BinprotRangeScanCreate create(Vbid(0), config);
    userConnection->sendCommand(create);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());

    BinprotRangeScanCancel cancel(Vbid(0), id);
    userConnection->sendCommand(cancel);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // Cancel again will fail.
    userConnection->sendCommand(cancel);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());
}

// Empty range fails at the create point
TEST_P(RangeScanTest, CreateEmpty) {
    auto start = cb::base64::encode("L", false);
    auto end = cb::base64::encode("M\xFF", false);
    nlohmann::json emptyRange = {{"range", {{"start", start}, {"end", end}}}};
    BinprotRangeScanCreate create(Vbid(0), emptyRange);
    userConnection->sendCommand(create);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());
}

size_t RangeScanTest::drainKeyResponse(
        const BinprotResponse& response,
        const std::unordered_set<std::string> expectedKeySet) {
    if (response.getDataView().empty()) {
        return 0;
    }

    size_t count = 0;
    cb::mcbp::response::RangeScanContinueKeyPayload payload(
            response.getDataView());

    auto key = payload.next();
    while (key.data()) {
        EXPECT_EQ(1, expectedKeySet.count(std::string{key})) << key;
        auto [itr, emplaced] = allKeys.emplace(key);
        EXPECT_TRUE(emplaced) << "Duplicate key returned " << key;
        key = payload.next();

        ++count;
    }
    return count;
}

size_t RangeScanTest::drainItemResponse(
        const BinprotResponse& response,
        const std::unordered_set<std::string> expectedKeySet) {
    if (response.getDataView().empty()) {
        return 0;
    }

    size_t count = 0;
    cb::mcbp::response::RangeScanContinueValuePayload payload(
            response.getDataView());

    auto record = payload.next();
    while (record.key.data()) {
        EXPECT_EQ(1, expectedKeySet.count(std::string{record.key}));
        auto [itr, emplaced] = allKeys.emplace(record.key);
        EXPECT_TRUE(emplaced) << "Duplicate key returned " << record.key;
        if (mcbp::datatype::is_snappy(record.meta.getDatatype())) {
            cb::compression::Buffer buffer;
            EXPECT_TRUE(cb::compression::inflate(
                    cb::compression::Algorithm::Snappy, record.value, buffer));
            EXPECT_EQ(
                    1,
                    expectedKeySet.count(std::string{std::string_view{buffer}}))
                    << record.key;
        } else {
            EXPECT_EQ(1, expectedKeySet.count(std::string{record.value}));
        }

        record = payload.next();
        ++count;
    }
    return count;
}

size_t RangeScanTest::drainScan(
        cb::rangescan::Id id,
        bool keyScan,
        size_t itemLimit,
        const std::unordered_set<std::string> expectedKeySet) {
    BinprotResponse resp;
    size_t recordsReturned = 0;
    do {
        // Keep sending continue until we get the response with complete
        BinprotRangeScanContinue scanContinue(
                Vbid(0), id, itemLimit, std::chrono::milliseconds(0));
        userConnection->sendCommand(scanContinue);

        // Keep receiving responses until the sequence ends (!success)
        while (true) {
            userConnection->recvResponse(resp);

            // An error or more/complete status may carry keys/values, so try
            // and drain the payload
            if (keyScan) {
                recordsReturned += drainKeyResponse(resp, expectedKeySet);
            } else {
                recordsReturned += drainItemResponse(resp, expectedKeySet);
            }

            if (resp.getStatus() != cb::mcbp::Status::Success) {
                // Stop this loop once !success is seen
                break;
            }
        }

        // Don't expect any errors in this scan. It should return only the
        // following status codes for each response.
        EXPECT_TRUE(resp.getStatus() == cb::mcbp::Status::Success ||
                    resp.getStatus() == cb::mcbp::Status::RangeScanMore ||
                    resp.getStatus() == cb::mcbp::Status::RangeScanComplete)
                << resp.getStatus();
    } while (resp.getStatus() != cb::mcbp::Status::RangeScanComplete);
    return recordsReturned;
}

TEST_P(RangeScanTest, KeyOnly) {
    config["key_only"] = true;

    BinprotRangeScanCreate create(Vbid(0), config);
    userConnection->sendCommand(create);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());

    EXPECT_EQ(userKeys.size(),
              drainScan(id, true, 2, userKeys)); // 2 items per continue
}

TEST_P(RangeScanTest, ValueScan) {
    BinprotRangeScanCreate create(Vbid(0), config);
    userConnection->sendCommand(create);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());

    EXPECT_EQ(userKeys.size(),
              drainScan(id, false, 2, userKeys)); // 2 items per continue
}

TEST_P(RangeScanTest, ExclusiveRangeStart) {
    config["key_only"] = true;
    // 1, 2, 3
    auto start = cb::base64::encode("0", false);
    auto end = cb::base64::encode("3", false);
    config["range"] = {{"excl_start", start}, {"end", end}};

    BinprotRangeScanCreate create(Vbid(0), config);
    userConnection->sendCommand(create);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());
    EXPECT_EQ(3, sequential.size() - 1);
    EXPECT_EQ(3, drainScan(id, true, 2, sequential)); // 2 items per continue
}

TEST_P(RangeScanTest, ExclusiveRangeEnd) {
    config["key_only"] = true;

    // 0, 1, 2
    auto start = cb::base64::encode("0", false);
    auto end = cb::base64::encode("3", false);
    config["range"] = {{"start", start}, {"excl_end", end}};

    BinprotRangeScanCreate create(Vbid(0), config);
    userConnection->sendCommand(create);

    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());
    EXPECT_EQ(3, sequential.size() - 1);
    EXPECT_EQ(3, drainScan(id, true, 2, sequential)); // 2 items per continue
}
