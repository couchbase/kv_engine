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

#include <memcached/range_scan_id.h>
#include <platform/base64.h>

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

    std::string start;
    std::string end;
    nlohmann::json config;
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
