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

#include <mcbp/codec/range_scan_continue_codec.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/storeddockey.h>
#include <platform/base64.h>
#include <platform/compress.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <utilities/test_manifest.h>

#include <unordered_set>

class RangeScanTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        TestappXattrClientTest::SetUp();

        // We will be using collections directly
        userConnection->setFeature(cb::mcbp::Feature::Collections, true);

        // Ensure vb is active as some tests change this
        adminConnection->executeInBucket(bucketName, [](auto& connection) {
            connection.setVbucket(Vbid(0), vbucket_state_active, {});
        });

        // Create a new collection for the test
        if (!manifest) {
            // There's a static init fiasco to be fixed with the test code so
            // create on demand
            manifest = std::make_unique<CollectionsManifest>();
        }

        if (!manifest->exists(
                    CollectionEntry::Entry{"RangeScanTest", collectionId})) {
            ++collectionId;
            manifest->add(
                    CollectionEntry::Entry{"RangeScanTest", collectionId});

            adminConnection->executeInBucket(bucketName, [this](auto& conn) {
                auto response = conn.execute(BinprotGenericCommand{
                        cb::mcbp::ClientOpcode::CollectionsSetManifest,
                        {},
                        std::string{*manifest}});
                EXPECT_TRUE(response.isSuccess()) << response.getStatus();
            });
        }

        auto mInfo = storeTestKeys();

        start = cb::base64::encode("user", false);
        end = cb::base64::encode("user\xFF", false);

        // if snappy evict so values comes from disk and we can validate snappy
        if (::testing::get<3>(GetParam()) == ClientSnappySupport::Yes) {
            adminConnection->executeInBucket(bucketName, [this](auto& conn) {
                conn.setFeature(cb::mcbp::Feature::Collections, true);
                for (const auto& key : userKeys) {
                    StoredDocKey collectionKey{key, CollectionID{collectionId}};
                    conn.evict(std::string{collectionKey}, Vbid(0));
                }
            });
        }
        // Reduce the max-scan lifetime to give coverage of the dynamic change
        adminConnection->executeInBucket(bucketName, [&](auto& connection) {
            // Encode a set_flush_param (like cbepctl)
            BinprotGenericCommand cmd1{cb::mcbp::ClientOpcode::SetParam,
                                       "range_scan_max_lifetime",
                                       "60"};
            cmd1.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
                    cb::mcbp::request::SetParamPayload::Type::Flush)));

            const auto resp1 = connection.execute(cmd1);
            ASSERT_EQ(cb::mcbp::Status::Success, resp1.getStatus());

            BinprotGenericCommand cmd2{cb::mcbp::ClientOpcode::SetParam,
                                       "range_scan_read_buffer_send_size",
                                       "8192"};
            cmd2.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
                    cb::mcbp::request::SetParamPayload::Type::Flush)));
            const auto resp2 = connection.execute(cmd1);
            ASSERT_EQ(cb::mcbp::Status::Success, resp2.getStatus());

            EXPECT_EQ("60", connection.statsMap("range-scans")["max_duration"]);
        });

        // add a name to all scans, the server limits the name length so we
        // create a trimmed name here, which is still useful enough to line up
        // failures in log files.
        std::string name =
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
        name.resize(cb::rangescan::MaximumNameSize);

        // Setup to scan for user prefixed docs. Utilise wait_for_seqno so the
        // tests should be stable (have data ready to scan)
        config = {{"range", {{"start", start}, {"end", end}}},
                  {"collection", fmt::format("{0:x}", collectionId)},
                  {"name", name},
                  {"snapshot_requirements",
                   {{"seqno", mInfo.seqno},
                    {"vb_uuid", std::to_string(mInfo.vbucketuuid)},
                    {"timeout_ms", 120000}}}};
    }

    // All successful scans are scanning for these keys
    std::unordered_set<std::string> userKeys = {"user-alan",
                                                "useralan",
                                                "user.claire",
                                                "user::zoe",
                                                "user:aaaaaaaa",
                                                "users"};
    std::unordered_map<StoredDocKey, GetMetaResponse> userKeysMeta;

    // Other keys that get stored in the bucket
    std::vector<std::string> otherKeys = {
            "useq", "uses", "abcd", "uuu", "uuuu", "xyz"};

    std::unordered_set<std::string> sequential = {"0", "1", "2", "3"};

    MutationInfo storeTestKeys() {
        // collection changes per test (as it can get dropped/recreated)
        std::vector<std::pair<StoredDocKey, std::string>> keysForTest;
        for (const auto& key : userKeys) {
            keysForTest.emplace_back(
                    StoredDocKey{key, CollectionID{collectionId}}, key);
        }

        for (const auto& kv : keysForTest) {
            nlohmann::json value = {{"key", kv.second}};
            // MB-55225: For Snappy::No tests write the document compressed.
            // This ensures when the range scan runs without snappy - we see can
            // validate that no snappy value comes back.
            store_document(std::string{kv.first},
                           value.dump(),
                           docFlags /* non-zero flags */,
                           0,
                           hasSnappySupport() == ClientSnappySupport::No);
        }

        for (const auto& kv : keysForTest) {
            // So we can validate that RangeScan meta matches GetMeta
            auto meta = userConnection->getMeta(
                    std::string{kv.first}, Vbid(0), GetMetaVersion::V2);
            EXPECT_EQ(cb::mcbp::Status::Success, meta.first);
            auto [itr, emplaced] =
                    userKeysMeta.try_emplace(kv.first, meta.second);
            EXPECT_TRUE(emplaced);
        }

        for (const auto& key : otherKeys) {
            StoredDocKey collectionKey(key, CollectionID{collectionId});
            store_document(std::string{collectionKey}, key);
        }

        for (const auto& key : sequential) {
            StoredDocKey collectionKey(key, CollectionID{collectionId});
            store_document(std::string{collectionKey}, key);
        }

        Document doc;
        StoredDocKey finalKey("final", CollectionID{collectionId});
        doc.value = "persist me";
        doc.info.id = std::string{finalKey};
        return userConnection->mutate(doc, Vbid(0), MutationType::Set);
    }

    size_t drainKeyResponse(
            const BinprotResponse& response,
            const std::unordered_set<std::string>& expectedKeySet);

    size_t drainItemResponse(
            const BinprotResponse& response,
            const std::unordered_set<std::string>& expectedKeySet);
    struct ScanCounters {
        size_t records{0};
        size_t frames{0};
        size_t continuesIssued{0};
    };
    ScanCounters drainScan(
            cb::rangescan::Id id,
            bool keyScan,
            size_t itemLimit,
            const std::unordered_set<std::string>& expectedKeySet);

    void testErrorsDuringContinue(cb::mcbp::Status error);

    void smallBufferTest(size_t itemLimit, size_t expectedContinues);

    const uint32_t docFlags = 0xAABBCCDD;
    std::string start;
    std::string end;
    nlohmann::json config;
    std::unordered_set<std::string> allKeys;
    static std::unique_ptr<CollectionsManifest> manifest;
    static uint32_t collectionId;
};

std::unique_ptr<CollectionsManifest> RangeScanTest::manifest;
uint32_t RangeScanTest::collectionId = 8;

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
    auto resp = userConnection->execute(create);
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
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // JSON but no datatype
    cmd.setValue(config.dump());
    cmd.setDatatype(cb::mcbp::Datatype::Raw);
    userConnection->sendCommand(cmd);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // JSON but invalid UUID (should only be numeric) - will disconnect client.
    config["snapshot_requirements"]["vb_uuid"] = "123abc";
    cmd.setValue(config.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    userConnection->sendCommand(cmd);
    try {
        userConnection->recvResponse(resp);
        FAIL() << "Expected connection to be closed after invalid UUID";
    } catch (std::system_error& e) {
        EXPECT_EQ(std::errc::connection_reset, e.code());
    }
    // Reset userConnection so it reconnects in next test.
    userConnection.reset();
}

TEST_P(RangeScanTest, CreateKeyExceedMaxLength) {
    // fill construct some keys. s is invalid, e is not
    std::string s(251, 'L');
    std::string e(250, 'L');

    // Exceed the 250 byte limit (the non-encoded size is what matters)
    auto keyInvalid = cb::base64::encode(s, false);
    auto keyValid = cb::base64::encode(e, false);
    BinprotRangeScanCreate create1(
            Vbid(0),
            nlohmann::json{
                    {"range", {{"start", keyInvalid}, {"end", keyValid}}}});
    userConnection->sendCommand(create1);
    BinprotResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    EXPECT_EQ("invalid key size in range start:251, end:250",
              resp.getErrorContext());

    // flip input and expect failure
    BinprotRangeScanCreate create2(
            Vbid(0),
            nlohmann::json{
                    {"range", {{"start", keyValid}, {"end", keyInvalid}}}});
    userConnection->sendCommand(create2);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    EXPECT_EQ("invalid key size in range start:250, end:251",
              resp.getErrorContext());

    BinprotRangeScanCreate create3(
            Vbid(0),
            nlohmann::json{
                    {"range", {{"start", keyInvalid}, {"end", keyInvalid}}}});
    userConnection->sendCommand(create3);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    EXPECT_EQ("invalid key size in range start:251, end:251",
              resp.getErrorContext());

    // Now both valid, maximum keys
    BinprotRangeScanCreate create4(
            Vbid(0),
            nlohmann::json{
                    {"range", {{"start", keyValid}, {"end", keyValid}}}});
    userConnection->sendCommand(create4);
    userConnection->recvResponse(resp);

    // NotFound because the scan range is empty (but was legal)
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());
    EXPECT_NE(std::string::npos,
              resp.getErrorContext().find("no keys in range"));
}

TEST_P(RangeScanTest, CreateCancel) {
    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = userConnection->execute(create);

    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getDataView().data(), resp.getDataView().size());

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
    auto resp = userConnection->execute(create);
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());
}

size_t RangeScanTest::drainKeyResponse(
        const BinprotResponse& response,
        const std::unordered_set<std::string>& expectedKeySet) {
    if (response.getDataView().empty()) {
        return 0;
    }

    EXPECT_EQ(4, response.getExtrasView().size());
    const auto* extras = reinterpret_cast<
            const cb::mcbp::response::RangeScanContinueResponseExtras*>(
            response.getExtrasView().data());
    EXPECT_EQ(
            cb::mcbp::response::RangeScanContinueResponseExtras::Flags::KeyScan,
            extras->getFlags());

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
        const std::unordered_set<std::string>& expectedKeySet) {
    if (response.getDataView().empty()) {
        return 0;
    }

    EXPECT_EQ(4, response.getExtrasView().size());
    const auto* extras = reinterpret_cast<
            const cb::mcbp::response::RangeScanContinueResponseExtras*>(
            response.getExtrasView().data());
    EXPECT_EQ(cb::mcbp::response::RangeScanContinueResponseExtras::Flags::
                      ValueScan,
              extras->getFlags());

    size_t count = 0;
    cb::mcbp::response::RangeScanContinueValuePayload payload(
            response.getDataView());

    auto record = payload.next();
    while (record.key.data()) {
        EXPECT_EQ(1, expectedKeySet.count(std::string{record.key}));
        auto [itr, emplaced] = allKeys.emplace(record.key);
        EXPECT_TRUE(emplaced) << "Duplicate key returned " << record.key;
        std::string value;

        if (cb::mcbp::datatype::is_snappy(record.meta.getDatatype())) {
            EXPECT_TRUE(hasSnappySupport() == ClientSnappySupport::Yes)
                    << "Snappy is disabled but a RangeScan value is "
                    << "snappy compressed";
            cb::compression::Buffer buffer;
            EXPECT_TRUE(cb::compression::inflateSnappy(record.value, buffer));
            value = std::string{std::string_view{buffer}};
        } else {
            value = record.value;
        }
        nlohmann::json jsonValue = nlohmann::json::parse(value);
        EXPECT_EQ(1, expectedKeySet.count(jsonValue["key"]))
                << jsonValue["key"];

        // Check meta matches
        StoredDocKey collectionKey(record.key, CollectionID{collectionId});
        const auto& meta = userKeysMeta.at(collectionKey);
        EXPECT_EQ(meta.flags, record.meta.getFlags());
        EXPECT_EQ(meta.expiry, record.meta.getExpiry());
        // compare and ignore snappy as it varies based on test and where the
        // value came from. Above we validate that no snappy was present for
        // the none snappy test.
        EXPECT_EQ(meta.datatype & ~PROTOCOL_BINARY_DATATYPE_SNAPPY,
                  record.meta.getDatatype() & ~PROTOCOL_BINARY_DATATYPE_SNAPPY);

        record = payload.next();
        ++count;
    }
    return count;
}

RangeScanTest::ScanCounters RangeScanTest::drainScan(
        cb::rangescan::Id id,
        bool keyScan,
        size_t itemLimit,
        const std::unordered_set<std::string>& expectedKeySet) {
    BinprotResponse resp;
    size_t recordsReturned = 0;
    size_t frames = 0;
    size_t continuesIssued = 0;

    do {
        // Keep sending continue until we get the response with complete
        BinprotRangeScanContinue scanContinue(
                Vbid(0),
                id,
                itemLimit,
                std::chrono::milliseconds(0) /* no time limit*/,
                0 /*no byte limit*/);
        userConnection->sendCommand(scanContinue);
        ++continuesIssued;

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

            ++frames;
            if (resp.getStatus() != cb::mcbp::Status::Success) {
                // Stop this loop once !success is seen
                break;
            }
        }

        // Don't expect any errors in this scan. It should return only the
        // following status codes for each response.
        if (!(resp.getStatus() == cb::mcbp::Status::Success ||
              resp.getStatus() == cb::mcbp::Status::RangeScanMore ||
              resp.getStatus() == cb::mcbp::Status::RangeScanComplete)) {
            // stop the loop or it could run for ever
            throw std::runtime_error(
                    "RangeScanTest::drainScan unexpected status:" +
                    to_string(resp.getStatus()));
        }
    } while (resp.getStatus() != cb::mcbp::Status::RangeScanComplete);

    return ScanCounters{recordsReturned, frames, continuesIssued};
}

TEST_P(RangeScanTest, KeyOnly) {
    config["key_only"] = true;

    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = userConnection->execute(create);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getDataView().data(), resp.getDataView().size());

    EXPECT_EQ(userKeys.size(),
              drainScan(id, true, 2, userKeys).records); // 2 items per continue
}

TEST_P(RangeScanTest, ValueScan) {
    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = userConnection->execute(create);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getDataView().data(), resp.getDataView().size());
    EXPECT_EQ(
            userKeys.size(),
            drainScan(id, false, 2, userKeys).records); // 2 items per continue
}

// Set the buffer to be 0 and check that each key is sent in a single mcbp
// response (frames).
void RangeScanTest::smallBufferTest(size_t itemLimit,
                                    size_t expectedContinues) {
    // Reduce the buffer size so each read triggers a yield
    adminConnection->executeInBucket(bucketName, [&](auto& connection) {
        // Encode a set_flush_param (like cbepctl)
        BinprotGenericCommand cmd1{cb::mcbp::ClientOpcode::SetParam,
                                   "range_scan_read_buffer_send_size",
                                   "0"};
        cmd1.setExtrasValue<uint32_t>(htonl(static_cast<uint32_t>(
                cb::mcbp::request::SetParamPayload::Type::Flush)));
        const auto resp = connection.execute(cmd1);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });

    config["key_only"] = true;

    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = userConnection->execute(create);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getDataView().data(), resp.getDataView().size());

    // The 0 byte internal buffer means that every key read triggers a mcbp
    // response (frame) and there is 1 extra frame that contains the final
    // complete status.
    auto result = drainScan(id, true, itemLimit, userKeys);
    EXPECT_EQ(expectedContinues, result.continuesIssued);
    EXPECT_EQ(userKeys.size() + 1, result.frames);
    EXPECT_EQ(userKeys.size(), result.records);
}

TEST_P(RangeScanTest, ScanWithSmallBufferNoLimit) {
    smallBufferTest(0 /* no limit*/, 1 /* 1 continue expected */);
}

TEST_P(RangeScanTest, ScanWithSmallBufferWithLimit) {
    // For MB-57350 test with an item limit. Prior to fixing, the scan would run
    // to completion with one continue and ignore the limit.
    // The test expects (x / 2) + 1 continues - the 1 extra is required because
    // the scan happens to stop after reading the last key, but won't know that
    // it's the last key until 1 extra continue pushes the scan over to the end.
    smallBufferTest(2, (userKeys.size() / 2) + 1);
}

TEST_P(RangeScanTest, ExclusiveRangeStart) {
    config["key_only"] = true;
    // 1, 2, 3
    auto start = cb::base64::encode("0", false);
    auto end = cb::base64::encode("3", false);
    config["range"] = {{"excl_start", start}, {"end", end}};

    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = userConnection->execute(create);

    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getDataView().data(), resp.getDataView().size());
    EXPECT_EQ(3, sequential.size() - 1);
    EXPECT_EQ(
            3,
            drainScan(id, true, 2, sequential).records); // 2 items per continue
}

TEST_P(RangeScanTest, ExclusiveRangeEnd) {
    config["key_only"] = true;

    // 0, 1, 2
    auto start = cb::base64::encode("0", false);
    auto end = cb::base64::encode("3", false);
    config["range"] = {{"start", start}, {"excl_end", end}};

    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = userConnection->execute(create);

    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getDataView().data(), resp.getDataView().size());
    EXPECT_EQ(3, sequential.size() - 1);
    EXPECT_EQ(
            3,
            drainScan(id, true, 2, sequential).records); // 2 items per continue
}

TEST_P(RangeScanTest, TestStats) {
    BinprotGenericCommand create(
            cb::mcbp::ClientOpcode::RangeScanCreate, {}, config.dump());
    create.setDatatype(cb::mcbp::Datatype::JSON);
    auto resp = userConnection->execute(create);

    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // Nothing in vbid1
    auto stats = userConnection->stats("range-scans 1");
    EXPECT_TRUE(stats.empty());

    // Scan in vbid0
    stats = userConnection->stats("range-scans 0");
    EXPECT_FALSE(stats.empty());

    auto statsAll = userConnection->stats("range-scans");

    // Clear the time field so we can compare.
    stats["current_time"] = "0";
    statsAll["current_time"] = "0";

    // With only one vb in use, all == vbid0
    EXPECT_EQ(stats, statsAll);
}

// This test case is not ideal as it doesn't guarantee that we will force the
// error to occur whilst the continue is executing (which was the source of the
// MB that lead to this test being added)
void RangeScanTest::testErrorsDuringContinue(cb::mcbp::Status error) {
    BinprotRangeScanCreate create(Vbid(0), config);
    auto resp = userConnection->execute(create);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getDataView().data(), resp.getDataView().size());

    BinprotRangeScanContinue scanContinue(
            Vbid(0),
            id,
            2,
            std::chrono::milliseconds(0) /* no time limit*/,
            0 /*no byte limit*/);
    userConnection->sendCommand(scanContinue);

    // On the admin connection, make a change to force an error path
    if (error == cb::mcbp::Status::NotMyVbucket) {
        // Drop the VB
        adminConnection->executeInBucket(bucketName, [](auto& connection) {
            connection.setVbucket(Vbid(0), vbucket_state_replica, {});
        });
    } else if (error == cb::mcbp::Status::NoBucket) {
        // Drop the B
        adminConnection->deleteBucket(bucketName);
    } else if (error == cb::mcbp::Status::UnknownCollection) {
        // Drop the collection
        manifest->remove(CollectionEntry::Entry{"RangeScanTest", collectionId});
        adminConnection->executeInBucket(bucketName, [this](auto& conn) {
            auto response = conn.execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::CollectionsSetManifest,
                    {},
                    std::string{*manifest}});

            // The manifest sticks, so if we set it again with the same uid,
            // that is invalid, just assert erange and carry on
            if (!response.isSuccess()) {
                ASSERT_EQ(cb::mcbp::Status::Erange, response.getStatus());
            }
        });
    } else if (error == cb::mcbp::Status::RangeScanCancelled) {
        adminConnection->executeInBucket(bucketName, [&id](auto& conn) {
            BinprotRangeScanCancel cancel(Vbid(0), id);
            auto resp = conn.execute(cancel);
            ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        });
    } else {
        FAIL() << error;
    }

    // Drain the scan to completion.
    // 1) Continue detects error immediately
    // 2) Continue detects error during the scan - so we may see a mix of
    ///   success{key}, range-scan-more{key}, error
    /// 3) Continue runs to completion no error is seen
    bool scanCanContinue = true;
    do {
        userConnection->recvResponse(resp);

        if (resp.getStatus() == cb::mcbp::Status::NotMyVbucket) {
            EXPECT_EQ(resp.getStatus(), error);
            // Expect no keys/values attached to this error. A cluster would
            // attach vbmap
            ASSERT_TRUE(resp.getDataView().empty());
            scanCanContinue = false;
        } else if (resp.getStatus() == cb::mcbp::Status::UnknownCollection) {
            EXPECT_EQ(resp.getStatus(), error);

            scanCanContinue = false;
            // Expect to find the collection manifest id
            nlohmann::json parsed;
            try {
                parsed = resp.getDataJson();
            } catch (const nlohmann::json::exception& e) {
                FAIL() << "Cannot parse json resp:" << resp.getDataView()
                       << " e:" << e.what();
            }

            auto itr = parsed.find("manifest_uid");
            EXPECT_NE(parsed.end(), itr);
            EXPECT_EQ(manifest->getUidString(), itr->get<std::string>());
        } else if (resp.getStatus() == cb::mcbp::Status::RangeScanCancelled) {
            EXPECT_EQ(resp.getStatus(), error);

            // Expect no keys/values attached to this error.
            ASSERT_TRUE(resp.getDataView().empty());
            scanCanContinue = false;
        } else if (resp.getStatus() == cb::mcbp::Status::RangeScanMore) {
            // range-scan-more
            userConnection->sendCommand(BinprotRangeScanContinue(
                    Vbid(0),
                    id,
                    2, // 2 keys per continue
                    std::chrono::milliseconds(0) /* no time limit*/,
                    0 /*no byte limit*/));
        } else if (resp.getStatus() == cb::mcbp::Status::Success) {
            // keys/values - next packet should be status
            ASSERT_NE(0, resp.getDataView().size());
        } else if (resp.getStatus() == cb::mcbp::Status::RangeScanComplete ||
                   resp.getStatus() == cb::mcbp::Status::KeyEnoent) {
            scanCanContinue = false;
        } else {
            FAIL() << "Unexpected/unhandled status code " << resp.getStatus();
        }
    } while (scanCanContinue);

    // And now cancel which fails but ensures no other responses were in the
    // pipe. Prior to fixing the bug, a second NMVB/UnknownCollection/Cancel
    // may be sent which would be detected here rather than KeyEnoent
    BinprotRangeScanCancel cancel(Vbid(0), id);
    userConnection->sendCommand(cancel);
    userConnection->recvResponse(resp);

    // The scan is either cancelled (KeyEnoent) or still exists (Success)- note
    // that it is on its way to "naturally" cancel, but it may not disappear
    // from the map of available scans until the I/O task gets to modify that
    // map - which happens after it has finished transmitting the scan state.
    ASSERT_TRUE(cb::mcbp::Status::KeyEnoent == resp.getStatus() ||
                cb::mcbp::Status::Success == resp.getStatus());
}

TEST_P(RangeScanTest, ErrorNMVB) {
    testErrorsDuringContinue(cb::mcbp::Status::NotMyVbucket);
}
TEST_P(RangeScanTest, ErrorUnknownCollection) {
    testErrorsDuringContinue(cb::mcbp::Status::UnknownCollection);
}
TEST_P(RangeScanTest, ErrorRangeScanCancelled) {
    testErrorsDuringContinue(cb::mcbp::Status::RangeScanCancelled);
}
TEST_P(RangeScanTest, ErrorNoBucket) {
    try {
        testErrorsDuringContinue(cb::mcbp::Status::NoBucket);
    } catch (const std::exception& e) {
        // The delete test can trigger disconnect and a reset error
#ifdef _WINDOWS_
        std::string_view expectedResetString = "connection reset";
#else
        std::string_view expectedResetString = "reset by peer";
#endif
        ASSERT_GT(strlen(e.what()), expectedResetString.size());
        EXPECT_TRUE(strstr(e.what(), expectedResetString.data())) << e.what();
    }
    // Re-create the bucket for any subsequent tests
    mcd_env->getTestBucket().setUpBucket(bucketName, "", *adminConnection);
    rebuildUserConnection(isTlsEnabled());
    // Delete this so that collections are re-created
    manifest.reset();
}
