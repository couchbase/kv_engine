/*
 *     Copyright 2024-Present Couchbase, Inc.
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
#include <memcached/config_parser.h>
#include <nlohmann/json.hpp>
#include <platform/random.h>
#include <platform/uuid.h>

class EncryptionTest : public TestappClientTest {
public:
    static void rewriteMemcachedJson(bool encrypted) {
        write_config_to_file(memcached_cfg.dump());

        // Verify that the file was written encypted/plain
        auto reader = cb::crypto::FileReader::create(
                mcd_env->getConfigurationFile(),
                [encrypted](auto id) -> cb::crypto::SharedEncryptionKey {
                    if (encrypted) {
                        return mcd_env->getDekManager().lookup(
                                cb::dek::Entity::Config, id);
                    }
                    throw std::runtime_error(
                            "rewriteMemcachedJson: Plain file should not "
                            "require an encryption key");
                });
        EXPECT_EQ(encrypted, reader->is_encrypted());
    }

    /// Wait for the bucket to only use the proivided key
    void waitForEncryptionKey(std::string_view key_id) {
        const auto timeout =
                std::chrono::steady_clock::now() + std::chrono::seconds{30};

        adminConnection->executeInBucket(bucketName, [](auto& conn) {
            conn.execute(BinprotCompactDbCommand{}, std::chrono::seconds{30});
        });

        while (std::chrono::steady_clock::now() < timeout) {
            nlohmann::json stats;
            adminConnection->executeInBucket(
                    bucketName, [&stats](auto& connection) {
                        connection.stats(
                                [&stats](auto& k, auto& v) {
                                    stats = nlohmann::json::parse(v);
                                },
                                "encryption-key-ids");
                    });
            if (stats.size() != 1 ||
                stats.front().get<std::string>() != key_id) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            } else {
                return;
            }
        }
        throw std::runtime_error(fmt::format(
                "waitForEncryptionKey: Timed out waiting for key {}", key_id));
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         EncryptionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(EncryptionTest, RotateEncryptionKeys) {
    // Rewrite memcached.json with a new key (unknown to memcached) and
    // tell memcached to re-read the file (should fail as it don't have
    // the key)
    auto& manager = mcd_env->getDekManager();
    manager.setActive(cb::dek::Entity::Config,
                      cb::crypto::DataEncryptionKey::generate());
    rewriteMemcachedJson(true);

    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, {}, {}});
    EXPECT_EQ(cb::mcbp::Status::Einternal, rsp.getStatus());

    // Tell memcached of the new key
    rsp = adminConnection->execute(BinprotSetActiveEncryptionKeysCommand{
            format_as(cb::dek::Entity::Config),
            manager.to_json(cb::dek::Entity::Config).dump()});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    // At this time memcached should be able to read the file
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, {}, {}});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    // Disable encryption of the file
    manager.setActive(cb::dek::Entity::Config,
                      cb::crypto::SharedEncryptionKey{});
    rewriteMemcachedJson(false);

    // Memcached should detect that it isn't encrypted and be able
    // to read the file
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, {}, {}});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
}

TEST_P(EncryptionTest, TestAuditAndLogDeksInUse) {
    // @todo the test needs to be updated once we add test cases for key
    //       rotation
    nlohmann::json stats;
    adminConnection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");
    ASSERT_TRUE(stats.contains("@audit"));
    ASSERT_TRUE(stats["@audit"].is_array());
    ASSERT_EQ(stats["@audit"].size(), 1);
    ASSERT_EQ(stats["@audit"].front().get<std::string>(),
              mcd_env->getDekManager().lookup(cb::dek::Entity::Audit)->getId());
    ASSERT_TRUE(stats.contains("@logs"));
    ASSERT_TRUE(stats["@logs"].is_array());
    ASSERT_EQ(stats["@logs"].size(), 1);
    ASSERT_EQ(stats["@logs"].front().get<std::string>(),
              mcd_env->getDekManager().lookup(cb::dek::Entity::Logs)->getId());
}

TEST_P(EncryptionTest, TestEncryptionKeyIds) {
    nlohmann::json stats;
    adminConnection->executeInBucket(bucketName, [&stats](auto& conn) {
        conn.stats([&stats](auto& k,
                            auto& v) { stats = nlohmann::json::parse(v); },
                   "encryption-key-ids");
    });

    // The returned stats is something like:
    //     [
    //         "38f04f51-0f76-47a4-b1aa-23f6e9f909f4",
    //         "0c7a52b5-7de1-4e96-883e-0d42393ce4c4",
    //         "unencrypted"
    //     ]
    EXPECT_FALSE(stats.empty());
    EXPECT_TRUE(stats.is_array()) << stats.dump();
    const auto unencrypted = cb::crypto::DataEncryptionKey().getId();
    for (const auto& key : stats) {
        ASSERT_TRUE(key.is_string()) << stats.dump();
        if (key.get<std::string>() != unencrypted) {
            // verify that it is a UUID (which is what we use in our
            // test framework)
            try {
                cb::uuid::from_string(key.get<std::string>());
            } catch (const std::exception& e) {
                FAIL() << "Encryption key ids must be a UUID: " << e.what();
            }
        }
    }

    stats.clear();
    adminConnection->unselectBucket();
    adminConnection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");
    EXPECT_FALSE(stats.empty());
    EXPECT_TRUE(stats.is_object()) << stats.dump();
    ASSERT_TRUE(stats.contains("@audit"));
    ASSERT_TRUE(stats["@audit"].is_array());
    ASSERT_TRUE(stats.contains("@logs"));
    ASSERT_TRUE(stats["@logs"].is_array());

    // MB-64825: Verify that the stat call still succeed if it spans a RBAC
    // refresh and the connection is bound to "no bucket"
    // See https://jira.issues.couchbase.com/browse/MB-64825
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::RbacRefresh});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    adminConnection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");
    EXPECT_FALSE(stats.empty());
    EXPECT_TRUE(stats.is_object()) << stats.dump();
    ASSERT_TRUE(stats.contains("@audit"));
    ASSERT_TRUE(stats["@audit"].is_array());
    ASSERT_TRUE(stats.contains("@logs"));
    ASSERT_TRUE(stats["@logs"].is_array());
}

TEST_P(EncryptionTest, TestPruneDeks) {
    nlohmann::json stats;
    adminConnection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");
    ASSERT_TRUE(stats.contains("@logs"));
    ASSERT_TRUE(stats["@logs"].is_array());
    ASSERT_EQ(1, stats["@logs"].size());

    // Make sure we have a bunch of keys in use for our logs
    auto& dekManager = mcd_env->getDekManager();
    std::vector<std::string> ids;
    ids.emplace_back(dekManager.lookup(cb::dek::Entity::Logs)->getId());
    for (int ii = 0; ii < 10; ++ii) {
        dekManager.setActive(cb::dek::Entity::Logs,
                             cb::crypto::DataEncryptionKey::generate());
        ids.emplace_back(dekManager.lookup(cb::dek::Entity::Logs)->getId());
        auto rsp = adminConnection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
                format_as(cb::dek::Entity::Logs),
                dekManager.to_json(cb::dek::Entity::Logs).dump()});
        auto clone = adminConnection->clone();
        clone->authenticate("@admin");
    }

    adminConnection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");
    ASSERT_TRUE(stats.contains("@logs"));
    ASSERT_TRUE(stats["@logs"].is_array());
    // verify that we have all the keys we created (note there may be more)
    std::vector<std::string> in_use =
            stats["@logs"].get<std::vector<std::string>>();
    for (const auto& id : ids) {
        EXPECT_TRUE(std::ranges::find(in_use, id) != in_use.end());
    }

    // prune all except the newest
    in_use.erase(std::ranges::find(in_use, ids.back()));
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::PruneEncryptionKeys,
                                  format_as(cb::dek::Entity::Logs),
                                  nlohmann::json(in_use).dump()});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();

    // Verify that we only have the newest active key on all data
    adminConnection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");
    ASSERT_TRUE(stats.contains("@logs"));
    ASSERT_TRUE(stats["@logs"].is_array());
    ASSERT_EQ(stats["@logs"].size(), 1) << stats["@logs"].dump();
    ASSERT_EQ(stats["@logs"].front().get<std::string>(),
              dekManager.lookup(cb::dek::Entity::Logs)->getId());
}

TEST_P(EncryptionTest, TestDisableEncryption) {
    // Verify that the bucket is encrypted
    auto connection = adminConnection->clone();
    connection->authenticate("@admin");
    connection->selectBucket(bucketName);

    nlohmann::json stats;
    connection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");

    ASSERT_EQ(1, stats.size()) << stats.dump();
    ASSERT_NE("unencrypted", stats.front().get<std::string>());

    // Disable encryption
    mcd_env->getTestBucket().keystore.setActiveKey({});
    std::string config =
            nlohmann::json(mcd_env->getTestBucket().keystore).dump();

    auto rsp = connection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
            bucketName,
            config});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    // Compact the vbucket
    rsp = connection->execute(BinprotCompactDbCommand{});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    // fetch the stats and it should be "unencrypted"
    connection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");

    ASSERT_EQ(1, stats.size()) << stats.dump();
    ASSERT_EQ("unencrypted", stats.front().get<std::string>());

    // Turn encryption back on as other tests expect the bucket to be
    // encrypted
    mcd_env->getTestBucket().keystore.setActiveKey(
            cb::crypto::DataEncryptionKey::generate());
    config = nlohmann::json(mcd_env->getTestBucket().keystore).dump();
    rsp = connection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
            bucketName,
            config});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    rsp = connection->execute(BinprotCompactDbCommand{});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    // fetch the stats and it should not be "unencrypted"
    connection->stats(
            [&stats](auto& k, auto& v) { stats = nlohmann::json::parse(v); },
            "encryption-key-ids");

    ASSERT_EQ(1, stats.size()) << stats.dump();
    ASSERT_NE("unencrypted", stats.front().get<std::string>());
}

TEST_P(EncryptionTest, TestAccessScannerRewrite) {
    // Verify that the bucket is encrypted
    auto connection = adminConnection->clone();
    connection->authenticate("@admin");
    connection->selectBucket(bucketName);

    // we won't get an access if we're all resident.
    populateData();

    auto num_shards = getNumShards();
    auto num_runs = waitForSnoozedAccessScanner();

    // Disable encryption and verify that the access log is rewritten
    // unencrypted
    mcd_env->getTestBucket().keystore.setActiveKey({});
    std::string config =
            nlohmann::json(mcd_env->getTestBucket().keystore).dump();

    auto rsp = connection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
            bucketName,
            config});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        conn.execute(BinprotCompactDbCommand{}, std::chrono::seconds{30});
    });

    // Now wait for the access scanner to run and rewrite the access log
    // unencrypted
    waitForEncryptionKey(cb::crypto::DataEncryptionKey::UnencryptedKeyId);
    while (num_runs == waitForSnoozedAccessScanner()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    verifyAccessLogFiles(num_shards, false, false);

    // Rerun the access scanner. This time we should get the .old plain
    // files
    rerunAccessScanner();
    verifyAccessLogFiles(num_shards, false, true);

    // Turn encryption back on and verify that the access log is
    // rewritten encrypted, and the .old files are removed
    cb::crypto::SharedEncryptionKey key =
            cb::crypto::DataEncryptionKey::generate();

    // Turn encryption back on and verify that the access log is rewritten
    // encrypted
    mcd_env->getTestBucket().keystore.setActiveKey(key);
    config = nlohmann::json(mcd_env->getTestBucket().keystore).dump();
    rsp = connection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
            bucketName,
            config});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        conn.execute(BinprotCompactDbCommand{}, std::chrono::seconds{30});
    });

    // Now wait for the access scanner to run and rewrite the access log
    // unencrypted
    waitForEncryptionKey(key->getId());
    while (num_runs == waitForSnoozedAccessScanner()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    verifyAccessLogFiles(num_shards, true, false);
}

TEST_P(EncryptionTest, BucketReloadWhenMissingKeys) {
    // The current bucket should be encrypted
    auto& bucket = mcd_env->getTestBucket();

    // Delete the current bucket
    adminConnection->deleteBucket(bucketName);

    auto config =
            cb::config::filter(bucket.getCreateBucketConfigString(bucketName),
                               [](auto k, auto) -> bool {
                                   if (k == "encryption") {
                                       return false;
                                   }
                                   return true;
                               });

    auto rsp = adminConnection->execute(
            BinprotCreateBucketCommand{bucketName, "ep.so", config});

    ASSERT_EQ(cb::mcbp::Status::EncryptionKeyNotAvailable, rsp.getStatus())
            << "It should not be possible to create a bucket without the keys";

    // Verify that we can recreate the bucket if we provide all buckets
    rsp = adminConnection->execute(BinprotCreateBucketCommand{
            bucketName,
            "ep.so",
            bucket.getCreateBucketConfigString(bucketName)});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();
    // Nuke the bucket and recreate it just like the next tests expects
    // it to be (in a clean state with a replica vbucket etc and enable
    // traffic)
    DeleteTestBucket();
    CreateTestBucket();
}
