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
                [encrypted](auto id) -> cb::crypto::SharedKeyDerivationKey {
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

        compactDb();

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

    BinprotResponse compactDb() {
        BinprotResponse response;
        adminConnection->executeInBucket(bucketName, [&response](auto& conn) {
            response = conn.execute(BinprotCompactDbCommand{},
                                    std::chrono::seconds{30});
        });
        return response;
    }

    /// Test that we can set the encryption keys for the provided entity
    /// when we mark all keys as unavailable
    void testSetEncryptionKeysWithAllUnavailable(
            cb::dek::Entity entity, cb::crypto::SharedKeyDerivationKey key);

    /// Test that we can set the encryption keys the bucket when we mark
    /// all current keys as unavailable
    void testSetEncryptionKeysWithAllUnavailable_bucket(
            cb::crypto::SharedKeyDerivationKey key);
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
                      cb::crypto::KeyDerivationKey::generate());
    rewriteMemcachedJson(true);

    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, {}, {}});
    EXPECT_EQ(cb::mcbp::Status::Einternal, rsp.getStatus());

    // Tell memcached of the new key
    rsp = adminConnection->execute(BinprotSetActiveEncryptionKeysCommand{
            format_as(cb::dek::Entity::Config),
            manager.to_json(cb::dek::Entity::Config)});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    // At this time memcached should be able to read the file
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, {}, {}});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    // Disable encryption of the file
    manager.setActive(cb::dek::Entity::Config, nullptr);
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
              mcd_env->getDekManager().lookup(cb::dek::Entity::Audit)->id);
    ASSERT_TRUE(stats.contains("@logs"));
    ASSERT_TRUE(stats["@logs"].is_array());
    ASSERT_EQ(stats["@logs"].size(), 1);
    ASSERT_EQ(stats["@logs"].front().get<std::string>(),
              mcd_env->getDekManager().lookup(cb::dek::Entity::Logs)->id);
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
    constexpr std::string_view unencrypted =
            cb::crypto::KeyDerivationKey::UnencryptedKeyId;
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
    ids.emplace_back(dekManager.lookup(cb::dek::Entity::Logs)->id);
    for (int ii = 0; ii < 10; ++ii) {
        dekManager.setActive(cb::dek::Entity::Logs,
                             cb::crypto::KeyDerivationKey::generate());
        ids.emplace_back(dekManager.lookup(cb::dek::Entity::Logs)->id);
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
              dekManager.lookup(cb::dek::Entity::Logs)->id);
}

TEST_P(EncryptionTest, TestPruneDeks_UnknownKeyId) {
    auto keys = nlohmann::json::array();
    keys.emplace_back("This is an unknown key id");
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::PruneEncryptionKeys,
                                  format_as(cb::dek::Entity::Logs),
                                  keys.dump()});
    ASSERT_EQ(cb::mcbp::Status::EncryptionKeyNotAvailable, rsp.getStatus());
    rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::PruneEncryptionKeys,
                                  format_as(cb::dek::Entity::Audit),
                                  keys.dump()});
    ASSERT_EQ(cb::mcbp::Status::EncryptionKeyNotAvailable, rsp.getStatus());
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
    rsp = compactDb();
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
            cb::crypto::KeyDerivationKey::generate());
    config = nlohmann::json(mcd_env->getTestBucket().keystore).dump();
    rsp = connection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
            bucketName,
            config});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    rsp = compactDb();
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
    std::vector<bool> shards_expected(num_shards, false);
    shards_expected[0] = true; // shard 0 has vb:0
    constexpr int one_alog_run_expected = 1;
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
    compactDb();

    // Now wait for the access scanner to run and rewrite the access log
    // unencrypted
    waitForEncryptionKey(cb::crypto::KeyDerivationKey::UnencryptedKeyId);
    while (num_runs == waitForSnoozedAccessScanner()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    verifyAccessLogFiles(shards_expected, false);

    // Rerun the access scanner. This time we should get the .old plain
    // files
    rerunAccessScanner(one_alog_run_expected);
    verifyAccessLogFiles(shards_expected, false);

    // Turn encryption back on and verify that the access log is
    // rewritten encrypted, and the .old files are removed
    cb::crypto::SharedKeyDerivationKey key =
            cb::crypto::KeyDerivationKey::generate();

    // Turn encryption back on and verify that the access log is rewritten
    // encrypted
    mcd_env->getTestBucket().keystore.setActiveKey(key);
    config = nlohmann::json(mcd_env->getTestBucket().keystore).dump();
    rsp = connection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SetActiveEncryptionKeys,
            bucketName,
            config});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    compactDb();

    // Now wait for the access scanner to run and rewrite the access log
    // unencrypted
    waitForEncryptionKey(key->id);
    while (num_runs == waitForSnoozedAccessScanner()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    verifyAccessLogFiles(shards_expected, true);
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

void EncryptionTest::testSetEncryptionKeysWithAllUnavailable(
        cb::dek::Entity entity, cb::crypto::SharedKeyDerivationKey key) {
    using namespace cb::crypto;

    std::vector<std::string> existing_keys;
    mcd_env->getDekManager().iterate(
            [&existing_keys, entity](auto ent, const auto& ks) {
                if (ent == entity) {
                    ks.iterateKeys([&existing_keys](const auto k) {
                        if (k) {
                            existing_keys.emplace_back(k->id);
                        }
                    });
                }
            });

    mcd_env->getDekManager().setActive(entity, key);

    KeyStore ks;
    ks.setActiveKey(key);

    auto rsp = adminConnection->execute(
            BinprotSetActiveEncryptionKeysCommand{entity, ks, existing_keys});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    // But if we try to set the active key for the logs entity with a
    // key that is not in the keystore on the server we should get an error
    existing_keys.emplace_back("This is an unknown key id");
    existing_keys.emplace_back("This is another unknown key id");
    rsp = adminConnection->execute(
            BinprotSetActiveEncryptionKeysCommand{entity, ks, existing_keys});
    EXPECT_EQ(cb::mcbp::Status::EncryptionKeyNotAvailable, rsp.getStatus());
    // Verify that the error context contains the unknown keys
    EXPECT_TRUE(rsp.getDataView().contains("This is an unknown key id"));
    EXPECT_TRUE(rsp.getDataView().contains("This is another unknown key id"));

    // Verify that I don't break the logic with the invalid request and can
    // set the active keys again
    existing_keys.resize(existing_keys.size() - 2);

    rsp = adminConnection->execute(
            BinprotSetActiveEncryptionKeysCommand{entity, ks, existing_keys});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

void EncryptionTest::testSetEncryptionKeysWithAllUnavailable_bucket(
        cb::crypto::SharedKeyDerivationKey key) {
    using namespace cb::crypto;

    std::vector<std::string> existing_keys;
    auto& bucket_keystore = mcd_env->getTestBucket().keystore;

    bucket_keystore.iterateKeys([&existing_keys](const auto& k) {
        if (k) {
            existing_keys.emplace_back(k->id);
        }
    });
    bucket_keystore.setActiveKey(key);

    KeyStore ks;
    ks.setActiveKey(key);

    auto rsp = adminConnection->execute(BinprotSetActiveEncryptionKeysCommand{
            bucketName, ks, existing_keys});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    // But if we try to set the active key for the logs entity with a
    // key that is not in the keystore on the server we should get an error
    existing_keys.emplace_back("This is an unknown key id");
    existing_keys.emplace_back("This is another unknown key id");
    rsp = adminConnection->execute(BinprotSetActiveEncryptionKeysCommand{
            bucketName, ks, existing_keys});
    EXPECT_EQ(cb::mcbp::Status::EncryptionKeyNotAvailable, rsp.getStatus());
    // Verify that the error context contains the unknown keys
    EXPECT_TRUE(rsp.getDataView().contains("This is an unknown key id"));
    EXPECT_TRUE(rsp.getDataView().contains("This is another unknown key id"));

    // Verify that I don't break the logic with the invalid request and can
    // set the active keys again
    existing_keys.resize(existing_keys.size() - 2);

    rsp = adminConnection->execute(BinprotSetActiveEncryptionKeysCommand{
            bucketName, ks, existing_keys});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

TEST_P(EncryptionTest, SetActiveKeysWhenMissingKey_logs) {
    // disable
    testSetEncryptionKeysWithAllUnavailable(cb::dek::Entity::Logs, {});
    // set new active
    testSetEncryptionKeysWithAllUnavailable(
            cb::dek::Entity::Logs, cb::crypto::KeyDerivationKey::generate());
}

TEST_P(EncryptionTest, SetActiveKeysWhenMissingKey_audit) {
    // disable
    testSetEncryptionKeysWithAllUnavailable(cb::dek::Entity::Audit, {});
    // set new active
    testSetEncryptionKeysWithAllUnavailable(
            cb::dek::Entity::Audit, cb::crypto::KeyDerivationKey::generate());
}

TEST_P(EncryptionTest, SetActiveKeysWhenMissingKey_config) {
    // disable
    testSetEncryptionKeysWithAllUnavailable(cb::dek::Entity::Config, {});
    // set new active
    testSetEncryptionKeysWithAllUnavailable(
            cb::dek::Entity::Config, cb::crypto::KeyDerivationKey::generate());
}

TEST_P(EncryptionTest, SetActiveKeysWhenMissingKey_bucket) {
    // disable
    testSetEncryptionKeysWithAllUnavailable_bucket({});
    // set new active
    testSetEncryptionKeysWithAllUnavailable_bucket(
            cb::crypto::KeyDerivationKey::generate());
}
