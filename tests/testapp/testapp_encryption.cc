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
#include <nlohmann/json.hpp>
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
}
