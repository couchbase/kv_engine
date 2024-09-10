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

TEST_P(EncryptionTest, TestEncryptionKeyIds) {
    nlohmann::json stats;
    adminConnection->executeInBucket(bucketName, [&stats](auto& conn) {
        conn.stats([&stats](auto& k,
                            auto& v) { stats = nlohmann::json::parse(v); },
                   "encryption-key-ids");
    });

    // The returned stats is something like:
    //     {
    //         "38f04f51-0f76-47a4-b1aa-23f6e9f909f4" : [0],
    //         "0c7a52b5-7de1-4e96-883e-0d42393ce4c4" : [0, 1]
    //     }
    // but we only have a single vbucket in memcached testapp which means
    // we get an array of a single element containing '0'
    EXPECT_FALSE(stats.empty());
    for (const auto& key : stats) {
        ASSERT_TRUE(key.is_array()) << stats.dump();
        EXPECT_EQ(0, key.front().get<int>()) << stats.dump();
    }
}
