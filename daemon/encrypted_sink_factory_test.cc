/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "encrypted_sink_factory.h"

#include <cbcrypto/common.h>
#include <dek/manager.h>
#include <folly/portability/GTest.h>

class EncryptedSinkFactoryTest : public ::testing::Test {
protected:
    void TearDown() override {
        // leaving it unencrypted to avoid leaking state into other tests
        cb::dek::Manager::instance().setActive(cb::dek::Entity::Logs, nullptr);
    }

    cb::dek::Manager& manager = cb::dek::Manager::instance();
};

TEST_F(EncryptedSinkFactoryTest, NoActiveKeyYieldsEmpty) {
    manager.setActive(cb::dek::Entity::Logs, nullptr);

    auto config = cb::logger::makeFileSinkEncryptionConfig();
    ASSERT_TRUE(config.getKey);
    // No active key => empty key => the sink writes plaintext
    EXPECT_EQ(nullptr, config.getKey());
}

TEST_F(EncryptedSinkFactoryTest, ReturnsActiveLogKey) {
    auto key = cb::crypto::KeyDerivationKey::generate();
    const auto id = key->id;
    manager.setActive(cb::dek::Entity::Logs, std::move(key));

    auto config = cb::logger::makeFileSinkEncryptionConfig();
    ASSERT_TRUE(config.getKey());
    auto active = config.getKey();
    ASSERT_NE(nullptr, active);
    EXPECT_EQ(id, active->id);
}

TEST_F(EncryptedSinkFactoryTest, ConfigVersionTracksGenerationCounter) {
    auto config = cb::logger::makeFileSinkEncryptionConfig();
    // The version pointer must alias the manager's generation counter so the
    // sink can cheaply detect a rekey
    ASSERT_NE(nullptr, config.configVersion);
    EXPECT_EQ(manager.getEntityGenerationCounter(cb::dek::Entity::Logs),
              config.configVersion);

    const auto before = config.configVersion->load();
    manager.setActive(cb::dek::Entity::Logs,
                      cb::crypto::KeyDerivationKey::generate());
    EXPECT_LT(before, config.configVersion->load());
}
