/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "manager.h"
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <filesystem>
using namespace cb::dek;

TEST(ManagerTest, GlobalInstance) {
    EXPECT_EQ(&Manager::instance(), &Manager::instance());
    EXPECT_NE(&Manager::instance(), Manager::create().get());
}

TEST(ManagerTest, GenerationUpdated) {
    auto version = Manager::instance()
                           .getEntityGenerationCounter(Entity::Config)
                           ->load();
    SharedKeyDerivationKey key = cb::crypto::KeyDerivationKey::generate();
    Manager::instance().setActive(Entity::Config, key);
    EXPECT_EQ(version + 1,
              Manager::instance()
                      .getEntityGenerationCounter(Entity::Config)
                      ->load());

    // Setting the same key does *not* change the version
    Manager::instance().setActive(Entity::Config, key);
    Manager::instance().setActive(Entity::Config, key);
    Manager::instance().setActive(Entity::Config, key);
    Manager::instance().setActive(Entity::Config, key);
    Manager::instance().setActive(Entity::Config, key);
    EXPECT_EQ(version + 1,
              Manager::instance()
                      .getEntityGenerationCounter(Entity::Config)
                      ->load());

    // But setting a new key will
    Manager::instance().setActive(Entity::Config,
                                  cb::crypto::KeyDerivationKey::generate());
    EXPECT_EQ(version + 2,
              Manager::instance()
                      .getEntityGenerationCounter(Entity::Config)
                      ->load());
}

TEST(ManagerTest, GenerationUpdatedKeyStore) {
    cb::crypto::KeyStore ks;
    ks.setActiveKey(cb::crypto::KeyDerivationKey::generate());
    Manager::instance().setActive(Entity::Config, ks);
    auto version = Manager::instance()
                           .getEntityGenerationCounter(Entity::Config)
                           ->load();

    // Setting to the same value does not change the version
    Manager::instance().setActive(Entity::Config, ks);
    Manager::instance().setActive(Entity::Config, ks);
    Manager::instance().setActive(Entity::Config, ks);
    Manager::instance().setActive(Entity::Config, ks);

    EXPECT_EQ(version,
              Manager::instance()
                      .getEntityGenerationCounter(Entity::Config)
                      ->load());

    // Setting a new value does change
    ks.setActiveKey(cb::crypto::KeyDerivationKey::generate());
    Manager::instance().setActive(Entity::Config, ks);
    EXPECT_EQ(version + 1,
              Manager::instance()
                      .getEntityGenerationCounter(Entity::Config)
                      ->load());
}

TEST(ManagerTest, SaveAndLoad) {
    auto manager = Manager::create();
    const auto file = std::filesystem::path{cb::io::mktemp("ManagerTest")};
    const std::string content = "This is the content";

    // No key: the data is stored in plain text
    manager->save(Entity::Config, file, content);
    EXPECT_EQ(content, cb::io::loadFile(file));
    EXPECT_EQ(content, manager->load(Entity::Config, file));

    manager->setActive(Entity::Config,
                       cb::crypto::KeyDerivationKey::generate());
    manager->save(Entity::Config, file, content);
    EXPECT_EQ(std::string::npos, cb::io::loadFile(file).find(content))
            << "The data should be encrypted";
    EXPECT_EQ(content, manager->load(Entity::Config, file));
    std::filesystem::remove(file);
}

TEST(ManagerTest, SaveFailure) {
    auto manager = Manager::create();
    const auto file = std::filesystem::path{cb::io::mktemp("ManagerTest")};
    std::filesystem::remove(file);
    EXPECT_THROW(manager->save(Entity::Config,
                               file / "non-existing-subdir" / "file",
                               "content"),
                 std::exception);
}
