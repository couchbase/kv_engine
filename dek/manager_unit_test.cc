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
using namespace cb::dek;

TEST(ManagerTest, GlobalInstance) {
    EXPECT_EQ(&Manager::instance(), &Manager::instance());
    EXPECT_NE(&Manager::instance(), Manager::create().get());
}

TEST(ManagerTest, GenerationUpdated) {
    auto version = Manager::instance()
                           .getEntityGenerationCounter(Entity::Config)
                           ->load();
    Manager::instance().setActive(Entity::Config, SharedEncryptionKey{});
    EXPECT_EQ(version + 1,
              Manager::instance()
                      .getEntityGenerationCounter(Entity::Config)
                      ->load());
}
