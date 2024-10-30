/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../../src/kvstore/couch-kvstore/vbucket_encryption_keys_manager.h"
#include <folly/portability/GTest.h>

TEST(VbucketEncryptionKeysManagerTest, TestEncryptionKeysManager) {
    VBucketEncryptionKeysManager instance;
    auto set = instance.getKeys();
    EXPECT_TRUE(set.empty());

    instance.setCurrentKey(Vbid{0}, "first-key");
    set = instance.getKeys();
    EXPECT_EQ(1, set.size());
    EXPECT_EQ(1, set.count("first-key"));

    instance.setNextKey(Vbid{1}, "second-key");
    set = instance.getKeys();
    EXPECT_EQ(2, set.size());
    EXPECT_EQ(1, set.count("first-key"));
    EXPECT_EQ(1, set.count("second-key"));

    instance.promoteNextKey(Vbid{1});
    set = instance.getKeys();
    EXPECT_EQ(2, set.size());
    EXPECT_EQ(1, set.count("first-key"));
    EXPECT_EQ(1, set.count("second-key"));

    instance.removeCurrentKey(Vbid{0});
    set = instance.getKeys();
    EXPECT_EQ(1, set.size());
    EXPECT_EQ(1, set.count("second-key"));
}