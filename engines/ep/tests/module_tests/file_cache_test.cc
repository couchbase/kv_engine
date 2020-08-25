/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "engines/ep/src/couch-kvstore/couch-kvstore-config.h"
#include "engines/ep/src/couch-kvstore/couch-kvstore-file-cache.h"
#include "engines/ep/src/couch-kvstore/couch-kvstore.h"

#include <folly/portability/GTest.h>

class FileCacheTest : public ::testing::Test {
public:
    void SetUp() override {
        // We don't care about the KVStore but we need it to create DbHolders
        // to put in the FileCache
        CouchKVStoreConfig config{
                4 /*vBuckets*/, 4 /*shards*/, "name", "couchstore", 0};
        store = std::make_unique<CouchKVStore>(config);

        CouchKVStoreFileCache::get().getHandle()->clear();
    }

protected:
    std::unique_ptr<CouchKVStore> store;
};

TEST_F(FileCacheTest, set) {
    auto file = DbHolder(*store);
    CouchKVStoreFileCache::get().getHandle()->set("k1", std::move(file));
    EXPECT_EQ(1, CouchKVStoreFileCache::get().getHandle()->numFiles());
}

TEST_F(FileCacheTest, insert) {
    auto file = DbHolder(*store);
    auto ret = CouchKVStoreFileCache::get().getHandle()->insert(
            "k1", std::move(file));
    EXPECT_TRUE(ret.second);
}

TEST_F(FileCacheTest, insertExisting) {
    auto file = DbHolder(*store);
    auto ret = CouchKVStoreFileCache::get().getHandle()->insert(
            "k1", std::move(file));
    EXPECT_TRUE(ret.second);

    file = DbHolder(*store);
    ret = CouchKVStoreFileCache::get().getHandle()->insert("k1",
                                                           std::move(file));
    EXPECT_FALSE(ret.second);
}

TEST_F(FileCacheTest, setGet) {
    auto file = DbHolder(*store);
    file.setFileRev(123);
    CouchKVStoreFileCache::get().getHandle()->set("k1", std::move(file));
    EXPECT_EQ(1, CouchKVStoreFileCache::get().getHandle()->numFiles());

    { // Scope for "dbHolder" which is a LockedPtr
        auto dbHolder = CouchKVStoreFileCache::get().getHandle()->get("k1");
        EXPECT_EQ(123, dbHolder->getFileRev());
    }

    EXPECT_EQ(1, CouchKVStoreFileCache::get().getHandle()->numFiles());
}

TEST_F(FileCacheTest, setErase) {
    auto file = DbHolder(*store);
    CouchKVStoreFileCache::get().getHandle()->set("k1", std::move(file));
    EXPECT_EQ(1, CouchKVStoreFileCache::get().getHandle()->numFiles());

    CouchKVStoreFileCache::get().getHandle()->erase("k1");
    EXPECT_EQ(0, CouchKVStoreFileCache::get().getHandle()->numFiles());
}

TEST_F(FileCacheTest, clear) {
    auto file1 = DbHolder(*store);
    CouchKVStoreFileCache::get().getHandle()->set("k1", std::move(file1));

    auto file2 = DbHolder(*store);
    CouchKVStoreFileCache::get().getHandle()->set("k2", std::move(file2));

    CouchKVStoreFileCache::get().getHandle()->clear();

    EXPECT_EQ(0, CouchKVStoreFileCache::get().getHandle()->numFiles());
}

TEST_F(FileCacheTest, shrink) {
    CouchKVStoreFileCache::get().getHandle()->resize(2);

    auto file1 = DbHolder(*store);
    CouchKVStoreFileCache::get().getHandle()->set("k1", std::move(file1));

    auto file2 = DbHolder(*store);
    CouchKVStoreFileCache::get().getHandle()->set("k2", std::move(file2));
    EXPECT_EQ(2, CouchKVStoreFileCache::get().getHandle()->numFiles());

    CouchKVStoreFileCache::get().getHandle()->resize(1);
    EXPECT_EQ(1, CouchKVStoreFileCache::get().getHandle()->numFiles());

    // k1 was evicted because it's older than k2
    auto itr1 = CouchKVStoreFileCache::get().getHandle()->find("k1");
    EXPECT_EQ(itr1, CouchKVStoreFileCache::get().getHandle()->end());

    // k2 is still in the cache
    auto itr2 = CouchKVStoreFileCache::get().getHandle()->find("k2");
    EXPECT_NE(itr2, CouchKVStoreFileCache::get().getHandle()->end());
}

// Test that when we destroy a KVStore we close the files that "belong" to it
// and that the files "belonging" to other KVStores remain in the cache
TEST_F(FileCacheTest, KVStoreDestruction) {
    CouchKVStoreFileCache::get().getHandle()->resize(2);

    auto file1 = DbHolder(*store);
    CouchKVStoreFileCache::get().getHandle()->set("k1", std::move(file1));
    EXPECT_NO_THROW(CouchKVStoreFileCache::get().getHandle()->get("k1"));

    CouchKVStoreConfig config{
            4 /*vBuckets*/, 4 /*shards*/, "name", "couchstore", 0};
    auto newStore = CouchKVStore(config);

    auto file2 = DbHolder(newStore);
    CouchKVStoreFileCache::get().getHandle()->set("k2", std::move(file2));
    EXPECT_NO_THROW(CouchKVStoreFileCache::get().getHandle()->get("k2"));

    store.reset(nullptr);

    EXPECT_EQ(1, CouchKVStoreFileCache::get().getHandle()->numFiles());
    EXPECT_THROW(CouchKVStoreFileCache::get().getHandle()->get("k1"),
                 std::logic_error);
    EXPECT_NO_THROW(CouchKVStoreFileCache::get().getHandle()->get("k2"));
}
