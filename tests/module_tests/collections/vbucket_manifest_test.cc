/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/manifest.h"
#include "collections/vbucket_manifest.h"
#include "tests/module_tests/makestoreddockey.h"

#include <gtest/gtest.h>

class MockVBManifest : public Collections::VB::Manifest {
public:
    bool exists(const std::string& collection, uint32_t rev) const {
        std::lock_guard<ReaderLock> readLock(lock.reader());
        return exists_UNLOCKED(collection, rev);
    }

    bool isOpen(const std::string& collection, uint32_t rev) const {
        std::lock_guard<ReaderLock> readLock(lock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isOpen();
    }

    bool isExclusiveOpen(const std::string& collection, uint32_t rev) const {
        std::lock_guard<ReaderLock> readLock(lock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isOpen() && !itr->second->isDeleting();
    }

    bool isDeleting(const std::string& collection, uint32_t rev) const {
        std::lock_guard<ReaderLock> readLock(lock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isDeleting();
    }

    bool isExclusiveDeleting(const std::string& collection,
                             uint32_t rev) const {
        std::lock_guard<ReaderLock> readLock(lock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isDeleting() && !itr->second->isOpen();
    }

    bool isOpenAndDeleting(const std::string& collection, uint32_t rev) const {
        std::lock_guard<ReaderLock> readLock(lock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isOpen() && itr->second->isDeleting();
    }

    size_t size() const {
        std::lock_guard<ReaderLock> readLock(lock.reader());
        return map.size();
    }

protected:
    bool exists_UNLOCKED(const std::string& collection, uint32_t rev) const {
        auto itr = map.find(collection);
        return itr != map.end() && itr->second->getRevision() == rev;
    }

    void expect_true(bool in) const {
        if (!in) {
            throw std::logic_error("expect_true found false");
        }
    }
};

class VBucketManifestTest : public ::testing::Test {
public:
    // An enum to determine how to check the seqno as it's not wise to encode
    // explicit value checks.
    enum class CollectionState {
        Open,
        ExclusiveOpen,
        Deleting,
        ExclusiveDeleting,
        OpenAndDeleting
    };

    MockVBManifest vbm;
};

TEST_F(VBucketManifestTest, collectionExists) {
    vbm.update(
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));
}

TEST_F(VBucketManifestTest, defaultCollectionExists) {
    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));
    vbm.update({R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));
}

TEST_F(VBucketManifestTest, updates) {
    EXPECT_EQ(1, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("$default", 0));

    vbm.update({R"({"revision":1, "separator":"::",
                "collections":["$default","vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 1));

    vbm.update({R"({"revision":2, "separator":"::",
                "collections":["$default", "vegetable", "fruit"]})"});
    EXPECT_EQ(3, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("fruit", 2));

    vbm.update({R"({"revision":3, "separator":"::",
            "collections":
            ["$default", "vegetable", "fruit", "meat", "dairy"]})"});
    EXPECT_EQ(5, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("meat", 3));
    EXPECT_TRUE(vbm.isExclusiveOpen("dairy", 3));
}

TEST_F(VBucketManifestTest, updates2) {
    vbm.update({R"({"revision":0, "separator":"::",
        "collections":["$default", "vegetable", "fruit", "meat", "dairy"]})"});
    EXPECT_EQ(5, vbm.size());

    // Remove meat and dairy, size is not affected because the delete is only
    // starting
    vbm.update({R"({"revision":1, "separator":"::",
        "collections":["$default", "vegetable", "fruit"]})"});
    EXPECT_EQ(5, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("meat", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("dairy", 1));

    // But vegetable is accessible, the others are locked out
    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));
    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("dairy::milk", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("meat::chicken", DocNamespace::Collections)));
}

TEST_F(VBucketManifestTest, updates3) {
    vbm.update({R"({"revision":0,"separator":"::",
        "collections":["$default", "vegetable", "fruit", "meat", "dairy"]})"});
    EXPECT_EQ(5, vbm.size());

    // Remove everything
    vbm.update({R"({"revision":1, "separator":"::","collections":[]})"});
    EXPECT_EQ(5, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("$default", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("fruit", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("meat", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("dairy", 1));

    // But vegetable is accessible, the others are 'locked' out
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("dairy::milk", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("meat::chicken", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("fruit::apple", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));
}

TEST_F(VBucketManifestTest, add_beginDelete_add) {
    // add vegetable
    vbm.update(
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // remove vegetable
    vbm.update({R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // add vegetable
    vbm.update(
            {R"({"revision":2,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isOpenAndDeleting("vegetable", 2));

    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
}

TEST_F(VBucketManifestTest, add_beginDelete_delete) {
    // add vegetable
    vbm.update(
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // remove vegetable
    vbm.update({R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // finally remove vegetable
    vbm.completeDeletion("vegetable");
    EXPECT_EQ(1, vbm.size());
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
}

TEST_F(VBucketManifestTest, add_beginDelete_add_delete) {
    // add vegetable
    vbm.update(
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // remove vegetable
    vbm.update({R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // add vegetable
    vbm.update(
            {R"({"revision":2,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isOpenAndDeleting("vegetable", 2));

    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // finally remove vegetable
    vbm.completeDeletion("vegetable");
    EXPECT_EQ(2, vbm.size());

    // No longer OpenAndDeleting, now ExclusiveOpen
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 2));

    EXPECT_TRUE(vbm.doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
}