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

#include "checkpoint.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_serialised_manifest_entry.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "tests/module_tests/test_helpers.h"

#include <cJSON_utils.h>

#include <gtest/gtest.h>

class MockVBManifest : public Collections::VB::Manifest {
public:
    MockVBManifest() : Collections::VB::Manifest({/* no collection data*/}) {
    }

    MockVBManifest(const std::string& json) : Collections::VB::Manifest(json) {
    }

    bool exists(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return exists_UNLOCKED(collection, rev);
    }

    bool isOpen(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isOpen();
    }

    bool isExclusiveOpen(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isExclusiveOpen();
    }

    bool isDeleting(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isDeleting();
    }

    bool isExclusiveDeleting(const std::string& collection,
                             uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isExclusiveDeleting();
    }

    bool isOpenAndDeleting(const std::string& collection, uint32_t rev) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(collection, rev));
        auto itr = map.find(collection);
        return itr->second->isOpenAndDeleting();
    }

    size_t size() const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return map.size();
    }

    bool compareEntry(const Collections::VB::ManifestEntry& entry) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        if (exists_UNLOCKED(entry.getCollectionName(), entry.getRevision())) {
            auto itr = map.find(entry.getCollectionName());
            const auto& myEntry = *itr->second;
            return myEntry.getStartSeqno() == entry.getStartSeqno() &&
                   myEntry.getEndSeqno() == entry.getEndSeqno();
        }
        return false;
    }

    bool operator==(const MockVBManifest& rhs) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        for (const auto& e : map) {
            if (!rhs.compareEntry(*e.second)) {
                return false;
            }
        }

        // finally check the separator's match
        return rhs.separator == separator;
    }

    bool operator!=(const MockVBManifest& rhs) const {
        return !(*this == rhs);
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

    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<uint16_t> {
    public:
        DummyCB() {
        }

        void callback(uint16_t& dummy) {
        }
    };

    VBucketManifestTest()
        : vbm(),
          vbucket(0,
                  vbucket_state_active,
                  global_stats,
                  checkpoint_config,
                  /*kvshard*/ nullptr,
                  /*lastSeqno*/ 0,
                  /*lastSnapStart*/ 0,
                  /*lastSnapEnd*/ 0,
                  /*table*/ nullptr,
                  std::make_shared<DummyCB>(),
                  /*newSeqnoCb*/ nullptr,
                  config,
                  VALUE_ONLY) {
    }

    queued_item getLastSystemEvent() {
        std::vector<queued_item> items;
        vbucket.checkpointManager.getAllItemsForCursor(
                CheckpointManager::pCursorName, items);
        std::vector<queued_item> events;
        for (const auto& qi : items) {
            if (qi->getOperation() == queue_op::system_event) {
                events.push_back(qi);
            }
        }

        if (0 == events.size()) {
            throw std::logic_error("Found no events");
        }

        return events.back();
    }

    std::string getLastEventJson() {
        auto event = getLastSystemEvent();
        cb::const_char_buffer buffer(event->getData(), event->getNBytes());
        return Collections::VB::Manifest::serialToJson(
                SystemEvent(event->getFlags()), buffer, event->getBySeqno());
    }

    /**
     * Create a new VB::Manifest using the JSON we create from the tests
     * vbm object (found in the vbucket checkpoint).
     *
     * Then compare that the new manifest matches the tests current vbm.
     * @returns gtest assertion with info about the mismatch on failure.
     */
    ::testing::AssertionResult checkJson() {
        MockVBManifest newManifest(getLastEventJson());
        if (newManifest != vbm) {
            return ::testing::AssertionFailure() << "manifest mismatch\n"
                                                 << "generated\n"
                                                 << newManifest << "\nfrom\n"
                                                 << vbm;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult noThrowUpdate(const char* json) {
        try {
            vbm.wlock().update(vbucket, {json});
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "Exception thrown for update with " << json
                   << ", e.what:" << e.what();
        }
        return checkJson();
    }

    ::testing::AssertionResult throwUpdate(const char* json) {
        try {
            vbm.wlock().update(vbucket, {json});
            return ::testing::AssertionFailure()
                   << "No exception thrown for update of " << json;
        } catch (std::exception& e) {
            return ::testing::AssertionSuccess();
        }
    }

protected:
    MockVBManifest vbm;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    EPVBucket vbucket;
};

TEST_F(VBucketManifestTest, collectionExists) {
    vbm.wlock().update(
            vbucket,
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));

    EXPECT_TRUE(checkJson());
}

TEST_F(VBucketManifestTest, defaultCollectionExists) {
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));
    vbm.wlock().update(vbucket,
                       {R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));

    EXPECT_TRUE(checkJson());
}

TEST_F(VBucketManifestTest, updates) {
    EXPECT_EQ(1, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("$default", 0));

    vbm.wlock().update(vbucket, {R"({"revision":1, "separator":"::",
                "collections":["$default","vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 1));

    vbm.wlock().update(vbucket, {R"({"revision":2, "separator":"::",
                "collections":["$default", "vegetable", "fruit"]})"});
    EXPECT_EQ(3, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("fruit", 2));

    vbm.wlock().update(vbucket, {R"({"revision":3, "separator":"::",
            "collections":
            ["$default", "vegetable", "fruit", "meat", "dairy"]})"});
    EXPECT_EQ(5, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("meat", 3));
    EXPECT_TRUE(vbm.isExclusiveOpen("dairy", 3));

    EXPECT_TRUE(checkJson());
}

TEST_F(VBucketManifestTest, updates2) {
    vbm.wlock().update(vbucket, {R"({"revision":0, "separator":"::",
        "collections":["$default", "vegetable", "fruit", "meat", "dairy"]})"});
    EXPECT_EQ(5, vbm.size());

    EXPECT_TRUE(checkJson());

    // Remove meat and dairy, size is not affected because the delete is only
    // starting
    vbm.wlock().update(vbucket, {R"({"revision":1, "separator":"::",
        "collections":["$default", "vegetable", "fruit"]})"});
    EXPECT_EQ(5, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("meat", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("dairy", 1));

    // But vegetable is accessible, the others are locked out
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("dairy::milk", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("meat::chicken", DocNamespace::Collections)));

    EXPECT_TRUE(checkJson());
}

TEST_F(VBucketManifestTest, updates3) {
    vbm.wlock().update(vbucket, {R"({"revision":0,"separator":"::",
        "collections":["$default", "vegetable", "fruit", "meat", "dairy"]})"});
    EXPECT_EQ(5, vbm.size());

    // Remove everything
    vbm.wlock().update(
            vbucket, {R"({"revision":1, "separator":"::","collections":[]})"});
    EXPECT_EQ(5, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("$default", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("fruit", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("meat", 1));
    EXPECT_TRUE(vbm.isExclusiveDeleting("dairy", 1));

    // But vegetable is accessible, the others are 'locked' out
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("dairy::milk", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("meat::chicken", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("fruit::apple", DocNamespace::Collections)));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("anykey", DocNamespace::DefaultCollection)));

    EXPECT_TRUE(checkJson());
}

TEST_F(VBucketManifestTest, add_beginDelete_add) {
    // add vegetable
    vbm.wlock().update(
            vbucket,
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // remove vegetable
    vbm.wlock().update(vbucket,
                       {R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));
    // add vegetable
    vbm.wlock().update(
            vbucket,
            {R"({"revision":2,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isOpenAndDeleting("vegetable", 2));

    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    EXPECT_TRUE(checkJson());
}

TEST_F(VBucketManifestTest, add_beginDelete_delete) {
    // add vegetable
    vbm.wlock().update(
            vbucket,
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // remove vegetable
    vbm.wlock().update(vbucket,
                       {R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // finally remove vegetable
    vbm.wlock().completeDeletion(vbucket, {"vegetable"}, 1);
    EXPECT_EQ(1, vbm.size());
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    EXPECT_TRUE(checkJson());
}

TEST_F(VBucketManifestTest, add_beginDelete_add_delete) {
    // add vegetable
    vbm.wlock().update(
            vbucket,
            {R"({"revision":0,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 0));
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // remove vegetable
    vbm.wlock().update(vbucket,
                       {R"({"revision":1,"separator":"::","collections":[]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isExclusiveDeleting("vegetable", 1));
    EXPECT_FALSE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // add vegetable
    vbm.wlock().update(
            vbucket,
            {R"({"revision":2,"separator":"::","collections":["vegetable"]})"});
    EXPECT_EQ(2, vbm.size());
    EXPECT_TRUE(vbm.isOpenAndDeleting("vegetable", 2));

    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    // finally remove vegetable
    vbm.wlock().completeDeletion(vbucket, {"vegetable"}, 3);
    EXPECT_EQ(2, vbm.size());

    // No longer OpenAndDeleting, now ExclusiveOpen
    EXPECT_TRUE(vbm.isExclusiveOpen("vegetable", 2));

    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            makeStoredDocKey("vegetable::carrot", DocNamespace::Collections)));

    EXPECT_TRUE(checkJson());

    Collections::VB::Manifest g("");
}

TEST_F(VBucketManifestTest, invalidDeletes) {
    // add vegetable
    vbm.wlock().update(vbucket,
                       {R"({"revision":1,"separator":"::",)"
                        R"("collections":["$default","vegetable"]})"});
    // Delete vegetable
    vbm.wlock().update(vbucket,
                       {R"({"revision":2,"separator":"::",)"
                        R"("collections":["$default"]})"});

    EXPECT_THROW(vbm.wlock().completeDeletion(vbucket, {"unknown"}, 1),
                 std::logic_error);
    EXPECT_THROW(vbm.wlock().completeDeletion(vbucket, {"$default"}, 1),
                 std::logic_error);
    EXPECT_NO_THROW(vbm.wlock().completeDeletion(vbucket, {"vegetable"}, 1));

    // Delete $default
    vbm.wlock().update(vbucket,
                       {R"({"revision":3,"separator":"::",)"
                        R"("collections":[]})"});
    // Add $default
    vbm.wlock().update(vbucket,
                       {R"({"revision":4,"separator":"::",)"
                        R"("collections":["$default"]})"});
    EXPECT_NO_THROW(vbm.wlock().completeDeletion(vbucket, {"$default"}, 3));
}

// Check that a deleting collection doesn't keep adding system events
TEST_F(VBucketManifestTest, doubleDelete) {
    auto seqno = vbucket.getHighSeqno();
    // add vegetable
    vbm.wlock().update(vbucket,
                       {R"({"revision":1,"separator":"::",)"
                        R"("collections":["$default","vegetable"]})"});
    EXPECT_LT(seqno, vbucket.getHighSeqno());
    seqno = vbucket.getHighSeqno();

    // same again, should have be nothing created or deleted
    vbm.wlock().update(vbucket,
                       {R"({"revision":2,"separator":"::",)"
                        R"("collections":["$default","vegetable"]})"});

    EXPECT_EQ(seqno, vbucket.getHighSeqno());
    seqno = vbucket.getHighSeqno();

    // Now delete vegetable
    vbm.wlock().update(vbucket,
                       {R"({"revision":3,"separator":"::",)"
                        R"("collections":["$default"]})"});

    EXPECT_LT(seqno, vbucket.getHighSeqno());
    seqno = vbucket.getHighSeqno();

    // same again, should have be nothing created or deleted
    vbm.wlock().update(vbucket,
                       {R"({"revision":4,"separator":"::",)"
                        R"("collections":["$default"]})"});

    EXPECT_EQ(seqno, vbucket.getHighSeqno());
}

TEST_F(VBucketManifestTest, separatorChanges) {
    // Can change separator to @ as only default exists
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":1, "separator":"@", "collections":["$default"]})"));

    // Can change separator to / and add first collection
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":2, "separator":"/", "collections":["$default", "vegetable"]})"));

    // Cannot change separator to ## because non-default collections exist
    EXPECT_TRUE(throwUpdate(
            R"({"revision":3, "separator":"##", "collections":["$default", "vegetable"]})"));

    // Now just remove vegetable
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":3, "separator":"/", "collections":["$default"]})"));

    // vegetable still exists (isDeleting), but change to ##
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":4, "separator":"##", "collections":["$default"]})"));

    // Finish removal of vegetable
    vbm.wlock().completeDeletion(vbucket, "vegetable", 4);

    // Can change separator as only default exists
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":5, "separator":"@", "collections":["$default"]})"));

    // Remove default
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":6, "separator":"/", "collections":[]})"));

    // $default still exists (isDeleting), so cannot change to ##
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":7, "separator":"##", "collections":["$default"]})"));

    vbm.wlock().completeDeletion(vbucket, "$default", 5);

    // Can change separator as no collection exists
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":8, "separator":"-=-=-=-", "collections":[]})"));

    // Add a collection and check the new separator
    EXPECT_TRUE(noThrowUpdate(
            R"({"revision":9, "separator":"-=-=-=-", "collections":["meat"]})"));
    EXPECT_TRUE(vbm.lock().doesKeyContainValidCollection(
            {"meat-=-=-=-bacon", DocNamespace::Collections}));
}