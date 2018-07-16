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

#include "checkpoint_manager.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_serialised_manifest_entry.h"
#include "configuration.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/test_helpers.h"

#include <cJSON_utils.h>

#include <gtest/gtest.h>

class MockVBManifest : public Collections::VB::Manifest {
public:
    MockVBManifest() : Collections::VB::Manifest({/* no collection data*/}) {
    }

    MockVBManifest(const std::string& json) : Collections::VB::Manifest(json) {
    }

    bool exists(CollectionID identifier) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return exists_UNLOCKED(identifier);
    }

    bool isOpen(CollectionID identifier) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(identifier));
        auto itr = map.find(identifier);
        return itr->second.isOpen();
    }

    bool isExclusiveOpen(CollectionID identifier) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(identifier));
        auto itr = map.find(identifier);
        return itr->second.isExclusiveOpen();
    }

    bool isDeleting(CollectionID identifier) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(identifier));
        auto itr = map.find(identifier);
        return itr->second.isDeleting();
    }

    bool isExclusiveDeleting(CollectionID identifier) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(identifier));
        auto itr = map.find(identifier);
        return itr->second.isExclusiveDeleting();
    }

    bool isOpenAndDeleting(CollectionID identifier) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        expect_true(exists_UNLOCKED(identifier));
        auto itr = map.find(identifier);
        return itr->second.isOpenAndDeleting();
    }

    size_t size() const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return map.size();
    }

    bool compareEntry(CollectionID id,
                      const Collections::VB::ManifestEntry& entry) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        if (exists_UNLOCKED(id)) {
            auto itr = map.find(id);
            const auto& myEntry = itr->second;
            return myEntry.getStartSeqno() == entry.getStartSeqno() &&
                   myEntry.getEndSeqno() == entry.getEndSeqno();
        }
        return false;
    }

    bool operator==(const MockVBManifest& rhs) const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        if (rhs.size() != size()) {
            return false;
        }
        // Check all collections match
        for (const auto& e : map) {
            if (!rhs.compareEntry(e.first, e.second)) {
                return false;
            }
        }

        return true;
    }

    bool operator!=(const MockVBManifest& rhs) const {
        return !(*this == rhs);
    }

    int64_t getGreatestEndSeqno() const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return greatestEndSeqno;
    }

    size_t getNumDeletingCollections() const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        return nDeletingCollections;
    }

    bool isGreatestEndSeqnoCorrect() const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        // If this is zero greatestEnd should not be a seqno
        if (nDeletingCollections == 0) {
            return greatestEndSeqno == StoredValue::state_collection_open;
        }
        return greatestEndSeqno >= 0;
    }

    bool isNumDeletingCollectionsoCorrect() const {
        std::lock_guard<cb::ReaderLock> readLock(rwlock.reader());
        // If this is zero greatestEnd should not be a seqno
        if (greatestEndSeqno != StoredValue::state_collection_open) {
            return nDeletingCollections > 0;
        }
        return nDeletingCollections == 0;
    }

protected:
    bool exists_UNLOCKED(CollectionID identifier) const {
        auto itr = map.find(identifier);
        return itr != map.end();
    }

    void expect_true(bool in) const {
        if (!in) {
            std::stringstream ss;
            ss << *this;
            throw std::logic_error("expect_true found false manifest:" +
                                   ss.str());
        }
    }
};

/**
 * Test class that owns an active and replica manifest.
 * Updates applied to the active are applied to the replica by processing
 * the active's checkpoint.
 */
class ActiveReplicaManifest {
public:
    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<uint16_t> {
    public:
        DummyCB() {
        }

        void callback(uint16_t& dummy) {
        }
    };

    ActiveReplicaManifest()
        : active(),
          replica(),
          vbA(0,
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
              VALUE_ONLY),
          vbR(1,
              vbucket_state_replica,
              global_stats,
              checkpoint_config,
              /*kvshard*/ nullptr,
              /*lastSeqno*/ 0,
              /*lastSnapStart*/ 0,
              /*lastSnapEnd*/ snapEnd,
              /*table*/ nullptr,
              std::make_shared<DummyCB>(),
              /*newSeqnoCb*/ nullptr,
              config,
              VALUE_ONLY),
          lastCompleteDeletionArgs(0) {
    }

    ::testing::AssertionResult update(const std::string& json) {
        try {
            active.wlock().update(vbA, {json});
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "Exception thrown for update with " << json
                   << ", e.what:" << e.what();
        }
        queued_item manifest;
        try {
            manifest = applyCheckpointEventsToReplica();
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "Exception thrown for replica update, e.what:"
                   << e.what();
        }
        if (active != replica) {
            return ::testing::AssertionFailure()
                   << "active doesn't match replica active:\n"
                   << active << " replica:\n"
                   << replica;
        }

        auto rv = checkNumDeletingCollections();
        if (rv != ::testing::AssertionSuccess()) {
            return rv;
        }
        rv = checkGreatestEndSeqno();
        if (rv != ::testing::AssertionSuccess()) {
            return rv;
        }

        return checkJson(*manifest);
    }

    ::testing::AssertionResult completeDeletion(CollectionID identifier) {
        try {
            active.wlock().completeDeletion(vbA, identifier);
            lastCompleteDeletionArgs = identifier;
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "Exception thrown for completeDeletion with e.what:"
                   << e.what();
        }

        queued_item manifest;
        try {
            manifest = applyCheckpointEventsToReplica();
        } catch (std::exception& e) {
            return ::testing::AssertionFailure()
                   << "completeDeletion: Exception thrown for replica update, "
                      "e.what:"
                   << e.what();
        }

        // completeDeletion adds a new item without a seqno, which closes
        // the snapshot, re-open the snapshot so tests can continue.
        vbR.checkpointManager->updateCurrentSnapshotEnd(snapEnd);
        if (active != replica) {
            return ::testing::AssertionFailure()
                   << "completeDeletion: active doesn't match replica active:\n"
                   << active << " replica:\n"
                   << replica;
        }
        return checkJson(*manifest);
    }

    ::testing::AssertionResult doesKeyContainValidCollection(DocKey key) {
        if (!active.lock().doesKeyContainValidCollection(key)) {
            return ::testing::AssertionFailure() << "active failed the key";
        } else if (!replica.lock().doesKeyContainValidCollection(key)) {
            return ::testing::AssertionFailure() << "replica failed the key";
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult isLogicallyDeleted(DocKey key, int64_t seqno) {
        if (!active.lock().isLogicallyDeleted(key, seqno)) {
            return ::testing::AssertionFailure()
                   << "active failed the key seqno:" << seqno << "\n"
                   << active;
        } else if (!replica.lock().isLogicallyDeleted(key, seqno)) {
            return ::testing::AssertionFailure()
                   << "replica failed the key seqno:" << seqno << "\n"
                   << replica;
        }
        return ::testing::AssertionSuccess();
    }

    bool isOpen(CollectionID identifier) {
        return active.isOpen(identifier) && replica.isOpen(identifier);
    }

    bool isDeleting(CollectionID identifier) {
        return active.isDeleting(identifier) && replica.isDeleting(identifier);
    }

    bool isExclusiveOpen(CollectionID identifier) {
        return active.isExclusiveOpen(identifier) &&
               replica.isExclusiveOpen(identifier);
    }

    bool isExclusiveDeleting(CollectionID identifier) {
        return active.isExclusiveDeleting(identifier) &&
               replica.isExclusiveDeleting(identifier);
    }

    bool isOpenAndDeleting(CollectionID identifier) {
        return active.isOpenAndDeleting(identifier) &&
               replica.isOpenAndDeleting(identifier);
    }

    bool checkSize(size_t s) {
        return active.size() == s && replica.size() == s;
    }

    VBucket& getActiveVB() {
        return vbA;
    }

    MockVBManifest& getActiveManifest() {
        return active;
    }

    int64_t getLastSeqno() const {
        return lastSeqno;
    }

    ::testing::AssertionResult checkGreatestEndSeqno(int64_t expectedSeqno) {
        if (active.getGreatestEndSeqno() != expectedSeqno) {
            return ::testing::AssertionFailure()
                   << "active failed expectedSeqno:" << expectedSeqno << "\n"
                   << active;
        } else if (replica.getGreatestEndSeqno() != expectedSeqno) {
            return ::testing::AssertionFailure()
                   << "replica failed expectedSeqno:" << expectedSeqno << "\n"
                   << replica;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult checkNumDeletingCollections(size_t expected) {
        if (active.getNumDeletingCollections() != expected) {
            return ::testing::AssertionFailure()
                   << "active failed expected:" << expected << "\n"
                   << active;
        } else if (replica.getNumDeletingCollections() != expected) {
            return ::testing::AssertionFailure()
                   << "replica failed expected:" << expected << "\n"
                   << replica;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult checkNumDeletingCollections() {
        if (!active.isNumDeletingCollectionsoCorrect()) {
            return ::testing::AssertionFailure()
                   << "checkNumDeletingCollections active failed " << active;
        } else if (!replica.isNumDeletingCollectionsoCorrect()) {
            return ::testing::AssertionFailure()
                   << "checkNumDeletingCollections replica failed " << replica;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult checkGreatestEndSeqno() {
        if (!active.isGreatestEndSeqnoCorrect()) {
            return ::testing::AssertionFailure()
                   << "checkGreatestEndSeqno active failed " << active;
        } else if (!replica.isGreatestEndSeqnoCorrect()) {
            return ::testing::AssertionFailure()
                   << "checkGreatestEndSeqno replica failed " << replica;
        }
        return ::testing::AssertionSuccess();
    }

    static void getEventsFromCheckpoint(VBucket& vb,
                                        std::vector<queued_item>& events) {
        std::vector<queued_item> items;
        vb.checkpointManager->getAllItemsForCursor(
                CheckpointManager::pCursorName, items);
        for (const auto& qi : items) {
            if (qi->getOperation() == queue_op::system_event) {
                events.push_back(qi);
            }
        }

        if (events.empty()) {
            throw std::logic_error("getEventsFromCheckpoint: no events in vb:" +
                                   std::to_string(vb.getId()));
        }
    }

    /**
     * 1. scan the VBucketManifestTestVBucket's checkpoint for all system
     * events.
     * 2. for all system-events, pretend to be the DcpConsumer and call
     *    the VBucket's manifest's replica functions on.
     * @param replicaVB A vbucket acting as the replica, we will create/delete
     *        collections against this VB.
     * @param replicaManfiest The replica VB's manifest, we will create/delete
     *         collections against this manifest.
     *
     * @returns the last queued_item (which would be used to create a json
     *          manifest)
     */
    queued_item applyCheckpointEventsToReplica() {
        std::vector<queued_item> events;
        getEventsFromCheckpoint(vbA, events);
        queued_item rv = events.back();
        for (const auto& qi : events) {
            lastSeqno = qi->getBySeqno();
            if (qi->getOperation() == queue_op::system_event) {
                auto dcpData = Collections::VB::Manifest::getSystemEventData(
                        {qi->getData(), qi->getNBytes()});

                switch (SystemEvent(qi->getFlags())) {
                case SystemEvent::Collection: {
                    if (qi->isDeleted()) {
                        // A deleted create means beginDelete collection
                        replica.wlock().replicaBeginDelete(vbR,
                                                           dcpData.manifestUid,
                                                           dcpData.cid,
                                                           qi->getBySeqno());
                    } else {
                        replica.wlock().replicaAdd(vbR,
                                                   dcpData.manifestUid,
                                                   dcpData.cid,
                                                   qi->getBySeqno());
                    }
                    break;
                }
                case SystemEvent::DeleteCollectionSoft:
                case SystemEvent::DeleteCollectionHard:
                    // DCP doesn't transmit these events, but to improve test
                    // coverage call completeDeletion on the replica only in
                    // response to these system events appearing in the
                    // checkpoint. The data held in the system event isn't
                    // suitable though for forming the arguments to the function
                    // e.g. Delete hard, the serialised manifest doesn't have
                    // the collection:rev we pass through, hence why we cache
                    // the collection:rev data in lastCompleteDeletionArgs
                    replica.wlock().completeDeletion(vbR,
                                                     lastCompleteDeletionArgs);
                    break;
                }
            }
        }
        return rv;
    }

    /**
     * Take SystemEvent item and obtain the JSON manifest.
     * Next create a new/temp MockVBManifest from the JSON.
     * Finally check that this new object is equal to the test class's active
     *
     * @returns gtest assertion fail (with details) or success
     */
    ::testing::AssertionResult checkJson(const Item& manifest) {
        MockVBManifest newManifest(
                Collections::VB::Manifest::serialToJson(manifest));
        if (active != newManifest) {
            return ::testing::AssertionFailure() << "manifest mismatch\n"
                                                 << "generated\n"
                                                 << newManifest << "\nvs\n"
                                                 << active;
        }
        return ::testing::AssertionSuccess();
    }

    MockVBManifest active;
    MockVBManifest replica;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    EPVBucket vbA;
    EPVBucket vbR;
    int64_t lastSeqno;
    CollectionID lastCompleteDeletionArgs;

    static const int64_t snapEnd{200};
};

class VBucketManifestTest : public ::testing::Test {
public:
    ActiveReplicaManifest manifest;
    CollectionsManifest cm{CollectionEntry::vegetable};
};

TEST_F(VBucketManifestTest, collectionExists) {
    EXPECT_TRUE(manifest.update(cm));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::vegetable));
}

TEST_F(VBucketManifestTest, defaultCollectionExists) {
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::defaultC)));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
}

TEST_F(VBucketManifestTest, add_delete_in_one_update) {
    EXPECT_TRUE(manifest.update(cm));
    EXPECT_TRUE(manifest.isOpen(CollectionEntry::vegetable));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:cucumber", CollectionEntry::vegetable}));
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)
                                        .add(CollectionEntry::vegetable2)));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:cucumber", CollectionEntry::vegetable2}));
    EXPECT_TRUE(manifest.isOpen(CollectionEntry::vegetable2));
    EXPECT_TRUE(manifest.isDeleting(CollectionEntry::vegetable));
}

TEST_F(VBucketManifestTest, updates) {
    EXPECT_TRUE(manifest.checkSize(1));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::defaultC));

    EXPECT_TRUE(manifest.update(cm));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::vegetable));

    EXPECT_TRUE(manifest.update(cm.add(CollectionEntry::fruit)));
    EXPECT_TRUE(manifest.checkSize(3));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::fruit));

    EXPECT_TRUE(manifest.update(
            cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)));
    EXPECT_TRUE(manifest.checkSize(5));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::meat));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::meat));
}

TEST_F(VBucketManifestTest, updates2) {
    EXPECT_TRUE(manifest.update(cm.add(CollectionEntry::fruit)
                                        .add(CollectionEntry::meat)
                                        .add(CollectionEntry::dairy)));
    EXPECT_TRUE(manifest.checkSize(5));

    // Remove meat and dairy, size is not affected because the delete is only
    // starting
    EXPECT_TRUE(manifest.update(
            cm.remove(CollectionEntry::meat).remove(CollectionEntry::dairy)));
    EXPECT_TRUE(manifest.checkSize(5));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::meat));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::dairy));

    // But vegetable is accessible, the others are locked out
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"dairy:milk", CollectionEntry::dairy}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"meat:chicken", CollectionEntry::meat}));
}

TEST_F(VBucketManifestTest, updates3) {
    EXPECT_TRUE(manifest.update(cm.add(CollectionEntry::fruit)
                                        .add(CollectionEntry::meat)
                                        .add(CollectionEntry::dairy)));
    EXPECT_TRUE(manifest.checkSize(5));

    // Remove everything
    CollectionsManifest cm2(NoDefault{});
    EXPECT_TRUE(manifest.update(cm2));
    EXPECT_TRUE(manifest.checkSize(5));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::defaultC));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::vegetable));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::fruit));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::meat));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::dairy));

    // But vegetable is accessible, the others are 'locked' out
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"dairy:milk", CollectionEntry::dairy}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"meat:chicken", CollectionEntry::meat}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"fruit:apple", CollectionEntry::fruit}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
}

TEST_F(VBucketManifestTest, add_beginDelete_add) {
    // remove default and add vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::defaultC)));
    auto seqno = manifest.getLastSeqno(); // seqno of the vegetable addition
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::vegetable));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));

    // The first manifest.update has dropped default collection and added
    // vegetable - test $default key with a seqno it could of existed with
    EXPECT_TRUE(manifest.isLogicallyDeleted(
            {"anykey", DocNamespace::DefaultCollection}, seqno - 1));
    // But vegetable is still good
    EXPECT_FALSE(manifest.isLogicallyDeleted(
            {"vegetable:carrot", CollectionEntry::vegetable}, seqno));

    // remove vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));
    seqno = manifest.getLastSeqno();
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::vegetable));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));

    // vegetable is now a deleting collection
    EXPECT_TRUE(manifest.isLogicallyDeleted(
            {"vegetable:carrot", CollectionEntry::vegetable}, seqno));

    // add vegetable a second time
    EXPECT_TRUE(manifest.update(cm.add(CollectionEntry::vegetable2)));
    auto oldSeqno = seqno;
    auto newSeqno = manifest.getLastSeqno();
    EXPECT_TRUE(manifest.checkSize(3));
    EXPECT_TRUE(manifest.isOpen(CollectionEntry::vegetable2));
    EXPECT_TRUE(manifest.isDeleting(CollectionEntry::vegetable));

    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable2}));

    // Now we expect older vegetables to be deleting and newer not to be.
    EXPECT_FALSE(manifest.isLogicallyDeleted(
            {"vegetable:carrot", CollectionEntry::vegetable2}, newSeqno));
    EXPECT_TRUE(manifest.isLogicallyDeleted(
            {"vegetable:carrot", CollectionEntry::vegetable}, oldSeqno));
}

TEST_F(VBucketManifestTest, add_beginDelete_delete) {
    EXPECT_TRUE(manifest.update(cm));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::vegetable));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));

    // remove vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));
    auto seqno = manifest.getLastSeqno();
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::vegetable));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));
    EXPECT_TRUE(manifest.isLogicallyDeleted(
            {"vegetable:carrot", CollectionEntry::vegetable}, seqno));

    // finally remove vegetable
    EXPECT_TRUE(manifest.completeDeletion(CollectionEntry::vegetable));
    EXPECT_TRUE(manifest.checkSize(1));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));
}

TEST_F(VBucketManifestTest, add_beginDelete_add_delete) {
    EXPECT_TRUE(manifest.update(cm));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::vegetable));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));

    // remove vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));
    EXPECT_TRUE(manifest.checkSize(2));
    EXPECT_TRUE(manifest.isExclusiveDeleting(CollectionEntry::vegetable));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));

    // add vegetable:2
    EXPECT_TRUE(manifest.update(cm.add(CollectionEntry::vegetable2)));
    EXPECT_TRUE(manifest.checkSize(3));
    EXPECT_TRUE(manifest.isOpen(CollectionEntry::vegetable2));
    EXPECT_TRUE(manifest.isDeleting(CollectionEntry::vegetable));

    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable2}));

    // finally remove vegetable:1
    EXPECT_TRUE(manifest.completeDeletion(CollectionEntry::vegetable));
    EXPECT_TRUE(manifest.checkSize(2));

    // No longer OpenAndDeleting, now ExclusiveOpen
    EXPECT_TRUE(manifest.isExclusiveOpen(CollectionEntry::vegetable2));

    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable2}));
}

TEST_F(VBucketManifestTest, invalidDeletes) {
    // add vegetable
    EXPECT_TRUE(manifest.update(cm));
    // Delete vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));

    // Invalid CID
    EXPECT_FALSE(manifest.completeDeletion(100));
    EXPECT_FALSE(manifest.completeDeletion(500));

    EXPECT_TRUE(manifest.completeDeletion(CollectionEntry::vegetable));
}

// Check that a deleting collection doesn't keep adding system events
TEST_F(VBucketManifestTest, doubleDelete) {
    auto seqno = manifest.getActiveVB().getHighSeqno();
    // add vegetable
    EXPECT_TRUE(manifest.update(cm));
    EXPECT_LT(seqno, manifest.getActiveVB().getHighSeqno());
    seqno = manifest.getActiveVB().getHighSeqno();

    // Apply same manifest (different revision). Nothing will be created or
    // deleted. Apply direct to vbm, not via manifest.update as that would
    // complain about the lack of events
    manifest.getActiveManifest().wlock().update(manifest.getActiveVB(), {cm});

    EXPECT_EQ(seqno, manifest.getActiveVB().getHighSeqno());
    seqno = manifest.getActiveVB().getHighSeqno();

    // Now delete vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));

    EXPECT_LT(seqno, manifest.getActiveVB().getHighSeqno());
    seqno = manifest.getActiveVB().getHighSeqno();

    // same again, should have nothing created or deleted
    manifest.getActiveManifest().wlock().update(manifest.getActiveVB(), {cm});

    EXPECT_EQ(seqno, manifest.getActiveVB().getHighSeqno());
}

TEST_F(VBucketManifestTest, replica_add_remove) {
    // add vegetable
    EXPECT_TRUE(manifest.update(cm));

    // add meat & dairy
    EXPECT_TRUE(manifest.update(
            cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)));

    // remove vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));

    // remove $default
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::defaultC)));

    // Check we can access the remaining collections
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"vegetable:carrot", CollectionEntry::vegetable}));
    EXPECT_FALSE(manifest.doesKeyContainValidCollection(
            {"anykey", DocNamespace::DefaultCollection}));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"meat:sausage", CollectionEntry::meat}));
    EXPECT_TRUE(manifest.doesKeyContainValidCollection(
            {"dairy:butter", CollectionEntry::dairy}));
}

TEST_F(VBucketManifestTest, replica_add_remove_completeDelete) {
    // add vegetable
    EXPECT_TRUE(manifest.update(cm));

    // remove vegetable
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));

    // Finish removal of vegetable
    EXPECT_TRUE(manifest.completeDeletion(CollectionEntry::vegetable));
}

class VBucketManifestTestEndSeqno : public VBucketManifestTest {};

TEST_F(VBucketManifestTestEndSeqno, singleAdd) {
    EXPECT_TRUE(
            manifest.checkGreatestEndSeqno(StoredValue::state_collection_open));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(0));
    EXPECT_TRUE(manifest.update(cm));
    EXPECT_TRUE(
            manifest.checkGreatestEndSeqno(StoredValue::state_collection_open));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(0));
    EXPECT_FALSE(manifest.isLogicallyDeleted(
            {"vegetable:sprout", CollectionEntry::vegetable}, 1));
}

TEST_F(VBucketManifestTestEndSeqno, singleDelete) {
    EXPECT_TRUE(
            manifest.checkGreatestEndSeqno(StoredValue::state_collection_open));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(0));
    // remove all collections
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::defaultC)
                                        .remove(CollectionEntry::vegetable)));

    EXPECT_TRUE(manifest.checkGreatestEndSeqno(1));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(1));
    EXPECT_TRUE(manifest.isLogicallyDeleted(
            {"vegetable:sprout", DocNamespace::DefaultCollection}, 1));
    EXPECT_FALSE(manifest.isLogicallyDeleted(
            {"vegetable:sprout", DocNamespace::DefaultCollection}, 2));
    EXPECT_TRUE(manifest.completeDeletion(CollectionID::DefaultCollection));
    EXPECT_TRUE(
            manifest.checkGreatestEndSeqno(StoredValue::state_collection_open));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(0));
}

TEST_F(VBucketManifestTestEndSeqno, addDeleteAdd) {
    EXPECT_TRUE(
            manifest.checkGreatestEndSeqno(StoredValue::state_collection_open));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(0));

    // Add
    EXPECT_TRUE(manifest.update(cm));

    // Delete
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));

    EXPECT_TRUE(manifest.checkGreatestEndSeqno(2));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(1));
    EXPECT_TRUE(manifest.isLogicallyDeleted(
            {"vegetable:sprout", CollectionEntry::vegetable}, 1));

    EXPECT_FALSE(manifest.isLogicallyDeleted(
            {"vegetable:sprout", CollectionEntry::vegetable}, 3));

    EXPECT_TRUE(manifest.update(cm.add(CollectionEntry::vegetable2)));

    EXPECT_TRUE(manifest.checkGreatestEndSeqno(2));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(1));
    EXPECT_TRUE(manifest.isLogicallyDeleted(
            {"vegetable:sprout", CollectionEntry::vegetable}, 1));

    EXPECT_FALSE(manifest.isLogicallyDeleted(
            {"vegetable:sprout", CollectionEntry::vegetable}, 3));

    EXPECT_TRUE(manifest.completeDeletion(CollectionEntry::vegetable));
    EXPECT_TRUE(
            manifest.checkGreatestEndSeqno(StoredValue::state_collection_open));
    EXPECT_TRUE(manifest.checkNumDeletingCollections(0));
}

class VBucketManifestCachingReadHandle : public VBucketManifestTest {};

TEST_F(VBucketManifestCachingReadHandle, basic) {
    // Add
    EXPECT_TRUE(manifest.update(cm));

    {
        auto rh = manifest.active.lock(
                {"vegetable:v1", CollectionEntry::vegetable});
        EXPECT_TRUE(rh.valid());
        EXPECT_EQ(CollectionEntry::vegetable.getId(),
                  rh.getKey().getDocNamespace());
        EXPECT_STREQ("vegetable:v1",
                     reinterpret_cast<const char*>(rh.getKey().data()));
    }

    {
        auto rh = manifest.active.lock({"fruit:v1", CollectionEntry::fruit});
        EXPECT_FALSE(rh.valid());

        // cached the key
        EXPECT_EQ(CollectionEntry::fruit.getId(),
                  rh.getKey().getDocNamespace());
        EXPECT_STREQ("fruit:v1",
                     reinterpret_cast<const char*>(rh.getKey().data()));
    }
    EXPECT_TRUE(manifest.update(cm.remove(CollectionEntry::vegetable)));

    {
        auto rh = manifest.active.lock(
                {"vegetable:v1", CollectionEntry::vegetable});
        EXPECT_FALSE(rh.valid());

        EXPECT_EQ(CollectionEntry::vegetable.getId(),
                  rh.getKey().getDocNamespace());
        EXPECT_STREQ("vegetable:v1",
                     reinterpret_cast<const char*>(rh.getKey().data()));
    }
    {
        auto rh = manifest.active.lock({"fruit:v1", CollectionEntry::fruit});
        EXPECT_FALSE(rh.valid());
        EXPECT_EQ(CollectionEntry::fruit.getId(),
                  rh.getKey().getDocNamespace());
        EXPECT_STREQ("fruit:v1",
                     reinterpret_cast<const char*>(rh.getKey().data()));
    }
}
