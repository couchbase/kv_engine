/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "configuration.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore_config.h"
#include "stats.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/kvstore_test.h"
#include <utilities/test_manifest.h>

struct WriteCallback {
    void operator()(TransactionContext&, KVStore::FlushStateMutation) {
    }
};

struct DeleteCallback {
    void operator()(TransactionContext&, KVStore::FlushStateDeletion) {
    }
};

class CollectionsKVStoreTestBase : public KVStoreBackend, public KVStoreTest {
public:
    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<Vbid> {
    public:
        DummyCB() {
        }

        void callback(Vbid& dummy) override {
        }
    };

    CollectionsKVStoreTestBase()
        : vbucket(Vbid(0),
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
                  SyncWriteResolvedCallback{},
                  NoopSyncWriteCompleteCb,
                  NoopSeqnoAckCb,
                  config,
                  EvictionPolicy::Value,
                  std::make_unique<Collections::VB::Manifest>()) {
    }

    void getEventsFromCheckpoint(std::vector<queued_item>& events) {
        std::vector<queued_item> items;
        vbucket.checkpointManager->getNextItemsForPersistence(items);
        for (const auto& qi : items) {
            if (qi->getOperation() == queue_op::system_event) {
                events.push_back(qi);
            }
        }

        ASSERT_FALSE(events.empty())
                << "getEventsFromCheckpoint: no events in " << vbucket.getId();
    }

    void applyEvents(VB::Commit& commitData, const CollectionsManifest& cm) {
        manifest.update(vbucket, makeManifest(cm));

        std::vector<queued_item> events;
        getEventsFromCheckpoint(events);

        for (auto& ev : events) {
            commitData.collections.recordSystemEvent(*ev);
            if (ev->isDeleted()) {
                kvstore->delSystemEvent(ev);
            } else {
                kvstore->setSystemEvent(ev);
            }
        }
    }

    void applyEvents(const CollectionsManifest& cm) {
        applyEvents(flush, cm);
    }

    void checkUid(const Collections::KVStore::Manifest& md,
                  const CollectionsManifest& cm) {
        EXPECT_EQ(cm.getUid(), md.manifestUid);
    }

    static std::vector<Collections::CollectionMetaData> getCreateEventVector(
            const CollectionsManifest& cm) {
        std::vector<Collections::CollectionMetaData> rv;
        auto& json = cm.getJson();
        for (const auto& scope : json["scopes"]) {
            for (const auto& collection : scope["collections"]) {
                cb::ExpiryLimit maxTtl;
                auto ttl = collection.find("maxTTL");
                if (ttl != collection.end()) {
                    maxTtl = std::chrono::seconds(ttl->get<int32_t>());
                }
                ScopeID sid = Collections::makeScopeID(
                        scope["uid"].get<std::string>());
                CollectionID cid = Collections::makeCollectionID(
                        collection["uid"].get<std::string>());
                auto name = collection["name"].get<std::string>();

                rv.push_back({sid, cid, name, maxTtl});
            }
        }
        return rv;
    }

    static std::vector<Collections::ScopeMetaData> getScopeEventVector(
            const CollectionsManifest& cm) {
        std::vector<Collections::ScopeMetaData> rv;
        auto& json = cm.getJson();
        for (const auto& scope : json["scopes"]) {
            rv.push_back(
                    {Collections::makeScopeID(scope["uid"].get<std::string>()),
                     scope["name"].get<std::string>()});
        }
        return rv;
    }

    void checkCollections(
            const Collections::KVStore::Manifest& md,
            const CollectionsManifest& cm,
            std::vector<CollectionID> expectedDropped = {}) const {
        auto expected = getCreateEventVector(cm);
        EXPECT_EQ(expected.size(), md.collections.size());
        for (const auto& expectedCollection : expected) {
            auto cmp = [&expectedCollection](
                               const Collections::KVStore::OpenCollection&
                                       openCollection) {
                return openCollection.metaData == expectedCollection;
            };
            auto found = std::find_if(
                    md.collections.begin(), md.collections.end(), cmp);
            EXPECT_NE(found, md.collections.end());
        }

        auto dropped = kvstore->getDroppedCollections(Vbid(0));
        if (!expectedDropped.empty()) {
            EXPECT_TRUE(md.droppedCollectionsExist);
            EXPECT_EQ(expectedDropped.size(), dropped.size());
            for (const auto cid : expectedDropped) {
                auto cmp = [cid](const Collections::KVStore::DroppedCollection&
                                         dropped) {
                    return dropped.collectionId == cid;
                };
                auto found = std::find_if(dropped.begin(), dropped.end(), cmp);
                EXPECT_NE(found, dropped.end());
            }
        } else {
            EXPECT_FALSE(md.droppedCollectionsExist);
            EXPECT_TRUE(dropped.empty());
        }
    }

    void checkScopes(const Collections::KVStore::Manifest& md,
                     const CollectionsManifest& cm) const {
        auto expectedScopes = getScopeEventVector(cm);
        EXPECT_EQ(expectedScopes.size(), md.scopes.size());
        for (const auto scope : expectedScopes) {
            auto cmp =
                    [scope](const Collections::KVStore::OpenScope& openScope) {
                        return openScope.metaData == scope;
                    };
            auto found = std::find_if(md.scopes.begin(), md.scopes.end(), cmp);
            EXPECT_NE(found, md.scopes.end());
        }
    }

    /**
     * Apply all of the system events to KVStore, which will in turn generate
     * and/or update the _local meta-data for collections. Finally check the
     * persisted meta-data is equal to the given "CollectionsManifest" and
     * optionally check that the persisted list of dropped collections matches
     * the given list
     */
    void applyAndCheck(const CollectionsManifest& cm,
                       std::vector<CollectionID> expectedDropped = {}) {
        VB::Commit commitData(manifest);
        kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
        applyEvents(commitData, cm);
        kvstore->commit(commitData);
        auto md = kvstore->getCollectionsManifest(Vbid(0));
        checkUid(md, cm);
        checkCollections(md, cm, expectedDropped);
        checkScopes(md, cm);
    };

protected:
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    EPVBucket vbucket;
    WriteCallback wc;
    DeleteCallback dc;
};

class CollectionsKVStoreTest
    : public CollectionsKVStoreTestBase,
      public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        KVStoreTest::SetUp();
        KVStoreBackend::setup(data_dir, GetParam());
    }

    void TearDown() override {
        KVStoreBackend::teardown();
        KVStoreTest::TearDown();
    }
};

// validate some ==operators work as expected
TEST(CollectionsKVStoreTest, test_KVStore_comparison) {
    using namespace Collections::KVStore;
    Manifest empty{Manifest::Empty{}};
    EXPECT_EQ(empty, empty);
    Manifest m1{Manifest::Default{}};
    EXPECT_EQ(m1, m1);
    EXPECT_NE(empty, m1);

    Manifest m2{Manifest::Default{}};
    EXPECT_EQ(m1, m2);
    m2.manifestUid = 1;
    EXPECT_NE(m1, m2);
    m1.manifestUid = m2.manifestUid;

    EXPECT_EQ(m1, m2);
    m2.droppedCollectionsExist = true;
    EXPECT_NE(m1, m2);
    m1.droppedCollectionsExist = m2.droppedCollectionsExist;

    EXPECT_EQ(m1, m2);
    m2.collections.push_back(
            OpenCollection{0, Collections::CollectionMetaData{}});
    EXPECT_NE(m1, m2);
    m2.collections = m1.collections;

    EXPECT_EQ(m1, m2);
    m2.collections.push_back(OpenCollection{
            0,
            Collections::CollectionMetaData{
                    ScopeID{88}, CollectionID{101}, "c101", {}}});
    EXPECT_NE(m1, m2);
    m2.collections = m1.collections;

    EXPECT_EQ(m1, m2);
    m2.scopes.push_back(OpenScope{0, Collections::ScopeMetaData{}});
    EXPECT_NE(m1, m2);
    m2.scopes = m1.scopes;

    EXPECT_EQ(m1, m2);
    m2.scopes.push_back(
            OpenScope{0, Collections::ScopeMetaData{ScopeID{91}, "s91"}});
    EXPECT_NE(m1, m2);
}

TEST_P(CollectionsKVStoreTest, initial_meta) {
    // Ask the kvstore for the initial meta
    auto md = kvstore->getCollectionsManifest(Vbid(0));

    // Expect 1 collection and 1 scope
    EXPECT_EQ(1, md.collections.size());
    EXPECT_EQ(1, md.scopes.size());

    // It's the default collection and the default scope
    EXPECT_EQ(0, md.collections[0].startSeqno);
    EXPECT_EQ("_default", md.collections[0].metaData.name);
    EXPECT_EQ(CollectionID::Default, md.collections[0].metaData.cid);
    EXPECT_EQ(ScopeID::Default, md.collections[0].metaData.sid);
    EXPECT_FALSE(md.collections[0].metaData.maxTtl.has_value());

    EXPECT_EQ(0, md.scopes[0].startSeqno);
    EXPECT_EQ(ScopeID::Default, md.scopes[0].metaData.sid);
    EXPECT_EQ("_default", md.scopes[0].metaData.name);

    EXPECT_EQ(0, md.manifestUid);
}

TEST_P(CollectionsKVStoreTest, one_update) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable);
    applyAndCheck(cm);
}

TEST_P(CollectionsKVStoreTest, two_updates) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable).add(CollectionEntry::fruit);
    applyAndCheck(cm);
}

TEST_P(CollectionsKVStoreTest, updates_with_scopes) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::vegetable, ScopeEntry::shop1);
    cm.add(ScopeEntry::shop2).add(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm);
}

TEST_P(CollectionsKVStoreTest, updates_between_commits) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::vegetable, ScopeEntry::shop1);
    applyAndCheck(cm);
    cm.add(ScopeEntry::shop2).add(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    applyAndCheck(cm);
}

TEST_P(CollectionsKVStoreTest, updates_and_drops_between_commits) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::vegetable, ScopeEntry::shop1);
    applyAndCheck(cm);
    cm.add(ScopeEntry::shop2).add(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    applyAndCheck(cm);
    cm.remove(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm, {CollectionUid::fruit});
    cm.remove(CollectionEntry::meat, ScopeEntry::shop2);
    applyAndCheck(cm, {CollectionUid::fruit, CollectionUid::meat});
    cm.remove(CollectionEntry::vegetable, ScopeEntry::shop1);
    applyAndCheck(cm,
                  {CollectionUid::fruit,
                   CollectionUid::meat,
                   CollectionUid::vegetable});
    cm.remove(CollectionEntry::defaultC);
    applyAndCheck(cm,

                  {CollectionUid::fruit,
                   CollectionUid::meat,
                   CollectionUid::vegetable,
                   CollectionUid::defaultC});
}

// Test that KV can handle multiple system events in a single 'commit'
// batch. Multiple events can legitimately occur if a failure occurs in the
// cluster meaning some changes to the collection configuration were lost
// and KV is forced to go 'backwards' or onto another 'time-line'. For
// example collection{ID:8, name:"A1"} is created in manifest 5, but a
// failure occurs and manifest 5 is lost, an alternative manifest 5 can
// exist where collection{ID:8 name:"B7"} could be created. KV could end up
// with a create, drop and create for the collection with ID:8 (but a new name
// in the final creation). When these multiple events occur, the state of
// the KVStore meta-data must reflect what has happened.
//
// In general:
// - The open collections meta-data stores 1 entry for each open collection
//   *and* it must be the most recent (by-seqno).
// - The dropped collections meta-data stores 1 entry for each dropped
//   collection, each entry must store the start/end to span the earliest
//   create, to the most recent (by-seqno) drop.
// - A collection can be in both open and dropped lists (create/drop/create...)
class CollectionRessurectionKVStoreTest
    : public CollectionsKVStoreTestBase,
      public ::testing::WithParamInterface<
              std::tuple<std::string, int, bool, bool, int>> {
public:
    std::string getBackend() const {
        return std::get<0>(GetParam());
    }

    /// @return how many cycles the 'core' of test will run for
    int getCycles() const {
        return std::get<1>(GetParam());
    }

    /// @return true if the test cycle(s) should finish with the target
    /// collection dropped
    bool dropCollectionAtEnd() const {
        return std::get<2>(GetParam());
    }

    /// @return true if each test cycle should resurrect the target collection
    /// with a new name
    bool resurectWithNewName() const {
        return std::get<3>(GetParam());
    }

    /// @return a function (or not) to be used before the main test
    std::function<void()> getPrologue() {
        switch (std::get<4>(GetParam())) {
        case 0:
            return {};
        case 1:
            return std::bind(&CollectionRessurectionKVStoreTest::openCollection,
                             this);
        case 2:
            return std::bind(&CollectionRessurectionKVStoreTest::dropCollection,
                             this);
        }
        EXPECT_FALSE(true) << "No prologue defined for parameter:"
                           << std::get<4>(GetParam());
        return {};
    }

    /// @return a function (or not) to be used before the main test
    std::function<void()> getScopesPrologue() {
        switch (std::get<4>(GetParam())) {
        case 0:
            return {};
        case 1:
            return std::bind(
                    &CollectionRessurectionKVStoreTest::openScopeOpenCollection,
                    this);
        case 2:
            return std::bind(&CollectionRessurectionKVStoreTest::dropScope,
                             this);
        }
        EXPECT_FALSE(true) << "No prologue defined for parameter:"
                           << std::get<4>(GetParam());
        return {};
    }

    void SetUp() override {
        KVStoreTest::SetUp();
        KVStoreBackend::setup(data_dir, getBackend());
    }

    void TearDown() override {
        KVStoreBackend::teardown();
        KVStoreTest::TearDown();
    }

    // runs a flush batch that will leave the target collection in open state
    void openCollection() {
        cm.add(target);
        kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
        applyEvents(cm);
        kvstore->commit(flush);
    }

    // runs a flush batch that will leave the target collection in dropped state
    void dropCollection() {
        openCollection();
        cm.remove(target);
        kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
        applyEvents(cm);
        kvstore->commit(flush);
    }

    // runs a flush batch that will leave the target collection in open state
    void openScopeOpenCollection() {
        kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
        cm.add(targetScope);
        applyEvents(cm);
        cm.add(target, targetScope);
        applyEvents(cm);
        kvstore->commit(flush);
    }

    // runs a flush batch that will leave the target collection in dropped state
    void dropScope() {
        openScopeOpenCollection();
        cm.remove(targetScope);
        kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
        applyEvents(cm);
        kvstore->commit(flush);
    }

    void resurectionTest();
    void resurectionScopesTest();

    CollectionEntry::Entry target = CollectionEntry::vegetable;
    ScopeEntry::Entry targetScope = ScopeEntry::shop1;
    CollectionsManifest cm;
};

void CollectionRessurectionKVStoreTest::resurectionTest() {
    ASSERT_GT(getCycles(), 0) << "Require at least 1 cycle";

    // A 'prologue' function can be ran, this will create meta-data that gets
    // merged by the second commit batch
    auto prologue = getPrologue();
    if (prologue) {
        prologue();
    }

    // The interesting 'test' code runs from this begin to the following commit.
    // The test will run cycles of create/drop, so that the collection
    // has multiple generations within a single flush batch, we can then verify
    // that the meta-data stored by commit is correct
    kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
    if (!cm.exists(target)) {
        cm.add(target);
        applyEvents(cm);
    }

    CollectionEntry::Entry collection = target;

    // iterate cycles of remove/add
    for (int ii = 0; ii < getCycles(); ii++) {
        cm.remove(collection);
        applyEvents(cm);

        if (resurectWithNewName()) {
            collection.name = target.name + "_" + std::to_string(ii);
        }

        cm.add(collection);
        applyEvents(cm);
    }

    if (dropCollectionAtEnd()) {
        cm.remove(collection);
        applyEvents(cm);
    }
    kvstore->commit(flush);

    // Now validate
    auto md = kvstore->getCollectionsManifest(Vbid(0));
    checkUid(md, cm);
    checkCollections(md, cm, {target.uid});

    auto seqno = vbucket.getHighSeqno();

    // Finally validate the seqnos the local data stores (checkCollections
    // only compares name/uid/ttl from cm against md )
    for (const auto& collection : md.collections) {
        if (collection.metaData.cid == CollectionID::Default) {
            EXPECT_EQ(0, collection.startSeqno);
        } else if (collection.metaData.cid == target.uid) {
            EXPECT_EQ(2, md.collections.size());
            EXPECT_FALSE(dropCollectionAtEnd());
            EXPECT_EQ(seqno, collection.startSeqno);
        }
    }

    // Vegetable was dropped during the test, thus it must be part of the
    // drop list and it must span the very first create to the very last drop!
    auto droppedCollections = kvstore->getDroppedCollections(Vbid(0));
    EXPECT_TRUE(md.droppedCollectionsExist);
    EXPECT_EQ(1, droppedCollections.size()) << "Only vegetable was dropped";
    const auto& droppedMeta = droppedCollections[0];
    EXPECT_EQ(target.uid, droppedMeta.collectionId);
    // vegetable is always first created at seqno 1
    EXPECT_EQ(1, droppedMeta.startSeqno);
    // but can of been dropped many times
    if (dropCollectionAtEnd()) {
        EXPECT_EQ(seqno, droppedMeta.endSeqno);
    } else {
        // in this case seqno was assigned after create, so go back one for the
        // last drop of vegetable
        EXPECT_EQ(seqno - 1, droppedMeta.endSeqno);
    }
}

// Variant of test which uses non-default scope (and drop scope)
void CollectionRessurectionKVStoreTest::resurectionScopesTest() {
    ASSERT_GT(getCycles(), 0) << "Require at least 1 cycle";

    // A 'prologue' function can be ran, this will create meta-data that gets
    // merged by the second commit batch
    auto prologue = getScopesPrologue();
    if (prologue) {
        prologue();
    }

    // The interesting 'test' code runs from this begin to the following commit.
    // The test will run cycles of create/drop, so that the collection
    // has multiple generations within a single flush batch, we can then verify
    // that the meta-data stored by commit is correct
    kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
    if (!cm.exists(targetScope)) {
        cm.add(targetScope);
        applyEvents(cm);
        cm.add(target, targetScope);
        applyEvents(cm);
    }

    std::string expectedName = target.name;
    ScopeEntry::Entry scope = targetScope;

    // iterate cycles of remove/add
    for (int ii = 0; ii < getCycles(); ii++) {
        cm.remove(scope);
        applyEvents(cm);

        if (resurectWithNewName()) {
            expectedName = target.name + "_" + std::to_string(ii);
            scope.name = targetScope.name + "_" + std::to_string(ii);
        }
        cm.add(scope);
        applyEvents(cm);
        cm.add({expectedName, target.uid}, scope);
        applyEvents(cm);
    }

    if (dropCollectionAtEnd()) {
        cm.remove(scope);
        applyEvents(cm);
    }
    kvstore->commit(flush);

    // Now validate
    auto md = kvstore->getCollectionsManifest(Vbid(0));
    checkUid(md, cm);
    checkCollections(md, cm, {target.uid});

    auto seqno = vbucket.getHighSeqno();

    // Finally validate the seqnos the local data stores (checkCollections
    // only compares name/uid/ttl from cm against md )
    for (const auto& collection : md.collections) {
        if (collection.metaData.cid == CollectionID::Default) {
            EXPECT_EQ(0, collection.startSeqno);
        } else if (collection.metaData.cid == target.uid) {
            EXPECT_EQ(2, md.collections.size());

            EXPECT_FALSE(dropCollectionAtEnd());
            EXPECT_EQ(seqno, collection.startSeqno);
        }
    }

    // Validate scopes
    for (const auto& scope : md.scopes) {
        if (scope.metaData.sid == ScopeID::Default) {
            EXPECT_EQ(0, scope.startSeqno);
        } else if (scope.metaData.sid == targetScope.uid) {
            EXPECT_EQ(2, md.scopes.size());
            EXPECT_FALSE(dropCollectionAtEnd());
            EXPECT_EQ(seqno - 1, scope.startSeqno);
        }
    }

    // Vegetable was dropped during the test, thus it must be part of the
    // drop list and it must span the very first create to the very last drop!
    auto droppedCollections = kvstore->getDroppedCollections(Vbid(0));
    EXPECT_TRUE(md.droppedCollectionsExist);
    EXPECT_EQ(1, droppedCollections.size()) << "Only vegetable was dropped";
    const auto& droppedMeta = droppedCollections[0];
    EXPECT_EQ(target.uid, droppedMeta.collectionId);
    // vegetable is always first created at seqno 2 (after the scope)
    EXPECT_EQ(2, droppedMeta.startSeqno);
    // but can of been dropped many times
    if (dropCollectionAtEnd()) {
        EXPECT_EQ(seqno - 1, droppedMeta.endSeqno);
    } else {
        // in this case seqno was assigned after create, so go back 3 for the
        // last drop of vegetable (as the vents before drop collection are
        // drop scope, create scope, create collection
        EXPECT_EQ(seqno - 3, droppedMeta.endSeqno);
    }
}

TEST_P(CollectionRessurectionKVStoreTest, resurection) {
    resurectionTest();
}

TEST_P(CollectionRessurectionKVStoreTest, resurectionScopes) {
    resurectionScopesTest();
}

INSTANTIATE_TEST_SUITE_P(CollectionsKVStoreTests,
                         CollectionsKVStoreTest,
                         KVStoreParamTest::persistentConfigValues(),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

INSTANTIATE_TEST_SUITE_P(
        CollectionRessurectionKVStoreTests,
        CollectionRessurectionKVStoreTest,
        ::testing::Combine(KVStoreParamTest::persistentConfigValues(),
                           ::testing::Values(1, 3),
                           ::testing::Bool(),
                           ::testing::Bool(),
                           ::testing::Values(0, 1, 2)),
        [](const ::testing::TestParamInfo<
                std::tuple<std::string, int, bool, bool, int>>& info) {
            auto backend = std::get<0>(info.param);
            auto cycles = std::to_string(std::get<1>(info.param));
            auto dropAtEnd = std::to_string(std::get<2>(info.param));
            auto newName = std::to_string(std::get<3>(info.param));
            auto prologueSelection = std::to_string(std::get<4>(info.param));
            return backend + "_with_" + cycles + "cycles_" + dropAtEnd + "_" +
                   newName + "_" + prologueSelection;
        });
