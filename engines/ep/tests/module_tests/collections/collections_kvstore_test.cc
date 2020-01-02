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
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/kvstore_test.h"

/// Dummy callback to replace the flusher callback so we can create VBuckets
class DummyCB : public Callback<Vbid> {
public:
    DummyCB() {
    }

    void callback(Vbid& dummy) {
    }
};

struct WriteCallback {
    void operator()(TransactionContext&, KVStore::MutationSetResultState) {
    }
};

struct DeleteCallback {
    void operator()(TransactionContext&, KVStore::MutationStatus) {
    }
};

class CollectionsKVStoreTest : public KVStoreParamTest {
public:
    CollectionsKVStoreTest()
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

    void applyEvents(const CollectionsManifest& cm) {
        manifest.wlock().update(vbucket, {cm});

        std::vector<queued_item> events;
        getEventsFromCheckpoint(events);

        for (auto& ev : events) {
            if (ev->isDeleted()) {
                kvstore->delSystemEvent(*ev, dc);
            } else {
                kvstore->setSystemEvent(*ev, wc);
            }
        }
    }

    void checkUid(const Collections::KVStore::Manifest& md,
                  const CollectionsManifest& cm) {
        EXPECT_EQ(cm.getUid(), md.manifestUid);
    }

    void checkCollections(
            const Collections::KVStore::Manifest& md,
            const CollectionsManifest& cm,
            size_t expectedMatches,
            std::vector<CollectionID> expectedDropped = {}) const {
        EXPECT_EQ(expectedMatches, md.collections.size());
        auto expected = cm.getCreateEventVector();

        EXPECT_EQ(expectedMatches, expected.size());

        size_t matched = 0;

        // No ordering expectations from KVStore, so compare all
        for (const auto& e : expected) {
            for (const auto& c : md.collections) {
                if (c.metaData == e) {
                    matched++;
                    if (expectedMatches == matched) {
                        break; // done
                    }
                }
            }
        }
        EXPECT_EQ(expectedMatches, matched);

        auto dropped = kvstore->getDroppedCollections(Vbid(0));
        if (!expectedDropped.empty()) {
            EXPECT_TRUE(md.droppedCollectionsExist);
            matched = 0;

            EXPECT_EQ(expectedDropped.size(), dropped.size());
            for (const auto cid : expectedDropped) {
                auto p = [cid](const Collections::KVStore::DroppedCollection&
                                       dropped) {
                    return dropped.collectionId == cid;
                };
                auto found = std::find_if(dropped.begin(), dropped.end(), p);
                if (found != dropped.end()) {
                    matched++;
                    if (expectedDropped.size() == matched) {
                        break; // done
                    }
                }
            }
            EXPECT_EQ(expectedDropped.size(), matched);
        } else {
            EXPECT_FALSE(md.droppedCollectionsExist);
            EXPECT_TRUE(dropped.empty());
        }
    }

    void checkScopes(const Collections::KVStore::Manifest& md,
                     const CollectionsManifest& cm,
                     int expectedMatches) const {
        auto expectedScopes = cm.getScopeIdVector();
        EXPECT_EQ(expectedMatches, expectedScopes.size());
        EXPECT_EQ(expectedMatches, md.scopes.size());

        int matched = 0;
        for (const auto sid : expectedScopes) {
            auto found = std::find(md.scopes.begin(), md.scopes.end(), sid);
            if (found != md.scopes.end()) {
                matched++;
                if (expectedMatches == matched) {
                    break; // done
                }
            }
        }
        EXPECT_EQ(expectedMatches, matched);
    }

    void applyAndCheck(const CollectionsManifest& cm,
                       int expectedCollections,
                       int expectedScopes,
                       std::vector<CollectionID> expectedDropped = {}) {
        kvstore->begin(std::make_unique<TransactionContext>(vbucket.getId()));
        applyEvents(cm);
        kvstore->commit(flush);
        auto md = kvstore->getCollectionsManifest(Vbid(0));
        checkUid(md, cm);
        checkCollections(md, cm, expectedCollections, expectedDropped);
        checkScopes(md, cm, expectedScopes);
    };

protected:
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    EPVBucket vbucket;
    WriteCallback wc;
    DeleteCallback dc;
};

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
    EXPECT_FALSE(md.collections[0].metaData.maxTtl.is_initialized());

    EXPECT_EQ(ScopeID::Default, md.scopes[0]);
    EXPECT_EQ(0, md.manifestUid);
}

TEST_P(CollectionsKVStoreTest, one_update) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable);
    applyAndCheck(cm, 2, 1);
}

TEST_P(CollectionsKVStoreTest, two_updates) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable).add(CollectionEntry::fruit);
    applyAndCheck(cm, 3, 1);
}

TEST_P(CollectionsKVStoreTest, updates_with_scopes) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::vegetable, ScopeEntry::shop1);
    cm.add(ScopeEntry::shop2).add(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm, 3, 3);
}

TEST_P(CollectionsKVStoreTest, updates_between_commits) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::vegetable, ScopeEntry::shop1);
    applyAndCheck(cm, 2, 2);
    cm.add(ScopeEntry::shop2).add(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm, 3, 3);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    applyAndCheck(cm, 4, 3);
}

TEST_P(CollectionsKVStoreTest, updates_and_drops_between_commits) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1)
            .add(CollectionEntry::vegetable, ScopeEntry::shop1);
    applyAndCheck(cm, 2, 2);
    cm.add(ScopeEntry::shop2).add(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm, 3, 3);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    applyAndCheck(cm, 4, 3);
    cm.remove(CollectionEntry::fruit, ScopeEntry::shop2);
    applyAndCheck(cm, 3, 3, {CollectionUid::fruit});
    cm.remove(CollectionEntry::meat, ScopeEntry::shop2);
    applyAndCheck(cm, 2, 3, {CollectionUid::fruit, CollectionUid::meat});
    cm.remove(CollectionEntry::vegetable, ScopeEntry::shop1);
    applyAndCheck(cm,
                  1,
                  3,
                  {CollectionUid::fruit,
                   CollectionUid::meat,
                   CollectionUid::vegetable});
    cm.remove(CollectionEntry::defaultC);
    applyAndCheck(cm,
                  0,
                  3,
                  {CollectionUid::fruit,
                   CollectionUid::meat,
                   CollectionUid::vegetable,
                   CollectionUid::defaultC});
}

// Recreate a dropped collection not ideal whilst it still exists
TEST_P(CollectionsKVStoreTest, add_of_dropped) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable);
    applyAndCheck(cm, 2, 1);
    cm.remove(CollectionEntry::vegetable);
    applyAndCheck(cm, 1, 1, {CollectionUid::vegetable});
    cm.add(CollectionEntry::vegetable);
    try {
        applyAndCheck(cm, 1, 1);
        FAIL() << "Expected an exception";
    } catch (const std::exception& e) {
        EXPECT_STREQ(
                "CouchKVStore::updateOpenCollections found a new collection in "
                "dropped list, cid:0xa",
                e.what());
    }
}

static std::string kvstoreTestParams[] = {"couchdb"};

INSTANTIATE_TEST_CASE_P(CollectionsKVStoreTests,
                        CollectionsKVStoreTest,
                        ::testing::ValuesIn(kvstoreTestParams),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
