/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "collections/collections_types.h"
#include "collections/events_generated.h"
#include "collections/vbucket_manifest_handles.h"
#include "configuration.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "item.h"
#include "kvstore/kvstore.h"
#include "kvstore/kvstore_config.h"
#include "kvstore/kvstore_transaction_context.h"
#include "stats.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/kvstore_test.h"

#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

struct WriteCallback {
    void operator()(TransactionContext&, FlushStateMutation) {
    }
};

struct DeleteCallback {
    void operator()(TransactionContext&, FlushStateDeletion) {
    }
};

class CollectionsKVStoreTestBase : public KVStoreBackend, public KVStoreTest {
public:
    /// Dummy callback to replace the flusher callback so we can create VBuckets
    class DummyCB : public Callback<Vbid> {
    public:
        DummyCB() = default;

        void callback(Vbid& dummy) override {
        }
    };

    CollectionsKVStoreTestBase() {
        config.parseConfiguration({});
        checkpoint_config = std::make_unique<CheckpointConfig>(config);

        vbucket = std::make_shared<EPVBucket>(
                Vbid(0),
                vbucket_state_active,
                global_stats,
                *checkpoint_config,
                /*kvshard*/ nullptr,
                /*lastSeqno*/ 0,
                /*lastSnapStart*/ 0,
                /*lastSnapEnd*/ 0,
                /*table*/ nullptr,
                std::make_shared<DummyCB>(),
                SyncWriteResolvedCallback{},
                NoopSyncWriteCompleteCb,
                NoopSyncWriteTimeoutFactory,
                NoopSeqnoAckCb,
                config,
                EvictionPolicy::Value,
                std::make_unique<Collections::VB::Manifest>(
                        std::make_shared<Collections::Manager>()));
    }

    CheckpointManager::ItemsForCursor getEventsFromCheckpoint(
            std::vector<queued_item>& events) {
        std::vector<queued_item> items;
        auto res =
                vbucket->checkpointManager->getNextItemsForPersistence(items);
        for (const auto& qi : items) {
            if (qi->getOperation() == queue_op::system_event) {
                events.push_back(qi);
            }
        }

        EXPECT_FALSE(events.empty())
                << "getEventsFromCheckpoint: no events in " << vbucket->getId();

        return res;
    }

    // This is a variation for when events are spread over many checkpoints.
    // this function will iterate until no more events can be found.
    void drainEventsFromCheckpoint(std::vector<queued_item>& events) {
        bool done = false;
        do {
            std::vector<queued_item> items;

            auto res = vbucket->checkpointManager->getNextItemsForPersistence(
                    items);
            for (const auto& qi : items) {
                if (qi->getOperation() == queue_op::system_event) {
                    events.push_back(qi);
                }
            }
            done = items.empty();
        } while (!done);

        EXPECT_FALSE(events.empty())
                << "drainEventsFromCheckpoint: no events in "
                << vbucket->getId();
    }

    void applyEvents(TransactionContext& txnCtx,
                     VB::Commit& commitData,
                     const CollectionsManifest& cm) {
        manifest.update(
                std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
                *vbucket,
                makeManifest(cm));

        std::vector<queued_item> events;
        auto res = getEventsFromCheckpoint(events);
        commitData.historical = res.historical;

        for (auto& ev : events) {
            commitData.collections.recordSystemEvent(*ev);
            if (ev->isDeleted()) {
                kvstore->delSystemEvent(txnCtx, ev);
            } else {
                kvstore->setSystemEvent(txnCtx, ev);
            }
        }
    }

    void applyEvents(TransactionContext& txnCtx,
                     const CollectionsManifest& cm) {
        applyEvents(txnCtx, flush, cm);
    }

    void checkUid(const Collections::KVStore::Manifest& md,
                  const CollectionsManifest& cm) {
        EXPECT_EQ(cm.getUid(), md.manifestUid);
    }

    // Function convert the test CollectionsManifest JSON into a vector of
    // CollectionMetaData objects. These objects are more convenient for test
    // comparisons
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
                ScopeID sid{scope["uid"].get<std::string>()};
                CollectionID cid{collection["uid"].get<std::string>()};
                auto name = collection["name"].get<std::string>();

                auto jsonMetered = collection.find("metered");
                auto metered = Collections::Metered::No;
                if (jsonMetered != collection.end()) {
                    // metered is specified in the manifest, use the value
                    metered = Collections::getMetered(jsonMetered->get<bool>());
                }

                bool historyValue{false};
                auto history = collection.find("history");
                if (history != collection.end()) {
                    historyValue = history->get<bool>();
                }

                Collections::ManifestUid flushUidValue;
                auto flushUid = collection.find("flush_uid");
                if (flushUid != collection.end()) {
                    flushUidValue = Collections::makeManifestUid(
                            flushUid->get<std::string>());
                }

                rv.emplace_back(sid,
                                cid,
                                name,
                                maxTtl,
                                getCanDeduplicateFromHistory(historyValue),
                                metered,
                                flushUidValue);
            }
        }
        return rv;
    }

    static std::vector<Collections::ScopeMetaData> getScopeEventVector(
            const CollectionsManifest& cm) {
        std::vector<Collections::ScopeMetaData> rv;
        auto& json = cm.getJson();
        for (const auto& scope : json["scopes"]) {
            rv.push_back({ScopeID{scope["uid"].get<std::string>()},
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
            EXPECT_TRUE(std::ranges::any_of(md.collections, cmp));
        }

        // Check for duplicates in kvstore array of collections
        for (const auto& e : md.collections) {
            auto cmp = [&e](const Collections::KVStore::OpenCollection& entry) {
                return e.metaData.cid == entry.metaData.cid;
            };
            EXPECT_EQ(1, std::ranges::count_if(md.collections, cmp));
        }

        auto [status, dropped] = kvstore->getDroppedCollections(Vbid(0));
        ASSERT_TRUE(status);
        if (!expectedDropped.empty()) {
            EXPECT_TRUE(md.droppedCollectionsExist);
            EXPECT_EQ(expectedDropped.size(), dropped.size());
            for (CollectionID cid : expectedDropped) {
                auto cmp = [cid](const Collections::KVStore::DroppedCollection&
                                         dropped) {
                    return dropped.collectionId == cid;
                };
                EXPECT_TRUE(std::ranges::any_of(dropped, cmp));
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
        for (const auto& scope : expectedScopes) {
            auto cmp =
                    [scope](const Collections::KVStore::OpenScope& openScope) {
                        return openScope.metaData == scope;
                    };
            EXPECT_TRUE(std::ranges::any_of(md.scopes, cmp));
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
        auto ctx = kvstore->begin(vbucket->getId(),
                                  std::make_unique<PersistenceCallback>());
        applyEvents(*ctx, commitData, cm);
        kvstore->commit(std::move(ctx), commitData);
        auto [status, md] = kvstore->getCollectionsManifest(Vbid(0));
        EXPECT_TRUE(status);
        checkUid(md, cm);
        checkCollections(md, cm, expectedDropped);
        checkScopes(md, cm);
    }

    // Compare the stored startSeqno of the collection
    void checkStartSeqno(CollectionID cid, uint64_t startSeqno) {
        auto [status, md] = kvstore->getCollectionsManifest(Vbid(0));
        EXPECT_TRUE(status);
        for (const auto& e : md.collections) {
            if (e.metaData.cid == cid) {
                EXPECT_EQ(e.startSeqno, startSeqno);
                return;
            }
        }
        FAIL() << "checkStartSeqno failed to find:" << cid << std::endl;
    }

protected:
    EPStats global_stats;
    std::unique_ptr<CheckpointConfig> checkpoint_config;
    Configuration config;
    VBucketPtr vbucket;
    WriteCallback wc;
    DeleteCallback dc;
};

class CollectionsKVStoreTest
    : public CollectionsKVStoreTestBase,
      public ::testing::WithParamInterface<std::string> {
public:
    void SetUp() override {
        KVStoreTest::SetUp();
        KVStoreBackend::setup(data_dir, GetParam());
    }

    void TearDown() override {
        KVStoreBackend::teardown();
        KVStoreTest::TearDown();
    }

    void failForDuplicate();
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
    m2.collections.emplace_back(0, Collections::CollectionMetaData{});
    EXPECT_NE(m1, m2);
    m2.collections = m1.collections;

    EXPECT_EQ(m1, m2);
    m2.collections.emplace_back(
            0,
            Collections::CollectionMetaData{ScopeID{88},
                                            CollectionID{101},
                                            "c101",
                                            {},
                                            CanDeduplicate::Yes,
                                            Collections::Metered::Yes,
                                            Collections::ManifestUid{}});
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

    m1 = m2;
    EXPECT_EQ(m1, m2);
    // Add a collection but check a different metered state is noticed
    auto c1 = Collections::CollectionMetaData{ScopeID{88},
                                              CollectionID{102},
                                              "c1",
                                              {},
                                              CanDeduplicate::Yes,
                                              Collections::Metered::No,
                                              Collections::ManifestUid{}};
    m1.collections.emplace_back(0, c1);
    c1.metered = Collections::Metered::Yes;
    m2.collections.emplace_back(0, c1);
    EXPECT_NE(m1, m2);

    m1 = m2;
    EXPECT_EQ(m1, m2);
    // Add a collection but check a different history state is noticed
    auto c2 = Collections::CollectionMetaData{ScopeID{88},
                                              CollectionID{103},
                                              "c2",
                                              {},
                                              CanDeduplicate::Yes,
                                              Collections::Metered::No,
                                              Collections::ManifestUid{}};
    m1.collections.emplace_back(0, c2);
    c2.canDeduplicate = CanDeduplicate::No;
    m2.collections.emplace_back(0, c2);
    EXPECT_NE(m1, m2);

    m1 = m2;
    EXPECT_EQ(m1, m2);
    // Check TTL difference is detected
    auto c3 = Collections::CollectionMetaData{ScopeID{88},
                                              CollectionID{103},
                                              "c3",
                                              cb::NoExpiryLimit,
                                              CanDeduplicate::Yes,
                                              Collections::Metered::No,
                                              Collections::ManifestUid{}};
    m1.collections.emplace_back(0, c3);
    c3.maxTtl = std::chrono::seconds(1);
    m2.collections.emplace_back(0, c3);
    EXPECT_NE(m1, m2);

    // Check flush_uid difference is detected
    auto c4 = Collections::CollectionMetaData{ScopeID{88},
                                              CollectionID{103},
                                              "c3",
                                              cb::NoExpiryLimit,
                                              CanDeduplicate::Yes,
                                              Collections::Metered::No,
                                              Collections::ManifestUid{}};
    m1.collections.emplace_back(0, c3);
    c3.flushUid += 1;
    m2.collections.emplace_back(0, c3);
    EXPECT_NE(m1, m2);
}

TEST_P(CollectionsKVStoreTest, initial_meta) {
    // Ask the kvstore for the initial meta
    auto [status, md] = kvstore->getCollectionsManifest(Vbid(0));

    ASSERT_TRUE(status);

    // Expect 1 collection and 1 scope
    EXPECT_EQ(1, md.collections.size());
    EXPECT_EQ(1, md.scopes.size());

    // It's the default collection and the default scope
    EXPECT_EQ(0, md.collections[0].startSeqno);
    EXPECT_EQ("_default", md.collections[0].metaData.name);
    EXPECT_EQ(CollectionID::Default, md.collections[0].metaData.cid);
    EXPECT_EQ(ScopeID(ScopeID::Default), md.collections[0].metaData.sid);
    EXPECT_FALSE(md.collections[0].metaData.maxTtl.has_value());
    EXPECT_EQ(Collections::Metered::No, md.collections[0].metaData.metered);

    EXPECT_EQ(0, md.scopes[0].startSeqno);
    EXPECT_EQ(ScopeID(ScopeID::Default), md.scopes[0].metaData.sid);
    EXPECT_EQ("_default", md.scopes[0].metaData.name);
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

// Check that the metered state persists and comes back
TEST_P(CollectionsKVStoreTest, one_metered_update) {
    CollectionsManifest cm;
    auto vegetable = CollectionEntry::vegetable;
    vegetable.metered = false;
    cm.add(vegetable);
    applyAndCheck(cm);
}

TEST_P(CollectionsKVStoreTest, create_and_modify) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit, cb::NoExpiryLimit, true)
            .add(CollectionEntry::vegetable);
    applyAndCheck(cm);
    // Switch the history setting
    cm.remove(CollectionEntry::vegetable)
            .remove(CollectionEntry::fruit)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::vegetable, cb::NoExpiryLimit, true);
    applyAndCheck(cm);
}

TEST_P(CollectionsKVStoreTest, create_and_modify_same_batch) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit, cb::NoExpiryLimit, true)
            .add(CollectionEntry::vegetable);
    manifest.update(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            *vbucket,
            makeManifest(cm));
    // Switch the history setting
    cm.remove(CollectionEntry::vegetable)
            .remove(CollectionEntry::fruit)
            .add(CollectionEntry::fruit)
            .add(CollectionEntry::vegetable, cb::NoExpiryLimit, true);
    applyAndCheck(cm);
}

// Created for MB-58319. Modify the default collection when in epoch state, i.e.
// the VB "disk" state is empty and we flush the first copy of the default
// collection - but in this case it's a modified copy.
TEST_P(CollectionsKVStoreTest, epochAndModify) {
    CollectionsManifest cm;
    auto entry = CollectionEntry::defaultC;
    entry.metered = true;
    cm.update(entry, cb::NoExpiryLimit);
    applyAndCheck(cm);
}

// Created for MB-59088, modify a collection to enable metering depending on
// the "batching" of the flush would use the wrong (unmodified) state.
TEST_P(CollectionsKVStoreTest, MB_59088_modifyMetering) {
    CollectionsManifest cm;
    // Flush an unrelated collection, this ensures that the kvstore data exists
    // ready for the next flush.
    cm.add(CollectionEntry::vegetable, std::chrono::seconds{1});
    applyAndCheck(cm);

    // Just like the MB, modify the default collection to enable metering in
    // a single "flush", Prior to the fix this test would detect that the
    // kvstore meta collection state did not change.
    auto entry = CollectionEntry::defaultC;
    entry.metered = true;
    cm.update(entry, cb::NoExpiryLimit);
    applyAndCheck(cm);
}

// Check that the history state persists and comes back
TEST_P(CollectionsKVStoreTest, one_update_with_history) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable, {}, true);
    applyAndCheck(cm);
}

TEST_P(CollectionsKVStoreTest, max_ttl_changes) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable, std::chrono::seconds{1});
    applyAndCheck(cm);
    cm.update(CollectionEntry::vegetable, std::chrono::seconds{2});
    applyAndCheck(cm);

    // Finally flush multiple modifications, final state should match the final
    // update.
    cm.update(CollectionEntry::vegetable, std::chrono::seconds{1});
    // have to bypass some of the helpers to update twice
    manifest.update(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            *vbucket,
            makeManifest(cm));
    cm.update(CollectionEntry::vegetable, {});
    applyAndCheck(cm);
}

// covers a case in Collections::Flush where no state exists, but we must flush
// a default collection modify.
TEST_P(CollectionsKVStoreTest, epoch_default_ttl) {
    CollectionsManifest cm;
    cm.update(CollectionEntry::defaultC, std::chrono::seconds{1});
    applyAndCheck(cm);
}

// Flush the default collection only, this hits a specific case in flush.cc
// where there is no KVStore state, but we must set now write out the epoch
// state + the flushUid
TEST_P(CollectionsKVStoreTest, flush_default_epoch) {
    CollectionsManifest cm;
    cm.flush(CollectionEntry::defaultC);
    applyAndCheck(cm, {CollectionID::Default});
    EXPECT_NE(0, vbucket->getHighSeqno());
    checkStartSeqno(CollectionID::Default, vbucket->getHighSeqno());
}

// Test that the flush_uid is updated in the open-collection state
TEST_P(CollectionsKVStoreTest, add_flush) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable);
    applyAndCheck(cm);
    auto hs = vbucket->getHighSeqno();
    checkStartSeqno(CollectionEntry::vegetable, hs);

    // Flush the collection, apply system events and check the KVStore state
    cm.flush(CollectionEntry::vegetable);
    applyAndCheck(cm, {CollectionUid::vegetable});
    EXPECT_GT(vbucket->getHighSeqno(), hs);
    checkStartSeqno(CollectionEntry::vegetable, vbucket->getHighSeqno());
}

// Related to MB-44098 test that we fail to generate 'corrupt' collection or
// scope metadata (i.e duplicate entries). This is not the sequence of steps
// that lead to warmup failure seen in the MB, but tests that we can detect
// duplicates.
void CollectionsKVStoreTest::failForDuplicate() {
    std::vector<queued_item> events;
    getEventsFromCheckpoint(events);
    EXPECT_EQ(1, events.size());
    auto event = events.front();

    // Drive the KVStore so that we flush the same collection twice with no
    // drop, this would attempt to create it twice in the metadata
    auto ctx = kvstore->begin(vbucket->getId(),
                              std::make_unique<PersistenceCallback>());
    flush.collections.recordSystemEvent(*event);
    EXPECT_FALSE(event->isDeleted());
    kvstore->setSystemEvent(*ctx, event);
    kvstore->commit(std::move(ctx), flush);

    ctx = kvstore->begin(vbucket->getId(),
                         std::make_unique<PersistenceCallback>());
    event->setBySeqno(event->getBySeqno() + 1);
    flush.collections.recordSystemEvent(*event);
    kvstore->setSystemEvent(*ctx, event);

    // The attempt to commit fails
    EXPECT_THROW(kvstore->commit(std::move(ctx), flush), std::logic_error);
}

TEST_P(CollectionsKVStoreTest, failForDuplicateCollection) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    manifest.update(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            *vbucket,
            makeManifest(cm));
    failForDuplicate();
}

TEST_P(CollectionsKVStoreTest, failForDuplicateScope) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    manifest.update(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            *vbucket,
            makeManifest(cm));
    failForDuplicate();
}

TEST_P(CollectionsKVStoreTest, systemCollection) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::systemCollection);
    cm.add(ScopeEntry::systemScope);
    manifest.update(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            *vbucket,
            makeManifest(cm));
    // Check the events have the correct flag
    std::vector<queued_item> events;
    getEventsFromCheckpoint(events);
    ASSERT_EQ(2, events.size());

    for (const auto& qi : events) {
        if (SystemEvent::Collection == SystemEvent(qi->getFlags())) {
            const auto& collection =
                    Collections::VB::Manifest::getCollectionFlatbuffer(
                            qi->getValueView());
            // System collection-ness is derived from the already stored name
            EXPECT_TRUE(Collections::isSystemCollection(
                    collection.name()->str(), collection.collectionId()));
        } else {
            EXPECT_EQ(SystemEvent::Scope, SystemEvent(qi->getFlags()));

            const auto* scope = Collections::VB::Manifest::getScopeFlatbuffer(
                    qi->getValueView());
            // System scope-ness is derived from the already stored name
            EXPECT_TRUE(Collections::isSystemScope(scope->name()->str(),
                                                   scope->scopeId()));
        }
    }
    events.clear();

    cm.remove(CollectionEntry::systemCollection);
    cm.remove(ScopeEntry::systemScope);
    manifest.update(
            std::shared_lock<folly::SharedMutex>(vbucket->getStateLock()),
            *vbucket,
            makeManifest(cm));

    getEventsFromCheckpoint(events);
    ASSERT_EQ(2, events.size());

    for (const auto& qi : events) {
        EXPECT_TRUE(qi->isDeleted());
        if (SystemEvent::Collection == SystemEvent(qi->getFlags())) {
            const auto& droppedCollection =
                    Collections::VB::Manifest::getDroppedCollectionFlatbuffer(
                            qi->getValueView());
            EXPECT_TRUE(droppedCollection->systemCollection());
        } else {
            EXPECT_EQ(SystemEvent::Scope, SystemEvent(qi->getFlags()));
            const auto* droppedScope =
                    Collections::VB::Manifest::getDroppedScopeFlatbuffer(
                            qi->getValueView());
            EXPECT_TRUE(droppedScope->systemScope());
        }
    }
}

TEST_P(CollectionsKVStoreTest, systemCollectionReplicaTombstones) {
    // Drop collections that were never created (replicate of a tombstone)
    vbucket->checkpointManager->createSnapshot(
            1, 2, std::nullopt, std::nullopt, CheckpointType::Memory, 2);
    {
        std::shared_lock rlh(vbucket->getStateLock());
        manifest.wlock(rlh).replicaDrop(*vbucket,
                                        Collections::ManifestUid(1),
                                        CollectionID(8),
                                        true,
                                        1);
        manifest.wlock(rlh).replicaDropScope(
                *vbucket, Collections::ManifestUid(2), ScopeID(9), true, 2);
    }

    std::vector<queued_item> events;

    drainEventsFromCheckpoint(events);
    ASSERT_EQ(2, events.size());

    for (const auto& qi : events) {
        EXPECT_TRUE(qi->isDeleted());
        if (SystemEvent::Collection == SystemEvent(qi->getFlags())) {
            const auto& droppedCollection =
                    Collections::VB::Manifest::getDroppedCollectionFlatbuffer(
                            qi->getValueView());
            EXPECT_EQ(CollectionID(8), droppedCollection->collectionId());
            EXPECT_TRUE(droppedCollection->systemCollection());
        } else {
            EXPECT_EQ(SystemEvent::Scope, SystemEvent(qi->getFlags()));
            const auto* droppedScope =
                    Collections::VB::Manifest::getDroppedScopeFlatbuffer(
                            qi->getValueView());
            EXPECT_TRUE(droppedScope->systemScope());
        }
    }
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
    std::string getBackendString() const {
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
            return [this] { openCollection(); };
        case 2:
            return [this] { dropCollection(); };
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
            return [this] { openScopeOpenCollection(); };
        case 2:
            return [this] { dropScope(); };
        }
        EXPECT_FALSE(true) << "No prologue defined for parameter:"
                           << std::get<4>(GetParam());
        return {};
    }

    void SetUp() override {
        KVStoreTest::SetUp();
        KVStoreBackend::setup(data_dir, getBackendString());
    }

    void TearDown() override {
        KVStoreBackend::teardown();
        KVStoreTest::TearDown();
    }

    // runs a flush batch that will leave the target collection in open state
    void openCollection() {
        cm.add(target);
        auto ctx = kvstore->begin(vbucket->getId(),
                                  std::make_unique<PersistenceCallback>());
        applyEvents(*ctx, cm);
        kvstore->commit(std::move(ctx), flush);
    }

    // runs a flush batch that will leave the target collection in dropped state
    void dropCollection() {
        openCollection();
        cm.remove(target);
        auto ctx = kvstore->begin(vbucket->getId(),
                                  std::make_unique<PersistenceCallback>());
        applyEvents(*ctx, cm);
        kvstore->commit(std::move(ctx), flush);
    }

    // runs a flush batch that will leave the target collection in open state
    void openScopeOpenCollection() {
        auto ctx = kvstore->begin(vbucket->getId(),
                                  std::make_unique<PersistenceCallback>());
        cm.add(targetScope);
        applyEvents(*ctx, cm);
        cm.add(target, targetScope);
        applyEvents(*ctx, cm);
        kvstore->commit(std::move(ctx), flush);
    }

    // runs a flush batch that will leave the target collection in dropped state
    void dropScope() {
        openScopeOpenCollection();
        cm.remove(targetScope);
        auto ctx = kvstore->begin(vbucket->getId(),
                                  std::make_unique<PersistenceCallback>());
        applyEvents(*ctx, cm);
        kvstore->commit(std::move(ctx), flush);
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
    auto ctx = kvstore->begin(vbucket->getId(),
                              std::make_unique<PersistenceCallback>());
    if (!cm.exists(target)) {
        cm.add(target);
        applyEvents(*ctx, cm);
    }

    CollectionEntry::Entry collection = target;

    // iterate cycles of remove/add
    for (int ii = 0; ii < getCycles(); ii++) {
        cm.remove(collection);
        applyEvents(*ctx, cm);

        if (resurectWithNewName()) {
            collection.name = target.name + "_" + std::to_string(ii);
        }

        cm.add(collection);
        applyEvents(*ctx, cm);
    }

    if (dropCollectionAtEnd()) {
        cm.remove(collection);
        applyEvents(*ctx, cm);
    }
    kvstore->commit(std::move(ctx), flush);

    // Now validate
    auto [status, md] = kvstore->getCollectionsManifest(Vbid(0));
    ASSERT_TRUE(status);
    checkUid(md, cm);
    checkCollections(md, cm, {target.uid});

    auto seqno = vbucket->getHighSeqno();

    // Finally validate the seqnos the local data stores (checkCollections
    // only compares name/uid/ttl from cm against md )
    for (const auto& collectionToCheck : md.collections) {
        if (collectionToCheck.metaData.cid == CollectionID::Default) {
            EXPECT_EQ(0, collectionToCheck.startSeqno);
        } else if (collectionToCheck.metaData.cid == target.uid) {
            EXPECT_EQ(2, md.collections.size());
            EXPECT_FALSE(dropCollectionAtEnd());
            EXPECT_EQ(seqno, collectionToCheck.startSeqno);
        }
    }

    // Vegetable was dropped during the test, thus it must be part of the
    // drop list and it must span the very first create to the very last drop!
    auto [getDroppedStatus, droppedCollections] =
            kvstore->getDroppedCollections(Vbid(0));
    ASSERT_TRUE(getDroppedStatus);
    EXPECT_TRUE(md.droppedCollectionsExist);
    ASSERT_EQ(1, droppedCollections.size()) << "Only vegetable was dropped";
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
    auto ctx = kvstore->begin(vbucket->getId(),
                              std::make_unique<PersistenceCallback>());
    if (!cm.exists(targetScope)) {
        cm.add(targetScope);
        applyEvents(*ctx, cm);
        cm.add(target, targetScope);
        applyEvents(*ctx, cm);
    }

    std::string expectedName = target.name;
    ScopeEntry::Entry scope = targetScope;

    // iterate cycles of remove/add
    for (int ii = 0; ii < getCycles(); ii++) {
        cm.remove(scope);
        applyEvents(*ctx, cm);

        if (resurectWithNewName()) {
            expectedName = target.name + "_" + std::to_string(ii);
            scope.name = targetScope.name + "_" + std::to_string(ii);
        }
        cm.add(scope);
        applyEvents(*ctx, cm);
        cm.add({expectedName, target.uid}, scope);
        applyEvents(*ctx, cm);
    }

    if (dropCollectionAtEnd()) {
        cm.remove(scope);
        applyEvents(*ctx, cm);
    }
    kvstore->commit(std::move(ctx), flush);

    // Now validate
    auto [status, md] = kvstore->getCollectionsManifest(Vbid(0));
    ASSERT_TRUE(status);
    checkUid(md, cm);
    checkCollections(md, cm, {target.uid});

    auto seqno = vbucket->getHighSeqno();

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
    for (const auto& scopeToCheck : md.scopes) {
        if (scopeToCheck.metaData.sid == ScopeID::Default) {
            EXPECT_EQ(0, scopeToCheck.startSeqno);
        } else if (scopeToCheck.metaData.sid == targetScope.uid) {
            EXPECT_EQ(2, md.scopes.size());
            EXPECT_FALSE(dropCollectionAtEnd());
            EXPECT_EQ(seqno - 1, scopeToCheck.startSeqno);
        }
    }

    // Vegetable was dropped during the test, thus it must be part of the
    // drop list and it must span the very first create to the very last drop!
    auto [getDroppedStatus, droppedCollections] =
            kvstore->getDroppedCollections(Vbid(0));
    ASSERT_TRUE(getDroppedStatus);
    EXPECT_TRUE(md.droppedCollectionsExist);
    ASSERT_EQ(1, droppedCollections.size()) << "Only vegetable was dropped";
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

class TestScanContext : public Collections::VB::ScanContext {
public:
    TestScanContext(const std::vector<Collections::KVStore::OpenCollection>*
                            openCollections,
                    const std::vector<Collections::KVStore::DroppedCollection>&
                            droppedCollections)
        : ScanContext(openCollections, droppedCollections) {
    }

    uint64_t getStartSeqno() const {
        return startSeqno;
    }

    uint64_t getEndSeqno() const {
        return endSeqno;
    }
};

TEST(ScanContextTest, construct) {
    std::vector<Collections::KVStore::DroppedCollection> dc = {
            {{9, 200, CollectionID{8}}, {1, 105, CollectionID{9}}}};

    TestScanContext tsc{{}, dc};
    EXPECT_EQ(1, tsc.getStartSeqno());
    EXPECT_EQ(200, tsc.getEndSeqno());
    auto& dropped = tsc.getDroppedCollections();
    EXPECT_FALSE(dropped.empty());
    EXPECT_EQ(2, dropped.size());
    EXPECT_NE(0, dropped.count(8));
    EXPECT_NE(0, dropped.count(9));

    auto c1 = StoredDocKey("key in cid:0x9", CollectionID(9));

    EXPECT_TRUE(tsc.isLogicallyDeleted(c1, false, 2)); // io range and dropped
    EXPECT_FALSE(tsc.isLogicallyDeleted(c1, false, 300)); // not in range

    auto c2 = StoredDocKey("key in cid:0xa", CollectionID(10));
    // In range, but not in dropped set
    EXPECT_FALSE(tsc.isLogicallyDeleted(c2, false, 2));
}

TEST(ScanContextTest, isLogicallyDeleted) {
    using namespace Collections::KVStore;
    std::vector<OpenCollection> open;
    // Default collection first
    open.emplace_back(OpenCollection{0, Collections::CollectionMetaData{}});
    open.emplace_back(OpenCollection{
            100,
            Collections::CollectionMetaData{ScopeID::Default,
                                            CollectionID(8),
                                            "c1",
                                            cb::NoExpiryLimit,
                                            CanDeduplicate::No,
                                            Collections::Metered::No,
                                            Collections::ManifestUid{}}});

    // No dropped collections, only open collections
    TestScanContext tsc{&open, {}};
    // These are in default state, note that these aren't visible via a real
    // ScanContext
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), tsc.getStartSeqno());
    EXPECT_EQ(0, tsc.getEndSeqno());
    EXPECT_TRUE(tsc.getDroppedCollections().empty());

    // Any collection not in the open list is then considered dropped
    // irrespective of seqno
    auto c2 = StoredDocKey("key in cid:0x9", CollectionID(9));
    EXPECT_TRUE(tsc.isLogicallyDeleted(c2, false, 1));
    EXPECT_TRUE(tsc.isLogicallyDeleted(c2, true, 1));
    EXPECT_TRUE(tsc.isLogicallyDeleted(c2, false, 1000));

    // For a collection which is in the open map, only lower than start seqno
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            StoredDocKey("k", CollectionID::Default), false, 1));

    // For a collection that is in the open list, the start-seqno decides if it
    // is dropped.
    auto c1 = StoredDocKey("key in cid:0x8", CollectionID(8));
    // 99 is before the start seqno of 100
    EXPECT_TRUE(tsc.isLogicallyDeleted(c1, false, 99));
    // 1000 is after the start seqno of 100
    EXPECT_FALSE(tsc.isLogicallyDeleted(c1, false, 1000));

    // Check with some system event keys.

    // Create collection for the open collection (seqno 100)
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(CollectionID(8),
                                                       SystemEvent::Collection),
            false,
            100));
    // Create collection, but must be an older generation (if resurrection ever
    // did happen)
    EXPECT_TRUE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(CollectionID(8),
                                                       SystemEvent::Collection),
            false,
            99));

    // Delete=true, this is a drop marker which is always made visible to
    // backfill
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(CollectionID(8),
                                                       SystemEvent::Collection),
            true,
            99));

    // Modify depends on seqno
    EXPECT_TRUE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(
                    CollectionID(8), SystemEvent::ModifyCollection),
            false,
            99));

    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(
                    CollectionID(8), SystemEvent::ModifyCollection),
            false,
            101));

    // Scope event is always false
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeScopeEventKey(ScopeID(8)), false, 99));
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeScopeEventKey(ScopeID(8)), false, 99));
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeScopeEventKey(ScopeID(8)), true, 99));
}

// Test that when open collections is used, but there are no open collections
// (empty vector) every collection input to isLogicallyDeleted returns true
TEST(ScanContextTest, allLogicallyDeleted) {
    using namespace Collections::KVStore;
    // The OpenCollection list is empty
    std::vector<OpenCollection> open;
    // And no dropped collections
    TestScanContext tsc{&open, {}};

    // In this configuration every key isLogicallyDeleted=>true
    auto c2 = StoredDocKey("key in cid:0x9", CollectionID(9));
    EXPECT_TRUE(tsc.isLogicallyDeleted(c2, false, 1));
    EXPECT_TRUE(tsc.isLogicallyDeleted(c2, true, 1));
    EXPECT_TRUE(tsc.isLogicallyDeleted(c2, false, 1000));

    EXPECT_TRUE(tsc.isLogicallyDeleted(
            StoredDocKey("k", CollectionID::Default), false, 1));
    auto c1 = StoredDocKey("key in cid:0x8", CollectionID(8));
    // 99 is before the start seqno of 100
    EXPECT_TRUE(tsc.isLogicallyDeleted(c1, false, 99));
    // 1000 is after the start seqno of 100
    EXPECT_TRUE(tsc.isLogicallyDeleted(c1, false, 1000));

    // Create collection is gone
    EXPECT_TRUE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(CollectionID(8),
                                                       SystemEvent::Collection),
            false,
            100));

    EXPECT_TRUE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(
                    CollectionID(8), SystemEvent::ModifyCollection),
            false,
            101));

    // Drop marker isn't filtered by this function
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeCollectionEventKey(CollectionID(8),
                                                       SystemEvent::Collection),
            true,
            99));

    // Scope event is always false
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeScopeEventKey(ScopeID(8)), false, 99));
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeScopeEventKey(ScopeID(8)), false, 99));
    EXPECT_FALSE(tsc.isLogicallyDeleted(
            SystemEventFactory::makeScopeEventKey(ScopeID(8)), true, 99));
}

INSTANTIATE_TEST_SUITE_P(
        CollectionsKVStoreTests,
        CollectionsKVStoreTest,
        KVStoreParamTest::persistentConfigValues(),
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
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
                std::tuple<std::string, int, bool, bool, int>>& testInfo) {
            auto backend = std::get<0>(testInfo.param);
            auto cycles = std::to_string(std::get<1>(testInfo.param));
            auto dropAtEnd = std::to_string(std::get<2>(testInfo.param));
            auto newName = std::to_string(std::get<3>(testInfo.param));
            auto prologueSelection =
                    std::to_string(std::get<4>(testInfo.param));
            return backend + "_with_" + cycles + "cycles_" + dropAtEnd + "_" +
                   newName + "_" + prologueSelection;
        });
