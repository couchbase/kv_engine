/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Tests for Collection functionality in EPStore.
 */

#include "collections/manager.h"
#include "collections/shared_metadata_table.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "collections_test.h"
#include "tests/module_tests/test_helpers.h"
#include "warmup.h"

#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <utilities/test_manifest.h>

#include <fstream>

class CollectionsManifestUpdate : public CollectionsParameterizedTest {};

TEST_P(CollectionsManifestUpdate, update_epoch) {
    CollectionsManifest cm;
    setCollections(cookie, cm);
}

TEST_P(CollectionsManifestUpdate, update_add1) {
    CollectionsManifest cm, cm1;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(22));
    setCollections(cookie, cm);
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(22));

    // Store an apple in collection 22
    auto apple = makeStoredDocKey("apple", 22);
    store_item(vbid, apple, "red");

    // Finally, we cannot setCollections to something which is not a
    // successor (and not using force).
    // Here we create a manifest which has an increased uid, but collection 22
    // switched name from fruit to woodwind - very odd and not a successor.
    cm1.add(CollectionEntry::Entry{"woodwind", 22});
    cm1.add(CollectionEntry::Entry{"brass", 23});
    setCollections(
            cookie, cm1, cb::engine_errc::cannot_apply_collections_manifest);

    // But now force it
    cm1.setForce(true);
    setCollections(cookie, cm1);

    // The apple is gone (dropped when the force update changed 22)
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->get(apple, vbid, nullptr, {}).getStatus());

    // But collection '22' lives on
    store_item(vbid, makeStoredDocKey("clarinet", 22), "soprano");
}

// Force change a scope name
TEST_P(CollectionsManifestUpdate, update_scope_name_forced) {
    // Create diverged manifests cm and cm1
    CollectionsManifest cm, cm1;

    // cm has 1 scope which has 1 collection
    cm.add(ScopeEntry::Entry{"scope1", 22});
    cm.add(CollectionEntry::fruit, ScopeEntry::Entry{"scope1", 22});

    // cm1 has the same scope-id as cm, but name is different
    cm1.add(ScopeEntry::Entry{"scope2", 22});

    // Update to cm, the scope should be valid
    setCollections(cookie, cm);
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isScopeValid(22));
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::fruit));

    // Now we cannot move to cm1, it is rejected
    setCollections(
            cookie, cm1, cb::engine_errc::cannot_apply_collections_manifest);

    // But now force it
    cm1.setForce(true);
    setCollections(cookie, cm1);
    // Scope was recreated, new scope doesn't have the fruit collection
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isScopeValid(22));
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::fruit));
}

TEST_P(CollectionsManifestUpdate, update_add1_with_warmup) {
    CollectionsManifest cm, cm1;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(22));
    setCollections(cookie, cm);
    // Check the current manifest is not a forced update
    EXPECT_FALSE(store->getCollectionsManager()
                         .getCurrentManifest()
                         .rlock()
                         ->isForcedUpdate());

    if (isPersistent()) {
        resetEngineAndWarmup();
        EXPECT_EQ(cb::engine_errc::success,
                  store->setVBucketState(vbid, vbucket_state_active));

        // Check the current manifest is still not a forced update
        EXPECT_FALSE(store->getCollectionsManager()
                             .getCurrentManifest()
                             .rlock()
                             ->isForcedUpdate());
    }

    // Store an apple in collection 22
    auto apple = makeStoredDocKey("apple", 22);
    store_item(vbid, apple, "red");
    flushVBucketToDiskIfPersistent(vbid, 2); // 2 includes create collection

    // cm1 is default state - uid of 0, cannot go back
    setCollections(
            cookie, cm1, cb::engine_errc::cannot_apply_collections_manifest);
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(22));

    // Finally, we cannot setCollections to something which is not a
    // successor (in future this would be allowed but by an explicit force)
    // Here we create a manifest which has an increased uid, but collection 22
    // switched name from fruit to woodwind - very odd and not a successor.
    cm1.add(CollectionEntry::Entry{"woodwind", 22});
    cm1.add(CollectionEntry::Entry{"brass", 23});
    setCollections(
            cookie, cm1, cb::engine_errc::cannot_apply_collections_manifest);

    // But now force it
    cm1.setForce(true);
    setCollections(cookie, cm1);

    if (isPersistent()) {
        resetEngineAndWarmup();
        EXPECT_EQ(cb::engine_errc::success,
                  store->setVBucketState(vbid, vbucket_state_active));
    }

    // Check the current manifest is still a forced update
    EXPECT_TRUE(store->getCollectionsManager()
                        .getCurrentManifest()
                        .rlock()
                        ->isForcedUpdate());

    // The apple is gone (dropped when the force update changed 22)
    if (isFullEviction()) {
        EXPECT_EQ(cb::engine_errc::would_block,
                  store->get(apple, vbid, nullptr, QUEUE_BG_FETCH).getStatus());
        runBGFetcherTask();
    }
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->get(apple, vbid, nullptr, {}).getStatus());
    // But collection 22 lives on
    store_item(vbid, makeStoredDocKey("clarinet", 22), "soprano");
}

TEST_P(CollectionsManifestUpdate, update_add1_move1_warmup) {
    // Begin with fruit in default scope and the addition of a second scope
    CollectionsManifest cm;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    cm.add(ScopeEntry::shop1);

    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(22));
    setCollections(cookie, cm);
    // Check the current manifest is not a forced update
    EXPECT_FALSE(store->getCollectionsManager()
                         .getCurrentManifest()
                         .rlock()
                         ->isForcedUpdate());

    auto scope = store->getVBucket(vbid)->lockCollections().getScopeID(22);
    EXPECT_EQ(ScopeID{ScopeID::Default}, scope.value());

    if (isPersistent()) {
        resetEngineAndWarmup();
        EXPECT_EQ(cb::engine_errc::success,
                  store->setVBucketState(vbid, vbucket_state_active));

        // Check the current manifest is still not a forced update
        EXPECT_FALSE(store->getCollectionsManager()
                             .getCurrentManifest()
                             .rlock()
                             ->isForcedUpdate());
    }

    // Move the collection to a different scope - this is not allowed, scope is
    // immutable
    cm.remove(CollectionEntry::Entry{"fruit", 22})
            .add(CollectionEntry::Entry{"fruit", 22}, ScopeEntry::shop1);
    setCollections(
            cookie, cm, cb::engine_errc::cannot_apply_collections_manifest);

    // But now force it
    cm.setForce(true);
    setCollections(cookie, cm);

    // And see that the collection does exist, but in a new scope
    scope = store->getVBucket(vbid)->lockCollections().getScopeID(22);
    EXPECT_EQ(ScopeEntry::shop1.uid, scope.value());
}

TEST_P(CollectionsManifestUpdate, force_update_multiple_changes) {
    // Create diverged manifests, here both have 1 scope, 1 collection, but
    // both with different names.
    CollectionsManifest cm, cm1;
    cm.add(ScopeEntry::Entry{"fruit-masters", 22});
    cm.add(CollectionEntry::Entry{"fruit", 22},
           ScopeEntry::Entry{"fruit-masters", 22});
    cm1.add(ScopeEntry::Entry{"tooltime", 22});
    cm1.add(CollectionEntry::Entry{"hardware", 22},
            ScopeEntry::Entry{"tooltime", 22});

    setCollections(cookie, cm);

    // Store an apple in collection 22
    auto apple = makeStoredDocKey("apple", 22);
    store_item(vbid, apple, "red");

    // shop1 is open for business and fully stocked with an apple.
    cm1.setForce(true);
    setCollections(cookie, cm1);

    // The apple is gone (dropped when the force update changed 22)
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->get(apple, vbid, nullptr, {}).getStatus());

    // But collection '22' lives on
    store_item(vbid, makeStoredDocKey("hammer", 22), "lump");
}

class CollectionsManifestUpdatePersistent
    : public CollectionsParameterizedTest {};

// Manually drive setCollections so we can force a failure
TEST_P(CollectionsManifestUpdatePersistent, update_fail_persist) {
    // magma variant not happy that the dir gets pulled away. Since this test
    // isn't actually writing data for the KVStore - skip for magma
    if (isMagma()) {
        GTEST_SKIP();
    }

    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);

    EXPECT_EQ(cb::engine_errc::would_block,
              engine->set_collection_manifest(*cookie, std::string{cm}));

    // Remove the datadir, persistence will fail and be detected, command then
    // fails
    cb::io::rmrf(test_dbname);

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    runNextTask(lpAuxioQ);

    auto mockCookie = cookie_to_mock_cookie(cookie);
    EXPECT_EQ(cb::engine_errc::cannot_apply_collections_manifest,
              mockCookie->getStatus());
}

TEST_P(CollectionsManifestUpdatePersistent, update_fail_warmup) {
    CollectionsManifest cm, cm1;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    setCollections(cookie, cm);
    EXPECT_EQ(1, store->getVBucket(vbid)->lockCollections().getManifestUid());
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(22));

    flush_vbucket_to_disk(vbid, 1);

    std::string fname = engine->getConfiguration().getDbname() +
                        cb::io::DirectorySeparator +
                        std::string(Collections::ManifestFileName);

    std::ofstream writer(fname, std::ofstream::trunc | std::ofstream::binary);
    writer << "junk in here now";
    writer.close();
    EXPECT_TRUE(writer.good());

    try {
        resetEngineAndWarmup();
        FAIL() << "Expected an exception from running warmup";
    } catch (const std::exception& e) {
        // CheckedExecutor throws because warm-up stops short
        EXPECT_STREQ("CheckedExecutor failed fetchNextTask", e.what());
    }
    EXPECT_FALSE(store->getWarmup()->isComplete());

    // Warmup failed so we would not be able to diverge
}

// Warmup after set_collections is success, but before the vbucket was able
// to flush. After warmup the vbucket should still know about collections
// without any other set_collections
TEST_P(CollectionsManifestUpdatePersistent, update_then_warmup) {
    // Flush one key, this ensures the vbucket exists after warmup
    auto apple1 = makeStoredDocKey("k1", CollectionID::Default);
    store_item(vbid, apple1, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Now add a collection but do not flush
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::fruit));

    // Warmup and expect that we can still access the collection (warmup
    // replayed the 'forgotten' set_collections)
    resetEngineAndWarmup();
    // Store an apple in collection 22
    auto apple = makeStoredDocKey("apple", CollectionEntry::fruit);
    store_item(vbid, apple, "red");
}

INSTANTIATE_TEST_SUITE_P(CollectionsEphemeralOrPersistent,
                         CollectionsManifestUpdate,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsPersistent,
                         CollectionsManifestUpdatePersistent,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

// Run through the basic API of the SharedMetaDataTable
// We need two types which wrap owned and non-owned views of the meta
//
// 1) Non owning "lookup" type, but must be able to
// compare Meta and MetaView
class Meta;
class MetaView {
public:
    MetaView(const Meta& meta);
    std::string to_string() const {
        return "MetaView of " + std::string{name};
    }
    std::string_view name;
    int something;
};

// Owning type, what the table actually stores
// The owning type must be constructable from the view type and comparable
class Meta : public RCValue {
public:
    Meta(std::string_view name, int something)
        : name(name), something(something) {
    }
    // Construct owned type from view type
    Meta(const MetaView& view) : name(view.name), something(view.something) {
    }

    // Compare with view type
    bool operator==(const MetaView& view) const {
        return name == view.name && something == view.something;
    }

    std::string name;
    int something;
};

std::ostream& operator<<(std::ostream& os, const Meta& meta) {
    os << " name:" << meta.name << ", something:" << meta.something;
    return os;
}

MetaView::MetaView(const Meta& meta)
    : name(meta.name), something(meta.something) {
}

TEST(SharedMetaDataTable, basic) {
    Collections::SharedMetaDataTable<CollectionID, Meta> table;
    EXPECT_EQ(0, table.count(8));
    Meta m1{"brass", 9};
    Meta m2{"woodwind", 10};

    auto ref1 = table.createOrReference(8, MetaView{m1});
    EXPECT_EQ(2, ref1.refCount());
    EXPECT_EQ(1, table.count(8));
    auto ref2 = table.createOrReference(8, MetaView{m2});
    EXPECT_EQ(2, ref2.refCount());
    EXPECT_EQ(2, table.count(8));

    // Get 3rd ref to the same as ref2/m2, table should not increase
    auto ref3 = table.createOrReference(8, MetaView{m2});
    EXPECT_EQ(3, ref3.refCount());
    EXPECT_EQ(ref2.refCount(), ref3.refCount());
    EXPECT_EQ(2, table.count(8)) << table;

    EXPECT_EQ(ref1->name, "brass");
    EXPECT_EQ(ref2, ref3);

    // Release one and tell the table
    table.dereference(8, std::move(ref1));
    EXPECT_EQ(1, table.count(8));

    // And the others
    table.dereference(8, std::move(ref2));
    EXPECT_EQ(1, table.count(8));

    table.dereference(8, std::move(ref3));
    EXPECT_EQ(0, table.count(8));
}