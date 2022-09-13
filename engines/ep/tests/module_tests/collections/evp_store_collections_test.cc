/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
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

#include "collections_test.h"

#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "failover-table.h"
#include "item.h"
#include "kvstore/couch-kvstore/couch-kvstore-metadata.h"
#include "kvstore/kvstore.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/ep_request_utils.h"
#include "tests/mock/mock_couch_kvstore.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/collections/stat_checker.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/vbucket_utils.h"
#include "warmup.h"

#include <memcached/collections.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <utilities/test_manifest.h>

#include <folly/portability/GMock.h>

#include <spdlog/fmt/fmt.h>
#include <functional>
#include <optional>
#include <thread>

TEST_P(CollectionsParameterizedTest, uid_increment) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(setCollections(cookie, cm), cb::engine_errc::success);
    cm.add(CollectionEntry::vegetable);
    EXPECT_EQ(setCollections(cookie, cm), cb::engine_errc::success);
}

TEST_P(CollectionsParameterizedTest, uid_decrement) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(setCollections(cookie, cm), cb::engine_errc::success);
    CollectionsManifest newCm{};
    setCollections(
            cookie, newCm, cb::engine_errc::cannot_apply_collections_manifest);
}

TEST_P(CollectionsParameterizedTest, uid_equal) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(setCollections(cookie, cm), cb::engine_errc::success);

    // An equal manifest is tolerated (and ignored)
    EXPECT_EQ(setCollections(cookie, cm), cb::engine_errc::success);
}

TEST_P(CollectionsParameterizedTest, manifest_uid_equal_with_differences) {
    CollectionsManifest cm{CollectionEntry::meat};
    EXPECT_EQ(setCollections(cookie, cm), cb::engine_errc::success);

    auto uid = cm.getUid();
    cm.add(CollectionEntry::fruit);
    // force the uid back
    cm.updateUid(uid);
    // manifest is equal, but contains an extra collection, unexpected diversion
    setCollections(
            cookie, cm, cb::engine_errc::cannot_apply_collections_manifest);
}

// This test stores a key which matches what collections internally uses, but
// in a different namespace.
TEST_P(CollectionsPersistentParameterizedTest, namespace_separation) {
    // Use the event factory to get an event which we'll borrow the key from
    auto se = SystemEventFactory::makeCollectionEvent(
            CollectionEntry::meat, {}, {});
    DocKey key(se->getKey().data(),
               se->getKey().size(),
               DocKeyEncodesCollectionId::No);

    store_item(vbid, key, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Add the meat collection
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));

    EXPECT_EQ(1, vb->dirtyQueueSize);
    EXPECT_NE(0, vb->dirtyQueueAge);

    flushVBucketToDiskIfPersistent(vbid, 1);

    // evict and load - should not see the system key for create collections
    evict_key(vbid, key);
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    GetValue gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    runBGFetcherTask();

    gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_EQ(0, strncmp("value", gv.item->getData(), gv.item->getNBytes()));
}

TEST_P(CollectionsParameterizedTest, collections_basic) {
    // Default collection is open for business
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
    store_item(vbid,
               StoredDocKey{"meat:beef", CollectionEntry::meat},
               "value",
               0,
               {cb::engine_errc::unknown_collection});

    VBucketPtr vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));

    EXPECT_EQ(0, vb->getManifest().lock(CollectionEntry::meat).getDiskSize());
    EXPECT_EQ(0, vb->getManifest().lock(CollectionID::Default).getDiskSize());
    EXPECT_EQ(1,
              vb->getManifest().lock().getDefaultCollectionMaxVisibleSeqno());

    // Scope dataSize is also updated for persistent buckets
    EXPECT_EQ(0, vb->getManifest().lock().getDataSize(ScopeID::Default));

    // Trigger a flush to disk. Flushes the meat create event and 1 item
    flushVBucketToDiskIfPersistent(vbid, 2);

    if (persistent()) {
        // Both collections now have data in them. Meat has the system event and
        // default has 'key'
        EXPECT_NE(0,
                  vb->getManifest().lock(CollectionEntry::meat).getDiskSize());
        EXPECT_NE(0,
                  vb->getManifest().lock(CollectionID::Default).getDiskSize());
        // Scope dataSize is also updated for persistent buckets
        EXPECT_NE(0, vb->getManifest().lock().getDataSize(ScopeID::Default));
        EXPECT_EQ(
                1,
                vb->getManifest().lock().getDefaultCollectionMaxVisibleSeqno());
    } else {
        EXPECT_EQ(0,
                  vb->getManifest().lock(CollectionEntry::meat).getDiskSize());
        EXPECT_EQ(0,
                  vb->getManifest().lock(CollectionID::Default).getDiskSize());
        // Scope dataSize is also updated for persistent buckets
        EXPECT_EQ(0, vb->getManifest().lock().getDataSize(ScopeID::Default));
    }

    // System event not counted
    // Note: for persistent buckets, that is because
    // 1) It doesn't go in the hash-table
    // 2) It will only be accounted for on Full-Evict buckets after flush
    EXPECT_EQ(1, vb->getNumItems());

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    // Now we can write to beef
    store_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat}, "value");

    flushVBucketToDiskIfPersistent(vbid, 1);

    // And read a document from beef
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue gv = store->get(StoredDocKey{"meat:beef", CollectionEntry::meat},
                             vbid,
                             cookie,
                             options);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());

    // A key in meat that doesn't exist
    auto key1 = StoredDocKey{"meat:sausage", CollectionEntry::meat};
    EXPECT_EQ(cb::engine_errc::no_such_key,
              checkKeyExists(key1, vbid, options));

    // Begin the deletion
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::meat)));

    // We should have deleted the create marker
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Access denied (although the item still exists)
    gv = store->get(StoredDocKey{"meat:beef", CollectionEntry::meat},
                    vbid,
                    cookie,
                    options);
    EXPECT_EQ(cb::engine_errc::unknown_collection, gv.getStatus());
}

// BY-ID update: This test was created for MB-25344 and is no longer relevant as
// we cannot 'hit' a logically deleted key from the front-end. This test has
// been adjusted to still provide some value.
TEST_P(CollectionsParameterizedTest, unknown_collection_errors) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);

    ASSERT_EQ(cb::engine_errc::success, addItem(item1, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto item2 = make_item(vbid,
                           StoredDocKey{"dairy:cream", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    ASSERT_EQ(cb::engine_errc::success, addItem(item2, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Delete the dairy collection (so all dairy keys become logically deleted)
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    // Re-add the dairy collection
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::dairy2)));

    // Trigger a flush to disk. Flushes the dairy2 create event, dairy delete.
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Expect that we cannot add item1 again, item1 has no collection
    item1.setCas(0);
    EXPECT_EQ(cb::engine_errc::unknown_collection, store->add(item1, cookie));

    // Replace should fail, item2 has no collection
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->replace(item2, cookie));

    // Delete should fail, item2 has no collection
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->deleteItem(item2.getKey(),
                                cas,
                                vbid,
                                cookie,
                                {},
                                nullptr,
                                mutation_descr));

    // Unlock should fail 'unknown-col' rather than an unlock error
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->unlockKey(
                      item2.getKey(), vbid, 0, ep_current_time(), cookie));

    EXPECT_EQ("collection_unknown",
              store->validateKey(
                      StoredDocKey{"meat:sausage", CollectionEntry::meat},
                      vbid,
                      item2));
    EXPECT_EQ("collection_unknown",
              store->validateKey(item2.getKey(), vbid, item2));

    if (persistent()) {
        EXPECT_EQ(cb::engine_errc::unknown_collection,
                  store->statsVKey(
                          StoredDocKey{"meat:sausage", CollectionEntry::meat},
                          vbid,
                          cookie));
        EXPECT_EQ(cb::engine_errc::unknown_collection,
                  store->statsVKey(item2.getKey(), vbid, cookie));
    }

    // GetKeyStats
    struct key_stats ks;
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->getKeyStats(
                      item2.getKey(), vbid, cookie, ks, WantsDeleted::No));
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->getKeyStats(
                      item2.getKey(), vbid, cookie, ks, WantsDeleted::Yes));

    uint32_t deleted = 0;
    uint8_t dtype = 0;
    ItemMetaData meta;
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->getMetaData(
                      item2.getKey(), vbid, cookie, meta, deleted, dtype));

    cas = 0;
    meta.cas = 1;
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->deleteWithMeta(item2.getKey(),
                                    cas,
                                    nullptr,
                                    vbid,
                                    cookie,
                                    {vbucket_state_active},
                                    CheckConflicts::No,
                                    meta,
                                    GenerateBySeqno::Yes,
                                    GenerateCas::No,
                                    0,
                                    nullptr,
                                    DeleteSource::Explicit));

    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->setWithMeta(item2,
                                 0,
                                 nullptr,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 false,
                                 GenerateBySeqno::Yes,
                                 GenerateCas::No));

    if (persistent()) {
        const char* msg = nullptr;
        EXPECT_EQ(cb::mcbp::Status::UnknownCollection,
                  store->evictKey(item2.getKey(), vbid, &msg));
    }
}

// BY-ID update: This test was created for MB-25344 and is no longer relevant as
// we cannot 'hit' a logically deleted key from the front-end. This test has
// been adjusted to still provide some value.
TEST_P(CollectionsParameterizedTest, GET_unknown_collection_errors) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(cb::engine_errc::success, addItem(item1, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Delete the dairy collection (so all dairy keys become logically deleted)
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    // Re-add the dairy collection
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::dairy2)));

    // Trigger a flush to disk. Flushes the dairy2 create event, dairy delete
    flushVBucketToDiskIfPersistent(vbid, 2);

    // The dairy:2 collection is empty
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);

    // Get deleted can't get it
    auto gv = store->get(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                         vbid,
                         cookie,
                         options);
    EXPECT_EQ(cb::engine_errc::unknown_collection, gv.getStatus());

    options = static_cast<get_options_t>(QUEUE_BG_FETCH | HONOR_STATES |
                                         TRACK_REFERENCE | DELETE_TEMP |
                                         HIDE_LOCKED_CAS | TRACK_STATISTICS);

    // Normal Get can't get it
    gv = store->get(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                    vbid,
                    cookie,
                    options);
    EXPECT_EQ(cb::engine_errc::unknown_collection, gv.getStatus());

    // Same for getLocked
    gv = store->getLocked(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                          vbid,
                          ep_current_time(),
                          10,
                          cookie);
    EXPECT_EQ(cb::engine_errc::unknown_collection, gv.getStatus());

    // Same for getAndUpdateTtl
    gv = store->getAndUpdateTtl(
            StoredDocKey{"dairy:milk", CollectionEntry::dairy},
            vbid,
            cookie,
            ep_current_time() + 20);
    EXPECT_EQ(cb::engine_errc::unknown_collection, gv.getStatus());
}

TEST_P(CollectionsParameterizedTest, get_collection_id) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::dairy);
    cm.add(ScopeEntry::shop2);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    setCollections(cookie, cm);
    // Check bad 'paths'
    auto rv = store->getCollectionID("");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getCollectionID("..");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getCollectionID("a.b.c");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getCollectionID("dairy");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    // valid path, just illegal scope
    rv = store->getCollectionID("#illegal*.meat");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    // valid path, just illegal collection
    rv = store->getCollectionID("_default.#illegal*");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    // Unknowns
    rv = store->getCollectionID("shoppe.dairy");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);
    rv = store->getCollectionID(".unknown");
    EXPECT_EQ(cb::engine_errc::unknown_collection, rv.result);

    // Success cases next
    rv = store->getCollectionID(".");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(3, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::defaultC.getId(), rv.getCollectionId());

    rv = store->getCollectionID("_default.");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(3, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::defaultC.getId(), rv.getCollectionId());

    rv = store->getCollectionID("_default._default");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(3, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::defaultC.getId(), rv.getCollectionId());

    rv = store->getCollectionID(".dairy");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(3, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::dairy.getId(), rv.getCollectionId());

    rv = store->getCollectionID("_default.dairy");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(3, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::dairy.getId(), rv.getCollectionId());

    rv = store->getCollectionID("minimart.meat");
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(3, rv.getManifestId());
    EXPECT_EQ(CollectionEntry::meat.getId(), rv.getCollectionId());

    // Now we should fail getting _default
    cm.remove(CollectionEntry::defaultC);
    setCollections(cookie, cm);
    rv = store->getCollectionID(".");
    EXPECT_EQ(cb::engine_errc::unknown_collection, rv.result);
    rv = store->getCollectionID("._default");
    EXPECT_EQ(cb::engine_errc::unknown_collection, rv.result);
}

TEST_P(CollectionsParameterizedTest, get_scope_id) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, ScopeEntry::shop1);
    cm.add(ScopeEntry::shop2);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    setCollections(cookie, cm);

    // Check bad 'paths', require 0 or 1 dot
    auto rv = store->getScopeID("..");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    // Check bad 'paths', require 0 or 1 dot
    rv = store->getScopeID("a.b.c");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    // Illegal scope names
    rv = store->getScopeID(" .");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getScopeID("#illegal*.");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);
    rv = store->getScopeID("#illegal*.ignored");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, rv.result);

    // Valid path, unknown scopes
    rv = store->getScopeID("megamart");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);
    rv = store->getScopeID("megamart.collection");
    EXPECT_EQ(cb::engine_errc::unknown_scope, rv.result);

    // Success cases next
    rv = store->getScopeID(""); // no dot = _default
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(4, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::defaultS.getId(), rv.getScopeId());

    rv = store->getScopeID("."); // 1 dot = _default
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(4, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::defaultS.getId(), rv.getScopeId());

    rv = store->getScopeID(ScopeEntry::shop1.name);
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(4, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::shop1.getId(), rv.getScopeId());

    rv = store->getScopeID(ScopeEntry::shop2.name);
    EXPECT_EQ(cb::engine_errc::success, rv.result);
    EXPECT_EQ(4, rv.getManifestId());
    EXPECT_EQ(ScopeEntry::shop2.getId(), rv.getScopeId());

    // Test the collection/vbucket lookup
    auto sid = store->getScopeID(CollectionEntry::dairy);
    EXPECT_TRUE(sid.second.has_value());
    EXPECT_EQ(ScopeEntry::shop1.uid, sid.second.value());

    sid = store->getScopeID(CollectionEntry::fruit);
    EXPECT_FALSE(sid.second.has_value());
}

// Test high seqno values
TEST_P(CollectionsPersistentParameterizedTest, PersistedHighSeqno) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    ASSERT_EQ(cb::engine_errc::success, addItem(item1, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Mock a change in this document incrementing the high seqno
    EXPECT_EQ(cb::engine_errc::success, store->set(item1, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(3,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check the set of a new item in the same collection increments the high
    // seqno for this collection
    auto item2 = make_item(vbid,
                           StoredDocKey{"dairy:cream", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    ASSERT_EQ(cb::engine_errc::success, addItem(item2, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(4,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check a deletion
    item2.setDeleted();
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(5,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // No test of dropped collection as manifest removes the entry, so no seqno
    // is available for the dropped collection.
}

// Test persisted high seqno values with multiple collections
TEST_P(CollectionsPersistentParameterizedTest,
       PersistedHighSeqnoMultipleCollections) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    ASSERT_EQ(cb::engine_errc::success, addItem(item1, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Add the meat collection
    cm.add(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));
    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(3,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Set a new item in meat
    auto item2 = make_item(vbid,
                           StoredDocKey{"meat:beef", CollectionEntry::meat},
                           "beefy",
                           0,
                           0);
    ASSERT_EQ(cb::engine_errc::success, addItem(item2, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    // Skip 1 seqno for creation of meat
    EXPECT_EQ(4,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Now, set a new high seqno in both collections in a single flush
    EXPECT_EQ(cb::engine_errc::success, store->set(item1, cookie));
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));
    flushVBucketToDiskIfPersistent(vbid, 2);
    EXPECT_EQ(5,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::dairy.getId()));
    EXPECT_EQ(6,
              vb->getManifest().lock().getPersistedHighSeqno(
                      CollectionEntry::meat.getId()));

    // No test of dropped collection as manifest removes the entry, so no seqno
    // is available for the dropped collection.
}

// Test high seqno values
TEST_P(CollectionsParameterizedTest, HighSeqno) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    // Flushing the manifest to disk guarantees that the database file
    // is written and exists, any subsequent bgfetches (e.g. during
    // addItem) will definitely be executed.
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(cb::engine_errc::success, addItem(item1, cookie));
    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Mock a change in this document incrementing the high seqno
    EXPECT_EQ(cb::engine_errc::success, store->set(item1, cookie));
    EXPECT_EQ(3,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check the set of a new item in the same collection increments the high
    // seqno for this collection
    auto item2 = make_item(vbid,
                           StoredDocKey{"dairy:cream", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(cb::engine_errc::success, addItem(item2, cookie));
    EXPECT_EQ(4,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Check a deletion
    item2.setDeleted();
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));
    EXPECT_EQ(5,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));
}

// Test high seqno values with multiple collections
TEST_P(CollectionsParameterizedTest, HighSeqnoMultipleCollections) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    // Flushing the manifest to disk guarantees that the database file
    // is written and exists, any subsequent bgfetches (e.g. during
    // addItem) will definitely be executed.
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    auto item1 = make_item(vbid,
                           StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                           "creamy",
                           0,
                           0);
    EXPECT_EQ(cb::engine_errc::success, addItem(item1, cookie));

    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Add the meat collection
    cm.add(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));

    EXPECT_EQ(3,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Set a new item in meat
    auto item2 = make_item(vbid,
                           StoredDocKey{"meat:beef", CollectionEntry::meat},
                           "beefy",
                           0,
                           0);
    EXPECT_EQ(cb::engine_errc::success, addItem(item2, cookie));

    // Skip 1 seqno for creation of meat
    EXPECT_EQ(4,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::meat.getId()));

    // Dairy should remain unchanged
    EXPECT_EQ(2,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));

    // Now, set a new high seqno in both collections in a single flush
    EXPECT_EQ(cb::engine_errc::success, store->set(item1, cookie));
    EXPECT_EQ(cb::engine_errc::success, store->set(item2, cookie));

    EXPECT_EQ(5,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::dairy.getId()));
    EXPECT_EQ(6,
              vb->getManifest().lock().getHighSeqno(
                      CollectionEntry::meat.getId()));
}

// Test get random key in a non-default collection
TEST_P(CollectionsParameterizedTest, GetRandomKey) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);
    StoredDocKey key{"milk", CollectionEntry::dairy};
    auto item = store_item(vbid, key, "1", 0);
    store_item(vbid, StoredDocKey{"stuff", CollectionEntry::defaultC}, "2", 0);
    flushVBucketToDiskIfPersistent(vbid, 2);
    auto gv = store->getRandomKey(CollectionEntry::dairy.getId(), cookie);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_EQ(item, *gv.item);
}

class CollectionsFlushTest : public CollectionsTest {
public:
    void SetUp() override {
        CollectionsTest::SetUp();
    }

    void collectionsFlusher(int items);

private:
    Collections::KVStore::Manifest createCollectionAndFlush(
            const std::string& json, CollectionID collection, int items);
    Collections::KVStore::Manifest dropCollectionAndFlush(
            const std::string& json, CollectionID collection, int items);

    void storeItems(CollectionID collection,
                    int items,
                    cb::engine_errc = cb::engine_errc::success);

    /**
     * Create manifest object from persisted manifest and validate if we can
     * write to the collection.
     * @param manifest Manifest to check
     * @param collection - a collection name to test for writing
     *
     * @return true if the collection can be written
     */
    static bool canWrite(const Collections::VB::Manifest& manifest,
                         CollectionID collection);

    /**
     * Create manifest object from persisted manifest and validate if we can
     * write to the collection.
     * @param manifest Manifest to check
     * @param collection - a collection name to test for writing
     *
     * @return true if the collection cannot be written
     */
    static bool cannotWrite(const Collections::VB::Manifest& manifest,
                            CollectionID collection);
};

void CollectionsFlushTest::storeItems(CollectionID collection,
                                      int items,
                                      cb::engine_errc expected) {
    for (int ii = 0; ii < items; ii++) {
        std::string key = "key" + std::to_string(ii);
        store_item(vbid, StoredDocKey{key, collection}, "value", 0, {expected});
    }
}

Collections::KVStore::Manifest CollectionsFlushTest::createCollectionAndFlush(
        const std::string& json, CollectionID collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    // cannot write to collection
    storeItems(collection, items, cb::engine_errc::unknown_collection);
    vb->updateFromManifest(Collections::Manifest{json});
    storeItems(collection, items);
    flushVBucketToDiskIfPersistent(vbid, 1 + items); // create event + items
    EXPECT_EQ(items, vb->lockCollections().getItemCount(collection));
    return getManifest(vbid);
}

Collections::KVStore::Manifest CollectionsFlushTest::dropCollectionAndFlush(
        const std::string& json, CollectionID collection, int items) {
    VBucketPtr vb = store->getVBucket(vbid);
    storeItems(collection, items);
    vb->updateFromManifest(Collections::Manifest(json));
    // cannot write to collection
    storeItems(collection, items, cb::engine_errc::unknown_collection);
    flushVBucketToDiskIfPersistent(vbid,
                                   1 + items); // 1x del(create event) + items
    runCompaction(vbid);

    // Default is still ok
    storeItems(CollectionID::Default, items);
    flushVBucketToDiskIfPersistent(vbid, items); // just the items
    return getManifest(vbid);
}

bool CollectionsFlushTest::canWrite(const Collections::VB::Manifest& manifest,
                                    CollectionID collection) {
    std::string key = std::to_string(uint32_t{collection});
    return manifest.lock().doesKeyContainValidCollection(
            StoredDocKey{key, collection});
}

bool CollectionsFlushTest::cannotWrite(
        const Collections::VB::Manifest& manifest, CollectionID collection) {
    return !canWrite(manifest, collection);
}

/**
 * Drive manifest state changes through the test's vbucket
 *  1. Validate the flusher flushes the expected items
 *  2. Validate the updated collections manifest changes
 *  3. Use a validator function to check if a collection is (or is not)
 *     writeable
 */
void CollectionsFlushTest::collectionsFlusher(int items) {
    struct testFuctions {
        std::function<Collections::KVStore::Manifest(int)> function;
        std::function<bool(const Collections::VB::Manifest&)> validator;
    };

    CollectionsManifest cm(CollectionEntry::meat);
    // Setup the test using a vector of functions to run
    std::vector<testFuctions> test{
            // First 2 steps - add,delete for the meat collection
            {// 0
             [this, cm](int items) -> Collections::KVStore::Manifest {
                 return createCollectionAndFlush(
                         cm, CollectionEntry::meat, items);
             },
             [](const Collections::VB::Manifest& manifest) -> bool {
                 return CollectionsFlushTest::canWrite(manifest,
                                                       CollectionEntry::meat);
             }},
            {// 1
             [this, manifest = cm.remove(CollectionEntry::meat)](
                     int items) -> Collections::KVStore::Manifest {
                 return dropCollectionAndFlush(
                         manifest, CollectionEntry::meat, items);
             },
             [](const Collections::VB::Manifest& manifest) -> bool {
                 return CollectionsFlushTest::cannotWrite(
                         manifest, CollectionEntry::meat);
             }},

            // Final 3 steps - add,delete,add for the fruit collection
            {// 2
             [this, manifest = cm.add(CollectionEntry::dairy)](
                     int items) -> Collections::KVStore::Manifest {
                 return createCollectionAndFlush(
                         manifest, CollectionEntry::dairy, items);
             },
             [](const Collections::VB::Manifest& manifest) -> bool {
                 return CollectionsFlushTest::canWrite(manifest,
                                                       CollectionEntry::dairy);
             }},
            {// 3
             [this, manifest = cm.remove(CollectionEntry::dairy)](
                     int items) -> Collections::KVStore::Manifest {
                 return dropCollectionAndFlush(
                         manifest, CollectionEntry::dairy, items);
             },
             [](const Collections::VB::Manifest& manifest) {
                 return CollectionsFlushTest::cannotWrite(
                         manifest, CollectionEntry::dairy);
             }},
            {// 4
             [this, manifest = cm.add(CollectionEntry::dairy2)](
                     int items) -> Collections::KVStore::Manifest {
                 return createCollectionAndFlush(
                         manifest, CollectionEntry::dairy2, items);
             },
             [](const Collections::VB::Manifest& manifest) -> bool {
                 return CollectionsFlushTest::canWrite(manifest,
                                                       CollectionEntry::dairy2);
             }}};

    auto m1 = std::make_unique<Collections::VB::Manifest>(
            store->getSharedCollectionsManager());
    int step = 0;
    for (auto& f : test) {
        auto m2 = std::make_unique<Collections::VB::Manifest>(
                store->getSharedCollectionsManager(), f.function(items));
        // The manifest should change for each step
        EXPECT_NE(*m1, *m2) << "Failed step:" + std::to_string(step) << "\n"
                            << *m1 << "\n should not match " << *m2 << "\n";
        EXPECT_TRUE(f.validator(*m2))
                << "Failed at step:" << std::to_string(step) << " validating "
                << *m2;
        m1.swap(m2);
        step++;
    }
}

TEST_F(CollectionsFlushTest, collections_flusher_no_items) {
    collectionsFlusher(0);
}

TEST_F(CollectionsFlushTest, collections_flusher_with_items) {
    collectionsFlusher(3);
}

class CollectionsWarmupTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override {
        // Enable collections (which will enable namespace persistence).
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }
};

// Test item counting when we store/delete flush and store again
TEST_P(CollectionsParameterizedTest, MB_31212) {
    CollectionsManifest cm;
    auto vb = store->getVBucket(vbid);

    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::meat)));
    auto key = StoredDocKey{"beef", CollectionEntry::meat};
    // Now we can write to meat
    store_item(vbid, key, "value");
    delete_item(vbid, key);

    // Trigger a flush to disk. Flushes the meat create event and the delete
    flushVBucketToDiskIfPersistent(vbid, 2);

    // 0 items, we only have a delete on disk
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::meat));

    // Store the same key again and expect 1 item
    store_item(vbid, StoredDocKey{"beef", CollectionEntry::meat}, "value");

    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmup) {
    CollectionsManifest cm;
    uint32_t uid = 0xface2;
    cm.setUid(uid);
    size_t scopeDataSize = 0;
    {
        auto vb = store->getVBucket(vbid);

        // add performs a +1 on the manifest uid
        vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::meat)));

        // Trigger a flush to disk. Flushes the meat create event
        flushVBucketToDiskIfPersistent(vbid, 1);

        // Now we can write to beef
        store_item(vbid,
                   StoredDocKey{"meat:beef", CollectionEntry::meat},
                   "value");
        // But not dairy
        store_item(vbid,
                   StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                   "value",
                   0,
                   {cb::engine_errc::unknown_collection});

        flushVBucketToDiskIfPersistent(vbid, 1);

        EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));
        EXPECT_EQ(2,
                  vb->lockCollections().getPersistedHighSeqno(
                          CollectionEntry::meat));
        EXPECT_EQ(2, vb->lockCollections().getHighSeqno(CollectionEntry::meat));
        EXPECT_EQ(2,
                  store->getVBucket(vbid)->lockCollections().getHighSeqno(
                          CollectionEntry::meat));

        // create an extra collection which we do not write to (note uid++)
        vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
        flushVBucketToDiskIfPersistent(vbid, 1);

        // The high-seqno of the collection is the start, the seqno of the
        // creation event
        EXPECT_EQ(3,
                  store->getVBucket(vbid)->lockCollections().getHighSeqno(
                          CollectionEntry::fruit));
        scopeDataSize = store->getVBucket(vbid)->lockCollections().getDataSize(
                ScopeID::Default);
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    // validate the manifest uid comes back as expected
    EXPECT_EQ(uid + 2,
              store->getVBucket(vbid)->lockCollections().getManifestUid());

    // validate we warmup the item count and high seqnos
    EXPECT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::meat));
    EXPECT_EQ(2,
              store->getVBucket(vbid)->lockCollections().getPersistedHighSeqno(
                      CollectionEntry::meat));
    EXPECT_EQ(2,
              store->getVBucket(vbid)->lockCollections().getHighSeqno(
                      CollectionEntry::meat));

    EXPECT_EQ(scopeDataSize,
              store->getVBucket(vbid)->lockCollections().getDataSize(
                      ScopeID::Default));

    {
        Item item(StoredDocKey{"meat:beef", CollectionEntry::meat},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  "rare",
                  sizeof("rare"));
        item.setVBucketId(vbid);
        uint64_t cas;
        EXPECT_EQ(cb::engine_errc::success,
                  engine->storeInner(
                          cookie, item, cas, StoreSemantics::Set, false));
    }
    {
        Item item(StoredDocKey{"dairy:milk", CollectionEntry::dairy},
                  /*flags*/ 0,
                  /*exp*/ 0,
                  "skimmed",
                  sizeof("skimmed"));
        item.setVBucketId(vbid);
        uint64_t cas;
        EXPECT_EQ(cb::engine_errc::unknown_collection,
                  engine->storeInner(
                          cookie, item, cas, StoreSemantics::Set, false));
    }

    EXPECT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::meat));

    // Now what about the other collections, we still have the default and fruit
    // They were never written to but should come back with sensible state
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::fruit));
    EXPECT_EQ(3,
              store->getVBucket(vbid)->lockCollections().getPersistedHighSeqno(
                      CollectionEntry::fruit));
    EXPECT_EQ(3,
              store->getVBucket(vbid)->lockCollections().getHighSeqno(
                      CollectionEntry::fruit));

    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionEntry::defaultC));
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getPersistedHighSeqno(
                      CollectionEntry::defaultC));
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getHighSeqno(
                      CollectionEntry::defaultC));
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmupIgnoreLogicallyDeleted) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        CollectionsManifest cm(CollectionEntry::meat);
        vb->updateFromManifest(makeManifest(cm));

        // Trigger a flush to disk. Flushes the meat create event
        flushVBucketToDiskIfPersistent(vbid, 1);
        const int nitems = 10;
        for (int ii = 0; ii < nitems; ii++) {
            // Now we can write to beef
            std::string key = "meat:" + std::to_string(ii);
            store_item(vbid, StoredDocKey{key, CollectionEntry::meat}, "value");
        }

        flushVBucketToDiskIfPersistent(vbid, nitems);

        // Remove the meat collection
        vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::meat)));

        flushVBucketToDiskIfPersistent(vbid, 1);

        // Items still exist until the eraser runs
        EXPECT_EQ(nitems, vb->ht.getNumInMemoryItems());

        // Ensure collection purge has executed
        runCollectionsEraser(vbid);

        EXPECT_EQ(0, vb->ht.getNumInMemoryItems());
    } // VBucketPtr scope ends


    resetEngineAndWarmup();

    EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::meat));
}

//
// Create a collection then create a second engine which will warmup from the
// persisted collection state and should have the collection accessible.
//
TEST_F(CollectionsWarmupTest, warmupIgnoreLogicallyDeletedDefault) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        CollectionsManifest cm(CollectionEntry::meat);
        vb->updateFromManifest(makeManifest(cm));

        // Trigger a flush to disk. Flushes the meat create event
        flushVBucketToDiskIfPersistent(vbid, 1);
        const int nitems = 10;
        for (int ii = 0; ii < nitems; ii++) {
            std::string key = "key" + std::to_string(ii);
            store_item(vbid,
                       StoredDocKey{key, CollectionEntry::defaultC},
                       "value");
        }

        flushVBucketToDiskIfPersistent(vbid, nitems);

        // Remove the default collection
        vb->updateFromManifest(
                makeManifest(cm.remove(CollectionEntry::defaultC)));

        flushVBucketToDiskIfPersistent(vbid, 1);

        // Items still exist until the eraser runs
        EXPECT_EQ(nitems, vb->ht.getNumInMemoryItems());

        // But no manifest level stats exist
        EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(
                CollectionEntry::defaultC));

        // Ensure collection purge has executed
        runCollectionsEraser(vbid);

        EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumInMemoryItems());

    // meat collection still exists
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::meat));
}

TEST_F(CollectionsWarmupTest, warmupManifestUidLoadsOnCreate) {
    {
        auto vb = store->getVBucket(vbid);

        // Add the meat collection
        CollectionsManifest cm;
        cm.setUid(0xface2); // cm.add will +1 this uid
        vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::meat)));

        flushVBucketToDiskIfPersistent(vbid, 1);
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    // validate the manifest uid comes back
    EXPECT_EQ(0xface2 + 1,
              store->getVBucket(vbid)->lockCollections().getManifestUid());
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::meat));
}

TEST_F(CollectionsWarmupTest, warmupManifestUidLoadsOnDelete) {
    {
        auto vb = store->getVBucket(vbid);

        // Delete the $default collection
        CollectionsManifest cm;
        cm.setUid(0xface2); // cm.remove will +1 this uid
        vb->updateFromManifest(
                makeManifest(cm.remove(CollectionEntry::defaultC)));

        flushVBucketToDiskIfPersistent(vbid, 1);
    } // VBucketPtr scope ends

    resetEngineAndWarmup();

    // validate the manifest uid comes back
    EXPECT_EQ(0xface2 + 1,
              store->getVBucket(vbid)->lockCollections().getManifestUid());
}

// Set the manifest before warmup runs, without the fix, the manifest wouldn't
// get applied to the active vbucket
TEST_F(CollectionsWarmupTest, MB_38125) {
    resetEngineAndEnableWarmup();

    CollectionsManifest cm(CollectionEntry::fruit);

    // Cannot set the manifest yet - command follows ewouldblock pattern
    auto status = engine->set_collection_manifest(*cookie, std::string{cm});
    EXPECT_EQ(cb::engine_errc::would_block, status);

    // Now get the engine warmed up
    runReadersUntilWarmedUp();

    // cookie now notified and setCollections can go ahead
    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));
    setCollections(cookie, cm);

    auto vb = store->getVBucket(vbid);

    // Fruit is enabled
    EXPECT_TRUE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"grape", CollectionEntry::fruit}));
}

/**
 * Test checks that setCollections propagates the collection data to active
 * vbuckets.
 */
TEST_P(CollectionsParameterizedTest, basic) {
    // Add some more VBuckets just so there's some iteration happening
    const int extraVbuckets = 2;
    for (int vb = vbid.get() + 1; vb <= (vbid.get() + extraVbuckets); vb++) {
        store->setVBucketState(Vbid(vb), vbucket_state_active);
    }

    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);

    // Check all vbuckets got the collections
    for (int vb = vbid.get(); vb <= (vbid.get() + extraVbuckets); vb++) {
        auto vbp = store->getVBucket(Vbid(vb));
        EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                StoredDocKey{"meat:bacon", CollectionEntry::meat}));
        EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                StoredDocKey{"anykey", CollectionEntry::defaultC}));
    }
}

/**
 * Test checks that setCollections propagates the collection data to active
 * vbuckets and not the replicas
 */
TEST_P(CollectionsParameterizedTest, basic2) {
    // Add some more VBuckets just so there's some iteration happening
    const int extraVbuckets = 2;
    // Add active and replica
    for (int vb = vbid.get() + 1; vb <= (vbid.get() + extraVbuckets); vb++) {
        if (vb & 1) {
            store->setVBucketState(Vbid(vb), vbucket_state_active);
        } else {
            store->setVBucketState(Vbid(vb), vbucket_state_replica);
        }
    }

    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);

    // Check all vbuckets got the collections
    for (int vb = vbid.get(); vb <= (vbid.get() + extraVbuckets); vb++) {
        auto vbp = store->getVBucket(Vbid(vb));
        if (vbp->getState() == vbucket_state_active) {
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"meat:bacon", CollectionEntry::meat}));
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"anykey", CollectionEntry::defaultC}));
        } else {
            // Replica will be in default constructed settings
            EXPECT_FALSE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"meat:bacon", CollectionEntry::meat}));
            EXPECT_TRUE(vbp->lockCollections().doesKeyContainValidCollection(
                    StoredDocKey{"anykey", CollectionEntry::defaultC}));
        }
    }
}

// Test the compactor doesn't generate expired items for a dropped collection
TEST_P(CollectionsPersistentParameterizedTest,
       collections_expiry_after_drop_collection_compaction) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the meat collection + 1 item with TTL (and flush it all out)
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));
    StoredDocKey key{"lamb", CollectionEntry::meat};
    store_item(vbid, key, "value", ep_real_time() + 100);
    flushVBucketToDiskIfPersistent(vbid, 2);
    // And now drop the meat collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::meat)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Time travel
    TimeTraveller docBrown(2000);

    // Now compact to force expiry of our little lamb
    runCompaction(vbid);

    std::vector<queued_item> items;
    vb->checkpointManager->getNextItemsForPersistence(items);

    // No mutation of the original key is allowed as it would invalidate the
    // ordering of create @x, item @y, drop @z  x < y < z
    for (auto& i : items) {
        EXPECT_NE(key, i->getKey());
    }
}

TEST_P(CollectionsPersistentParameterizedTest,
       CollectionAddedAndRemovedBeforePersistence) {
    /**
     * MB-38528: Test that setPersistedHighSeqno when called when persisting a
     * collection creation event does not throw if the collection is not
     * found.
     * In the noted MB a replica received a collection creation and collection
     * drop very quickly after. By the time the creation had persisted, the drop
     * had already removed the collection from the vb manifest.
     */
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the dairy collection, but don't flush it just yet.
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    // set a hook to be called immediately before the flusher commits to disk.
    // This is after items have been read from the checkpoint manager, but
    // before the items are persisted - importantly in this case, before
    // saveDocsCallback is invoked (which calls setPersistedHighSeqno())
    auto& kvstore = *store->getRWUnderlying(vbid);
    kvstore.setPreFlushHook([&cm, &vb] {
        // now remove the collection. This will remove it from the vb manifest
        // _before_ the creation event tries to call setPersistedHighSeqno()
        cm.remove(CollectionEntry::dairy);
        vb->updateFromManifest(makeManifest(cm));
    });
    // flushing the creation to disk should not throw, even though the
    // collection was not found in the manifest
    EXPECT_NO_THROW(flushVBucketToDiskIfPersistent(vbid, 1));
}

TEST_P(CollectionsCouchstoreParameterizedTest, ConcCompactNewPrepare) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    cm.add(CollectionEntry::dairy);
    cm.add(CollectionEntry::meat);
    // MB-44590 include a collection drop in the test
    cm.remove(CollectionEntry::defaultC);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 3);

    // We add dairy and a key to it just to ensure that we're not breaking
    // collections that may change during compaction but not during the replay
    StoredDocKey dairyKey{"milk", CollectionEntry::dairy};
    auto dairyPending = makePendingItem(dairyKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*dairyPending, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    vb->seqnoAcknowledged(
            folly::SharedMutex::ReadHolder(vb->getStateLock()),
            "replica",
            vb->getHighSeqno());
    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto postCommitDairySize = 0;
    auto preCompactionMeatSize = 0;
    auto postFlushMeatSize = 0;

    {
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        postCommitDairySize = summary[CollectionEntry::dairy].diskSize;
        EXPECT_NE(0, postCommitDairySize);

        preCompactionMeatSize = summary[CollectionEntry::meat].diskSize;
        EXPECT_EQ(57 + 14 + MetaData::getMetaDataSize(MetaData::Version::V1),
                  preCompactionMeatSize);
    }

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool seenPrepare = false;
    kvstore.setConcurrentCompactionPreLockHook([&seenPrepare,
                                                &vb,
                                                &preCompactionMeatSize,
                                                &postFlushMeatSize,
                                                this](auto& compactionKey) {
        if (seenPrepare) {
            return;
        }
        seenPrepare = true;

        StoredDocKey meatKey{"beef", CollectionEntry::meat};
        auto meatPending = makePendingItem(meatKey, "value");
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  store->set(*meatPending, cookie));
        flushVBucketToDiskIfPersistent(vbid, 1);

        {
            Collections::Summary summary;
            vb->getManifest().lock().updateSummary(summary);

            // And that meat increases
            EXPECT_GT(summary[CollectionEntry::meat].diskSize,
                      preCompactionMeatSize);
            postFlushMeatSize = summary[CollectionEntry::meat].diskSize;
        }
    });

    runCompaction(vbid, 0, false);

    {
        // Check that dairy decreases
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        EXPECT_LT(summary[CollectionEntry::dairy].diskSize,
                  postCommitDairySize);

        // And that meat remains the same as post-flush
        EXPECT_GT(summary[CollectionEntry::meat].diskSize,
                  preCompactionMeatSize);
        EXPECT_EQ(summary[CollectionEntry::meat].diskSize, postFlushMeatSize);
    }
    // MB-44590: Check that the dropped collection was cleaned-up
    auto [status, dropped] = store->getVBucket(vbid)
                                     ->getShard()
                                     ->getRWUnderlying()
                                     ->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_TRUE(dropped.empty());
}

TEST_P(CollectionsCouchstoreParameterizedTest, ConcCompactPrepareAbort) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    cm.add(CollectionEntry::dairy);
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 2);

    // We add dairy and a key to it just to ensure that we're not breaking
    // collections that may change during compaction but not during the replay
    StoredDocKey dairyKey{"milk", CollectionEntry::dairy};
    auto dairyPending = makePendingItem(dairyKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*dairyPending, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          "replica",
                          vb->getHighSeqno());
    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid, 1);

    StoredDocKey meatKey{"beef", CollectionEntry::meat};
    auto meatPending = makePendingItem(meatKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*meatPending, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto postCommitDairySize = 0;
    auto preCompactionMeatSize = 0;
    auto postFlushMeatSize = 0;

    {
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        postCommitDairySize = summary[CollectionEntry::dairy].diskSize;
        EXPECT_NE(0, postCommitDairySize);

        preCompactionMeatSize = summary[CollectionEntry::meat].diskSize;
    }

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool seenPrepare = false;
    kvstore.setConcurrentCompactionPreLockHook([&seenPrepare,
                                                &vb,
                                                &preCompactionMeatSize,
                                                &postFlushMeatSize,
                                                this](auto& compactionKey) {
        if (seenPrepare) {
            return;
        }
        seenPrepare = true;

        vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                     std::chrono::seconds(1000));
        vb->processResolvedSyncWrites();
        flushVBucketToDiskIfPersistent(vbid, 1);

        {
            Collections::Summary summary;
            vb->getManifest().lock().updateSummary(summary);

            // And that meat decreases
            EXPECT_LT(summary[CollectionEntry::meat].diskSize,
                      preCompactionMeatSize);
            postFlushMeatSize = summary[CollectionEntry::meat].diskSize;
        }
    });

    runCompaction(vbid, 0, false);

    {
        // Check that dairy decreases
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        EXPECT_LT(summary[CollectionEntry::dairy].diskSize,
                  postCommitDairySize);

        // And that meat does too
        EXPECT_LT(summary[CollectionEntry::meat].diskSize,
                  preCompactionMeatSize);
        EXPECT_EQ(summary[CollectionEntry::meat].diskSize, postFlushMeatSize);
    }
}

TEST_P(CollectionsCouchstoreParameterizedTest, ConcCompactAbortPrepare) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    cm.add(CollectionEntry::dairy);
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 2);

    // We add dairy and a key to it just to ensure that we're not breaking
    // collections that may change during compaction but not during the replay
    StoredDocKey dairyKey{"milk", CollectionEntry::dairy};
    auto dairyPending = makePendingItem(dairyKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*dairyPending, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          "replica",
                          vb->getHighSeqno());
    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid, 1);

    StoredDocKey meatKey{"beef", CollectionEntry::meat};
    auto meatPending = makePendingItem(meatKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*meatPending, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                 std::chrono::seconds(1000));
    vb->processResolvedSyncWrites();

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto postCommitDairySize = 0;
    auto preCompactionMeatSize = 0;
    auto postFlushMeatSize = 0;

    {
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        postCommitDairySize = summary[CollectionEntry::dairy].diskSize;
        EXPECT_NE(0, postCommitDairySize);

        preCompactionMeatSize = summary[CollectionEntry::meat].diskSize;
    }

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool seenPrepare = false;
    kvstore.setConcurrentCompactionPreLockHook([&seenPrepare,
                                                &vb,
                                                &preCompactionMeatSize,
                                                &postFlushMeatSize,
                                                this](auto& compactionKey) {
        if (seenPrepare) {
            return;
        }
        seenPrepare = true;

        StoredDocKey meatKey{"beef", CollectionEntry::meat};
        auto meatPending = makePendingItem(meatKey, "value");
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  store->set(*meatPending, cookie));

        flushVBucketToDiskIfPersistent(vbid, 1);

        {
            Collections::Summary summary;
            vb->getManifest().lock().updateSummary(summary);

            // And that meat increases
            EXPECT_GT(summary[CollectionEntry::meat].diskSize,
                      preCompactionMeatSize);
            postFlushMeatSize = summary[CollectionEntry::meat].diskSize;
        }
    });

    runCompaction(vbid, 0, false);

    {
        // Check that dairy decreases
        Collections::Summary summary;
        vb->getManifest().lock().updateSummary(summary);
        EXPECT_LT(summary[CollectionEntry::dairy].diskSize,
                  postCommitDairySize);

        // And that meat increases
        EXPECT_GT(summary[CollectionEntry::meat].diskSize,
                  preCompactionMeatSize);
        EXPECT_EQ(summary[CollectionEntry::meat].diskSize, postFlushMeatSize);
    }
}

TEST_P(CollectionsCouchstoreParameterizedTest, ConcCompactDropCollection) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm;
    cm.remove(CollectionEntry::defaultC);
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool seenPrepare = false;
    kvstore.setConcurrentCompactionPreLockHook([&seenPrepare, &vb, &cm, this](
                                                       auto& compactionKey) {
        if (seenPrepare) {
            return;
        }
        seenPrepare = true;

        // Flush something to ensure that we try to update the size
        StoredDocKey meatKey{"beef", CollectionEntry::meat};
        auto meatPending = makePendingItem(meatKey, "value");
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  store->set(*meatPending, cookie));

        flushVBucketToDiskIfPersistent(vbid, 1);

        // And drop the collection to check that we don't try to update the size
        cm.remove(CollectionEntry::meat);
        vb->updateFromManifest(makeManifest(cm));
        flushVBucketToDiskIfPersistent(vbid, 1);
    });

    runCompaction(vbid, 0, false);

    // Check that the compaction didn't fail. Before the fix it would fail as
    // we'd attempt to update the stats of the dropped collection and throw
    EXPECT_EQ(0, engine->getEpStats().compactionFailed);

    // No stats for the dropped collection
    EXPECT_EQ(0,
              getCollectionStats(vbid, {CollectionEntry::meat.getId()}).size());
}

// Test reproduces MB-44590, here we have a drop collection and then compaction
// and flusher interleave. With MB-44590, the final KVStore state was incorrect
// as the dropped collection metadata still stored the dropped collection.
TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactDropCollectionMB_44590) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Now drop default and add a second collection
    CollectionsManifest cm;
    cm.remove(CollectionEntry::defaultC);
    cm.add(CollectionEntry::fruit);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    kvstore.setConcurrentCompactionPreLockHook([&vb, this](auto&) {
        // Flush an item during compaction
        store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "v1");
        flushVBucketToDiskIfPersistent(vbid, 1);
    });

    runCompaction(vbid, 0, false);

    // At the end of the test, the dropped collections meta data should now
    // be empty. MB-44590 it was not empty.
    auto [status, dropped] = store->getVBucket(vbid)
                                     ->getShard()
                                     ->getRWUnderlying()
                                     ->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_TRUE(dropped.empty());

    // No stats for the dropped collection
    EXPECT_EQ(0, getCollectionStats(vbid, {CollectionID::Default}).size());
}

// MB-44590 and MB-44694. This test reproduces what was seen in MB-44694, but is
// fixed by MB-44590. The test drops collections and also tombstone purges them.
// When MB-44590 occurs a second compaction/erase gets quite confused because
// it cannot find the tombstones it thinks should exist.
TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactDropCollectionMB_44694) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Create two collections
    CollectionsManifest cm;
    cm.add(CollectionEntry::vegetable);
    cm.add(CollectionEntry::fruit);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "v1");

    flushVBucketToDiskIfPersistent(vbid, 1);

    // Now remove the fruit collection
    cm.remove(CollectionEntry::fruit);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Store an item, so that the fruit tombstone is not high-seqno
    store_item(vbid, StoredDocKey{"carrot", CollectionEntry::vegetable}, "v2");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Setup the compaction hook so that another collection drops - triggering
    // MB-44590
    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    kvstore.setConcurrentCompactionPreLockHook(
            [&vb, &cm, this](auto& compactionKey) {
                // Drop a collection during flush
                cm.remove(CollectionEntry::vegetable);
                vb->updateFromManifest(makeManifest(cm));
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    // compact and force purging of all deletes
    runCompaction(vbid, 0, true);

    // At the end of the first compaction, the dropped collections meta data
    // should not be empty. It must include the collection which was dropped
    // during compaction
    auto [status, dropped] = store->getVBucket(vbid)
                                     ->getShard()
                                     ->getRWUnderlying()
                                     ->getDroppedCollections(vbid);
    EXPECT_TRUE(status);
    EXPECT_EQ(1, dropped.size());
    EXPECT_EQ(CollectionEntry::vegetable.getId(), dropped.front().collectionId);

    kvstore.setConcurrentCompactionPreLockHook(
            [](auto& compactionKey) { return; });

    // With MB-44590, this would trigger the exception seen in MB-44694
    runCollectionsEraser(vbid);

    // At the end of the test, the dropped collections meta data should be empty
    std::tie(status, dropped) = store->getVBucket(vbid)
                                        ->getShard()
                                        ->getRWUnderlying()
                                        ->getDroppedCollections(vbid);
    EXPECT_TRUE(status);
    EXPECT_TRUE(dropped.empty());

    // No stats for the dropped collection
    EXPECT_EQ(0,
              getCollectionStats(vbid, {CollectionEntry::vegetable.getId()})
                      .size());
}

// Issue was found with MB-49472
TEST_P(CollectionsCouchstoreParameterizedTest,
       ResurrectCollectionDuringCompactionReplay) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Create and drop the fruit collection
    CollectionsManifest cm;
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    setCollections(cookie, cm.remove(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));
    kvstore.setConcurrentCompactionPreLockHook([&vb, &cm, this](
                                                       auto& compactionKey) {
        // Create and drop collection during compaction
        setCollections(cookie, cm.add(CollectionEntry::fruit));
        flushVBucketToDiskIfPersistent(vbid, 1);
        store_item(vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "v1");
        flushVBucketToDiskIfPersistent(vbid, 1);
        setCollections(cookie, cm.remove(CollectionEntry::fruit));
        flushVBucketToDiskIfPersistent(vbid, 1);
    });

    // use runCompaction here because more data needs purging, and
    // runCollectionsEraser expects no more to purge
    runCompaction(vbid, 0);

    // At the end of the first compaction, the dropped collections meta data
    // should not be empty. It must include the collection which was dropped
    // during compaction
    auto [status, dropped] = store->getVBucket(vbid)
                                     ->getShard()
                                     ->getRWUnderlying()
                                     ->getDroppedCollections(vbid);
    EXPECT_TRUE(status);
    EXPECT_EQ(1, dropped.size());
    EXPECT_EQ(CollectionEntry::fruit.getId(), dropped.front().collectionId);

    kvstore.setConcurrentCompactionPreLockHook(
            [](auto& compactionKey) { return; });

    // runCollectionsEraser checks that KVStore::getDroppedCollections is
    // empty after running
    runCollectionsEraser(vbid);

    // No stats for the dropped collection
    EXPECT_EQ(
            0,
            getCollectionStats(vbid, {CollectionEntry::fruit.getId()}).size());
}

void CollectionsCouchstoreParameterizedTest::ConcCompact(
        std::function<void()> concurrentFunc) {
    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    // Wrap the given hook with a function that will reset
    kvstore.setConcurrentCompactionPreLockHook(
            [&kvstore, concurrentFunc](auto& compactionKey) {
                concurrentFunc();
                kvstore.setConcurrentCompactionPreLockHook({});
            });
    runCompaction(vbid, 0, false);

    // Check that the compaction didn't fail.
    EXPECT_EQ(0, engine->getEpStats().compactionFailed);
}

TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactDocSizeDecrease_MB47460) {
    // Setup - create a second collection; then write a key to each of
    // default and vegetable collections.
    CollectionsManifest cm;
    auto key1 = StoredDocKey{"mb47460", CollectionEntry::defaultC};
    auto key2 = StoredDocKey{"mb47460", CollectionEntry::vegetable};
    cm.add(CollectionEntry::vegetable);
    store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto value = std::string{
            "A large value which will be replaced with a shorter one in the "
            "concurrent compaction hook below"};
    store_item(vbid, key1, value);
    store_item(vbid, key2, value);
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Grab the stats before compaction
    auto preCompactSizes =
            getCollectionStats(vbid,
                               {CollectionEntry::defaultC.getId(),
                                CollectionEntry::vegetable.getId()});

    // Setup testing hooks so key in each collection  is modified with a
    // *smaller* value, which will cause a second (incremental) compaction -
    // replay to occur. Before the fix a counter would throw an underflow
    // exception and compaction would fail.
    auto compactionFunc = [&key1, &key2, this]() {
        store_item(vbid, key1, "Small replacement value");
        store_item(vbid, key2, "Small replacement value");
        flushVBucketToDiskIfPersistent(vbid, 2);
    };

    ConcCompact(compactionFunc);

    // Validate the updated diskSizes
    auto postCompactSizes =
            getCollectionStats(vbid,
                               {CollectionEntry::defaultC.getId(),
                                CollectionEntry::vegetable.getId()});
    for (const auto& [cid, stats] : postCompactSizes) {
        auto itr = preCompactSizes.find(cid);
        ASSERT_NE(preCompactSizes.end(), itr);
        EXPECT_LT(stats.diskSize, itr->second.diskSize);
    }
}

// Check that increase a document works as expected
TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactDocSizeIncrease_MB47460) {
    // Setup - create a second collection; then write a key to each of
    // default and vegetable collections.
    CollectionsManifest cm;
    auto key1 = StoredDocKey{"mb47460", CollectionEntry::defaultC};
    auto key2 = StoredDocKey{"mb47460", CollectionEntry::vegetable};
    cm.add(CollectionEntry::vegetable);
    store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, key1, "Small value");
    store_item(vbid, key2, "Small value");
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Grab the stats before compaction
    auto preCompactSizes =
            getCollectionStats(vbid,
                               {CollectionEntry::defaultC.getId(),
                                CollectionEntry::vegetable.getId()});

    // Setup testing hooks so key in each collection  is modified with a
    // *smaller* value, which will cause a second (incremental) compaction -
    // replay to occur.
    auto compactionFunc = [&cm, &key1, &key2, this]() {
        auto value = std::string{
                "A large value which will be replaces the a shorter one we "
                "stored earlier"};
        store_item(vbid, key1, value);
        store_item(vbid, key2, value);
        flushVBucketToDiskIfPersistent(vbid, 2);
    };

    ConcCompact(compactionFunc);

    // Validate the updated diskSizes, diskSize must increase
    auto postCompactSizes =
            getCollectionStats(vbid,
                               {CollectionEntry::defaultC.getId(),
                                CollectionEntry::vegetable.getId()});
    for (const auto& [cid, stats] : postCompactSizes) {
        auto itr = preCompactSizes.find(cid);
        ASSERT_NE(preCompactSizes.end(), itr);
        EXPECT_GT(stats.diskSize, itr->second.diskSize);
    }
}

// This is the case that triggered MB_47460, an underflow would occur when a
// collection disk size decreased and then was dropped during the concurrent
// compaction
TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactDocSizeDecreaseDropCollection_MB47460) {
    CollectionsManifest cm;
    auto key1 = StoredDocKey{"mb47460", CollectionEntry::defaultC};
    auto key2 = StoredDocKey{"mb47460", CollectionEntry::vegetable};
    cm.add(CollectionEntry::vegetable);
    store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto value = std::string{
            "A large value which will be replaced with a shorter one in the "
            "concurrent compaction hook below"};
    store_item(vbid, key1, value);
    store_item(vbid, key2, value);
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Setup testing hooks so key in each collection  is modified with a
    // *smaller* value, which will cause a second (incremental) compaction -
    // replay to occur.
    auto compactionFunc = [&cm, &key1, &key2, this]() {
        // Decrease the size and then remove the collections
        store_item(vbid, key1, "Small value");
        store_item(vbid, key2, "Small value");
        cm.remove(CollectionEntry::defaultC);
        cm.remove(CollectionEntry::vegetable);
        store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
        flushVBucketToDiskIfPersistent(vbid, 4);
    };

    ConcCompact(compactionFunc);

    // Validate the state, no stats should exist for the dropped collections
    EXPECT_EQ(0,
              getCollectionStats(vbid,
                                 {CollectionEntry::defaultC.getId(),
                                  CollectionEntry::vegetable.getId()})
                      .size());

    // And finally, both default and vegetable should be listed in the dropped
    // collections local doc.
    auto& kvs = *store->getRWUnderlying(vbid);
    auto [droppedRead, droppedCollections] = kvs.getDroppedCollections(vbid);
    EXPECT_TRUE(droppedRead);
    ASSERT_EQ(2, droppedCollections.size());
    // Order of collections in this document doesn't matter, so just check
    // they are present
    std::vector<CollectionID> droppedCids;
    for (const auto& d : droppedCollections) {
        droppedCids.push_back(d.collectionId);
    }
    EXPECT_THAT(droppedCids,
                ::testing::UnorderedElementsAre(
                        CollectionEntry::defaultC.getId(),
                        CollectionEntry::vegetable.getId()));
}

TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactDocSizeIncreaseDropCollection_MB47460) {
    CollectionsManifest cm;
    auto key1 = StoredDocKey{"mb47460", CollectionEntry::defaultC};
    auto key2 = StoredDocKey{"mb47460", CollectionEntry::vegetable};
    cm.add(CollectionEntry::vegetable);
    store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, key1, "Small value");
    store_item(vbid, key2, "Small value");
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto compactionFunc = [&cm, &key1, &key2, this]() {
        // Increase the size and then remove the collections
        auto value =
                std::string{"A large value which to replace the small value"};
        store_item(vbid, key1, value);
        store_item(vbid, key2, value);
        cm.remove(CollectionEntry::defaultC);
        cm.remove(CollectionEntry::vegetable);
        store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
        flushVBucketToDiskIfPersistent(vbid, 4);
    };

    ConcCompact(compactionFunc);

    // Both default and vegetable should be listed in the dropped collections
    // local doc.
    auto& kvs = *store->getRWUnderlying(vbid);
    auto [droppedRead, droppedCollections] = kvs.getDroppedCollections(vbid);
    EXPECT_TRUE(droppedRead);
    ASSERT_EQ(2, droppedCollections.size());
    // Order of collections in this document doesn't matter, so just check
    // they are present
    std::vector<CollectionID> droppedCids;
    for (const auto& d : droppedCollections) {
        droppedCids.push_back(d.collectionId);
    }
    EXPECT_THAT(droppedCids,
                ::testing::UnorderedElementsAre(
                        CollectionEntry::defaultC.getId(),
                        CollectionEntry::vegetable.getId()));
}

TEST_P(CollectionsCouchstoreParameterizedTest, ConcCompactResurrectCollection) {
    CollectionsManifest cm;
    auto key_1a = StoredDocKey{"key_1", CollectionEntry::defaultC};
    auto key_2a = StoredDocKey{"key_1", CollectionEntry::vegetable};
    auto key_1b = StoredDocKey{"key_2", CollectionEntry::defaultC};
    auto key_2b = StoredDocKey{"key_2", CollectionEntry::vegetable};

    cm.add(CollectionEntry::vegetable);
    store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);
    // Store 2 items to each collection, this will help validation at the end
    store_item(vbid, key_1a, "Value to replace with something smaller");
    store_item(vbid, key_2a, "Value to replace with something smaller");
    store_item(vbid, key_1b, "Value to replace with something smaller");
    store_item(vbid, key_2b, "Value to replace with something smaller");
    flushVBucketToDiskIfPersistent(vbid, 4);

    auto preCompactSizes =
            getCollectionStats(vbid,
                               {CollectionEntry::defaultC.getId(),
                                CollectionEntry::vegetable.getId()});
    EXPECT_EQ(2, preCompactSizes.size());
    for (const auto& [cid, stats] : preCompactSizes) {
        // 2 items
        EXPECT_EQ(2, stats.itemCount) << "Failure for cid:" << cid.to_string();
    }

    auto compactionFunc = [&cm, &key_1a, &key_2a, this]() {
        store_item(vbid, key_1a, "Small value");
        store_item(vbid, key_2a, "Small value");
        flushVBucketToDiskIfPersistent(vbid, 2);

        cm.remove(CollectionEntry::defaultC);
        cm.remove(CollectionEntry::vegetable);
        store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
        flushVBucketToDiskIfPersistent(vbid, 2);

        cm.add(CollectionEntry::defaultC);
        cm.add(CollectionEntry::vegetable);
        store->getVBucket(vbid)->updateFromManifest(makeManifest(cm));
        store_item(vbid, key_1a, "Still smaller value");
        store_item(vbid, key_2a, "Still smaller value");
        flushVBucketToDiskIfPersistent(vbid, 4);
    };

    ConcCompact(compactionFunc);

    // validate, the new generation of the collections only stores 1 item so
    // must be smaller
    auto postCompactSizes =
            getCollectionStats(vbid,
                               {CollectionEntry::defaultC.getId(),
                                CollectionEntry::vegetable.getId()});
    EXPECT_EQ(2, postCompactSizes.size());
    EXPECT_EQ(1, postCompactSizes.count(CollectionEntry::defaultC.getId()));
    EXPECT_EQ(1, postCompactSizes.count(CollectionEntry::vegetable.getId()));

    for (const auto& [cid, stats] : postCompactSizes) {
        auto itr = preCompactSizes.find(cid);
        ASSERT_NE(preCompactSizes.end(), itr);
        // Less disk size
        EXPECT_LT(stats.diskSize, itr->second.diskSize)
                << "Failure for cid:" << cid.to_string();
        // 1 item
        EXPECT_EQ(1, stats.itemCount) << "Failure for cid:" << cid.to_string();
        // But a higher seqno
        EXPECT_GT(stats.highSeqno, itr->second.highSeqno)
                << "Failure for cid:" << cid.to_string();
    }
}

class ConcurrentCompactPurge : public CollectionsTest,
                               public ::testing::WithParamInterface<bool> {
public:
    void SetUp() override {
        CollectionsTest::SetUp();
        if (GetParam()) {
            setVBucketStateAndRunPersistTask(
                    vbid,
                    vbucket_state_active,
                    {{"topology",
                      nlohmann::json::array({{"active", "replica"}})}});
        }
    }
    void TearDown() override {
        CollectionsTest::TearDown();
    }
};

TEST_P(ConcurrentCompactPurge, ConcCompactPurgeTombstones) {
    replaceCouchKVStoreWithMock();
    auto vb = store->getVBucket(vbid);

    auto compareDiskStatMemoryVsPersisted = [vb]() {
        auto kvs = vb->getShard()->getRWUnderlying();
        auto fruitSz =
                vb->getManifest().lock(CollectionEntry::fruit).getDiskSize();
        auto stats =
                kvs->getCollectionStats(vb->getId(), CollectionEntry::fruit);
        EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, stats.first);
        EXPECT_EQ(stats.second.diskSize, fruitSz);
    };

    // 1) Add a collection, flush it and record the diskSize
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);
    auto diskSizeAtCreation =
            vb->getManifest().lock(CollectionEntry::fruit).getDiskSize();
    EXPECT_NE(0, diskSizeAtCreation);
    compareDiskStatMemoryVsPersisted();

    if (GetParam()) {
        // Include a prepare->commit. The prepare will be purged and we can
        // verify the collection disk-size reduces when tombstone and
        // prepare were purged
        StoredDocKey durable{"durian", CollectionEntry::fruit};
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  store->set(*makePendingItem(durable, "pong"), cookie));
        flushVBucketToDiskIfPersistent(vbid, 1);

        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                vb->getHighSeqno());
        vb->processResolvedSyncWrites();
        flushVBucketToDiskIfPersistent(vbid, 1);
    }

    // 2) Store two items and check the diskSize increased and save the new size
    StoredDocKey key1{"apple", CollectionEntry::fruit};
    StoredDocKey key2{"apricot", CollectionEntry::fruit};
    store_item(vbid, key1, "v1");
    store_item(vbid, key2, "v1");
    flushVBucketToDiskIfPersistent(vbid, 2);
    auto diskSizeWithTwoItems =
            vb->getManifest().lock(CollectionEntry::fruit).getDiskSize();
    EXPECT_GT(diskSizeWithTwoItems, diskSizeAtCreation);
    compareDiskStatMemoryVsPersisted();

    // 3) Delete both keys, but store with a value. This checks that tombstones
    // are accounted in the diskSize and allows the test to check what happens
    // when we purge a tombstone. Note the value is larger then the original,
    // so we have deleted two items but used more bytes.
    store_deleted_item(vbid, key1, "v2.0"); // <- to be purged
    store_deleted_item(vbid, key2, "v2.0"); // <- to be purged
    // Finally store an item in another collection in a different collection.
    // This becomes the high-seqno allowing all of our fruit keys to be purged
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 3);
    auto diskSizeWithTwoTombstones =
            vb->getManifest().lock(CollectionEntry::fruit).getDiskSize();
    EXPECT_GT(diskSizeWithTwoTombstones, diskSizeWithTwoItems);
    compareDiskStatMemoryVsPersisted();

    // 4) Compact and purge tombstones. Concurrently update one of the keys.
    // After compaction expect that the diskSize is still larger than at 1) and
    // smaller than at 3).
    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    kvstore.setConcurrentCompactionPreLockHook(
            [this, &key2](auto& compactionKey) {
                store_item(vbid, key2, "v2");
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    // Force tombstone purging
    runCompaction(vbid, 0, true);

    // For GetParam==false (no prepare/commit) fruit collection consists of:
    //    1) create-collection k:14 + v:57 + m:+18 = 88
    //    2) apricot{v2} k:8+ v:12 + m:18 = 38
    // For GetParam==true (prepare/commit) fruit collection consists of:
    //    1) create-collection k:14 + v:57 + m:+18 = 88
    //    2) durian{pong} k:8 +  v:14 + m:15 = 39
    //    3) apricot{v2} k:8+ v:12 + m:18 = 38
    auto expectedSz = 0;
    if (!GetParam()) {
        expectedSz = 89 + 38;
    } else {
        expectedSz = 89 + 39 + 38;
    }

    auto diskSizeFinal =
            vb->getManifest().lock(CollectionEntry::fruit).getDiskSize();
    EXPECT_EQ(expectedSz, diskSizeFinal);
    compareDiskStatMemoryVsPersisted();
}

INSTANTIATE_TEST_SUITE_P(ConcurrentCompactPurgeTests,
                         ConcurrentCompactPurge,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

// Test the pager doesn't generate expired items for a dropped collection
TEST_P(CollectionsPersistentParameterizedTest,
       collections_expiry_after_drop_collection_pager) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the meat collection + 1 item with TTL (and flush it all out)
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));
    StoredDocKey key{"lamb", CollectionEntry::meat};
    store_item(vbid, key, "value", ep_real_time() + 100);
    flushVBucketToDiskIfPersistent(vbid, 2);
    // And now drop the meat collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::meat)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Time travel
    TimeTraveller docBrown(2000);

    // Now run the pager to force expiry of our little lamb
    auto task = std::make_shared<ExpiredItemPager>(*engine,
                                                   engine->getEpStats(),
                                                   0,
                                                   -1 /* taskTime */,
                                                   1 /* concurrency */);
    static_cast<ExpiredItemPager*>(task.get())->run();
    runNextTask(*task_executor->getLpTaskQ()[NONIO_TASK_IDX],
                "Expired item remover no vbucket assigned");

    std::vector<queued_item> items;
    vb->checkpointManager->getNextItemsForPersistence(items);

    // No mutation of the original key is allowed as it would invalidate the
    // ordering of create @x, item @y, drop @z  x < y < z
    for (auto& i : items) {
        EXPECT_NE(key, i->getKey());
    }
}

// Test to ensure the callback passed to engine->get_connection_manifest(...)
// will track any allocations against "non-bucket"
TEST_P(CollectionsParameterizedTest,
       GetCollectionManifestResponseCBAllocsUnderNonBucket) {
    auto addResponseFn = [](std::string_view key,
                            std::string_view extras,
                            std::string_view body,
                            uint8_t datatype,
                            cb::mcbp::Status status,
                            uint64_t cas,
                            const void* cookie) -> bool {
        // This callback should run in the memcached-context - there should be
        // no associated engine.
        EXPECT_FALSE(ObjectRegistry::getCurrentEngine());
        return true;
    };
    engine->get_collection_manifest(*cookie, addResponseFn);
}

class CollectionsExpiryLimitTest : public CollectionsTest,
                                   public ::testing::WithParamInterface<bool> {
public:
    void SetUp() override {
        config_string += "max_ttl=86400";
        CollectionsTest::SetUp();
    }

    void operation_test(
            std::function<void(Vbid, DocKey, std::string)> storeFunc,
            bool warmup);
};

void CollectionsExpiryLimitTest::operation_test(
        std::function<void(Vbid, DocKey, std::string)> storeFunc, bool warmup) {
    CollectionsManifest cm;
    // meat collection defines no expiry (overriding bucket ttl)
    cm.add(CollectionEntry::meat, std::chrono::seconds(0));
    // fruit defines nothing, gets bucket ttl
    cm.add(CollectionEntry::fruit);
    // dairy has its own expiry, greater than bucket
    cm.add(CollectionEntry::dairy, std::chrono::seconds(500000));
    // vegetable has its own expiry, less than bucket
    cm.add(CollectionEntry::vegetable, std::chrono::seconds(380));

    {
        VBucketPtr vb = store->getVBucket(vbid);
        vb->updateFromManifest(makeManifest(cm));
    }

    flushVBucketToDiskIfPersistent(vbid, 4);

    if (warmup) {
        resetEngineAndWarmup();
    }

    StoredDocKey meaty{"lamb", CollectionEntry::meat};
    StoredDocKey fruity{"apple", CollectionEntry::fruit};
    StoredDocKey milky{"milk", CollectionEntry::dairy};
    StoredDocKey potatoey{"potato", CollectionEntry::vegetable};

    storeFunc(vbid, meaty, "meaty");
    storeFunc(vbid, fruity, "fruit");
    storeFunc(vbid, milky, "milky");
    storeFunc(vbid, potatoey, "potatoey");

    auto f = [](const item_info&) { return true; };

    // verify meaty has 0 expiry
    auto rval = engine->getIfInner(cookie, meaty, vbid, f);
    ASSERT_EQ(cb::engine_errc::success, rval.first);
    Item* i = reinterpret_cast<Item*>(rval.second.get());
    auto info = engine->getItemInfo(*i);
    EXPECT_EQ(0, info.exptime);

    // Now the rest, we expect fruity to have the bucket ttl
    // we can expect milky to be > fruity
    // we can expect potatoey to be < fruity
    auto fruityValue = engine->getIfInner(cookie, fruity, vbid, f);
    auto milkyValue = engine->getIfInner(cookie, milky, vbid, f);
    auto potatoeyValue = engine->getIfInner(cookie, potatoey, vbid, f);
    ASSERT_EQ(cb::engine_errc::success, fruityValue.first);
    ASSERT_EQ(cb::engine_errc::success, milkyValue.first);
    ASSERT_EQ(cb::engine_errc::success, potatoeyValue.first);

    auto fruityInfo = engine->getItemInfo(
            *reinterpret_cast<Item*>(fruityValue.second.get()));
    auto milkyInfo = engine->getItemInfo(
            *reinterpret_cast<Item*>(milkyValue.second.get()));
    auto potatoeyInfo = engine->getItemInfo(
            *reinterpret_cast<Item*>(potatoeyValue.second.get()));

    EXPECT_NE(0, fruityInfo.exptime);
    EXPECT_NE(0, milkyInfo.exptime);
    EXPECT_NE(0, potatoeyInfo.exptime);
    EXPECT_GT(milkyInfo.exptime, fruityInfo.exptime);
    EXPECT_LT(potatoeyInfo.exptime, fruityInfo.exptime);
}

TEST_P(CollectionsExpiryLimitTest, set) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        EXPECT_EQ(0, item.getExptime());
        EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, add) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        EXPECT_EQ(0, item.getExptime());
        EXPECT_EQ(cb::engine_errc::success, store->add(item, cookie));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, replace) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        EXPECT_EQ(0, item.getExptime());
        EXPECT_EQ(cb::engine_errc::success, store->add(item, cookie));
        EXPECT_EQ(cb::engine_errc::success, store->replace(item, cookie));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, set_with_meta) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        auto item = make_item(vb, k, v);
        item.setCas(1);
        EXPECT_EQ(0, item.getExptime());
        uint64_t cas = 0;
        uint64_t seqno = 0;
        EXPECT_EQ(cb::engine_errc::success,
                  store->setWithMeta(item,
                                     cas,
                                     &seqno,
                                     cookie,
                                     {vbucket_state_active},
                                     CheckConflicts::No,
                                     true,
                                     GenerateBySeqno::Yes,
                                     GenerateCas::No,
                                     nullptr));
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsExpiryLimitTest, gat) {
    auto func = [this](Vbid vb, DocKey k, std::string v) {
        Item item = store_item(vb, k, v, 0);

        // re touch to 0
        auto rval = engine->getAndTouchInner(cookie, k, vb, 0);
        ASSERT_EQ(cb::engine_errc::success, rval.first);
    };
    operation_test(func, GetParam());
}

TEST_P(CollectionsParameterizedTest, item_counting) {
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));

    // Default collection is open for business
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");

    // 1 system event + 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::meat));

    store_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat}, "value");
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    // Now modify our two items
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    store_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat}, "value");
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    // Now delete our two items
    delete_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC});
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionEntry::meat));

    delete_item(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat});
    // 1 item
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::defaultC));
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionEntry::meat));
}

TEST_P(CollectionsParameterizedTest, CollectionStatsIncludesScope) {
    // Test that stats returned for key "collections" includes what scope
    // the collection is in
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, ScopeEntry::shop1);
    cm.add(ScopeEntry::shop2);
    cm.add(CollectionEntry::meat, ScopeEntry::shop2);
    cm.add(CollectionEntry::fruit, ScopeEntry::shop2);
    setCollections(cookie, cm);

    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 5);

    const auto makeStatPair = [](const ScopeEntry::Entry& scope,
                                 const CollectionEntry::Entry& collection) {
        // scope name is present in all collection stats, arbitrarily check the
        // ID stat exists and contains the scope name.
        return std::make_pair(fmt::format("{}:{}:scope_name",
                                          scope.getId().to_string(),
                                          collection.getId().to_string()),
                              scope.name);
    };

    std::map<std::string, std::string> expected{
            makeStatPair(ScopeEntry::defaultS, CollectionEntry::defaultC),
            makeStatPair(ScopeEntry::shop1, CollectionEntry::dairy),
            makeStatPair(ScopeEntry::shop2, CollectionEntry::meat),
            makeStatPair(ScopeEntry::shop2, CollectionEntry::fruit)};

    std::map<std::string, std::string> actual;
    const auto addStat = [&actual](std::string_view key,
                                   std::string_view value,
                                   const void* ctx) {
        actual[std::string(key)] = value;
    };

    auto cookie = create_mock_cookie();
    engine->doCollectionStats(cookie, addStat, "collections");
    destroy_mock_cookie(cookie);

    using namespace testing;

    for (const auto& exp : expected) {
        // newer GTest brings IsSubsetOf which could replace this
        EXPECT_THAT(actual, Contains(exp));
    }
}

TEST_P(CollectionsParameterizedTest, PerCollectionMemUsed) {
    // test that the per-collection memory usage (tracked by the hash table
    // statistics) changes when items in the collection are
    // added/updated/deleted/evicted and does not change when items in other
    // collections are similarly changed.
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));

    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    {
        SCOPED_TRACE("new item added to collection");
        // default collection memory usage should _increase_
        auto d = MemChecker(vb, CollectionEntry::defaultC, std::greater<>());
        // meta collection memory usage should _stay the same_
        auto m = MemChecker(vb, CollectionEntry::meat, std::equal_to<>());

        store_item(
                vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("new item added to collection");
        auto d = MemChecker(vb, CollectionEntry::defaultC, std::equal_to<>());
        auto m = MemChecker(vb, CollectionEntry::meat, std::greater<>());

        store_item(vbid,
                   StoredDocKey{"meat:beef", CollectionEntry::meat},
                   "value");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("update item with larger value");
        auto d = MemChecker(vb, CollectionEntry::defaultC, std::greater<>());
        auto m = MemChecker(vb, CollectionEntry::meat, std::equal_to<>());

        store_item(vbid,
                   StoredDocKey{"key", CollectionEntry::defaultC},
                   "valuesdfasdfasdfasdfasdfsadf");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("delete item");
        auto d = MemChecker(vb, CollectionEntry::defaultC, std::less<>());
        auto m = MemChecker(vb, CollectionEntry::meat, std::equal_to<>());

        delete_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC});
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    if (ephemeral()) {
        return;
    }

    {
        SCOPED_TRACE("evict item");
        auto d = MemChecker(vb, CollectionEntry::defaultC, std::equal_to<>());
        auto m = MemChecker(vb, CollectionEntry::meat, std::less<>());

        evict_key(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat});
    }
}

TEST_P(CollectionsPersistentParameterizedTest, PerCollectionDiskSize) {
    // test that the per-collection disk size (updated by saveDocsCallback)
    // changes when items in the collection are added/updated/deleted (but not
    // when evicted) and does not change when items in other collections are
    // similarly changed.
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));

    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    {
        SCOPED_TRACE("new item added to collection");
        // default collection disk size should _increase_
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::greater<>());
        // meta collection disk size should _stay the same_
        auto m = DiskChecker(vb, CollectionEntry::meat, std::equal_to<>());

        store_item(
                vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("new item added to collection");
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::equal_to<>());
        auto m = DiskChecker(vb, CollectionEntry::meat, std::greater<>());

        store_item(vbid,
                   StoredDocKey{"meat:beef", CollectionEntry::meat},
                   "value");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("update item with larger value");
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::greater<>());
        auto m = DiskChecker(vb, CollectionEntry::meat, std::equal_to<>());

        store_item(vbid,
                   StoredDocKey{"key", CollectionEntry::defaultC},
                   "valuesdfasdfasdfasdfasdfsadf");
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("delete item");
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::less<>());
        auto m = DiskChecker(vb, CollectionEntry::meat, std::equal_to<>());

        delete_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC});
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);

        if (isMagma()) {
            // Magma doesn't track tombstones so size should go back to 0
            EXPECT_EQ(getCollectionDiskSize(vb, CollectionEntry::defaultC.uid),
                      0);
        } else {
            EXPECT_GT(getCollectionDiskSize(vb, CollectionEntry::defaultC.uid),
                      0);
        }
    }

    {
        SCOPED_TRACE("evict item");
        // should not change the on disk size
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::equal_to<>());
        auto m = DiskChecker(vb, CollectionEntry::meat, std::equal_to<>());

        evict_key(vbid, StoredDocKey{"meat:beef", CollectionEntry::meat});
    }
}

TEST_P(CollectionsPersistentParameterizedTest, PerCollectionDiskSizeRollback) {
    resetEngineAndWarmup(magmaRollbackConfig);
    store->setVBucketState(vbid, vbucket_state_active);

    // test that the per-collection disk size (updated by saveDocsCallback)
    // changes when items in the collection are added/updated/deleted (but not
    // when evicted) and does not change when items in other collections are
    // similarly changed.
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));

    KVBucketTest::flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");
    KVBucketTest::flushVBucketToDiskIfPersistent(vbid);

    {
        SCOPED_TRACE("Rollback");
        // post rollback, stats should have been reset to what they are
        // currently
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::equal_to<>());
        auto m = DiskChecker(vb, CollectionEntry::meat, std::equal_to<>());

        auto seqnoToRollbackTo = vb->getHighSeqno();

        {
            // now add another item to change the stats. This item will be
            // removed by rollback
            SCOPED_TRACE("new item added to collection");
            // default collection disk size should _increase_ as we add the item
            auto d = DiskChecker(
                    vb, CollectionEntry::defaultC, std::greater<>());
            // meta collection disk size should _stay the same_
            auto m = DiskChecker(vb, CollectionEntry::meat, std::equal_to<>());

            store_item(vbid,
                       StoredDocKey{"key2", CollectionEntry::defaultC},
                       "value");
            KVBucketTest::flushVBucketToDiskIfPersistent(vbid);

            // checkers go out of scope and confirm expected stat changes
        }

        // force to replica to allow rollback
        ASSERT_EQ(cb::engine_errc::success,
                  store->setVBucketState(vbid, vbucket_state_replica));

        // definitely will be rolling back an item
        ASSERT_NE(seqnoToRollbackTo, vb->getHighSeqno());

        store->rollback(vbid, seqnoToRollbackTo);

        // definitely rolled back
        ASSERT_EQ(seqnoToRollbackTo, vb->getHighSeqno());

        // Checkers go out of scope and verify stats were reset to
        // original values
    }
}

/**
 * 'Sanitize' the StatChecker PostFunc supplied after taking into consideration
 * the test backend. Magma does not track prepares so a few stats work
 * differently.
 */
StatChecker::PostFunc getPrepareStatCheckerPostFuncForBackend(
        std::string backend, StatChecker::PostFunc fn) {
    if (backend.find(":backend=magma") != std::string::npos ||
        backend.find("nexus_primary_backend=magma") != std::string::npos) {
        // Magma doesn't currently track prepares as we remove them during
        // compaction and we don't know if we are removing a stale one or not,
        // making it impossible to count them accurately. As such we also cannot
        // count prepare bytes and we do not include prepares in the collection
        // disk size.
        return std::equal_to<>();
    }

    return fn;
}

TEST_P(CollectionsPersistentParameterizedTest,
       PerCollectionDiskSizeDurability) {
    // test that the per-collection disk size (updated by saveDocsCallback)
    // changes when items in the collection are added/updated/deleted (but not
    // when evicted) and does not change when items in other collections are
    // similarly changed.
    auto vb = store->getVBucket(vbid);

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto key = StoredDocKey{"key", CollectionEntry::defaultC};

    {
        SCOPED_TRACE("Prepare item");
        // default collection disk size should _increase_ - prepares are
        // included
        auto d = DiskChecker(vb,
                             CollectionEntry::defaultC,
                             getPrepareStatCheckerPostFuncForBackend(
                                     getBackend(), std::greater<>()));

        using namespace cb::durability;
        store_item(vbid,
                   key,
                   "value",
                   0 /* exptime */,
                   {cb::engine_errc::sync_write_pending} /* expected */,
                   PROTOCOL_BINARY_DATATYPE_JSON /* datatype */,
                   Requirements(Level::Majority, Timeout(10)));
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("Commit item");
        // default collection disk size should _increase_
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::greater<>());

        vb->commit(key, vb->getHighSeqno(), {}, vb->lockCollections(key));
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("Purge completed prepare");
        // default collection disk size should _decrease_

        StatChecker::PostFunc comparator = std::less<>();
        if (isPitrEnabled()) {
            // With PiTR enabled we won't purge the prepare
            comparator = std::equal_to<>();
        }

        auto d = DiskChecker(vb,
                             CollectionEntry::defaultC,
                             getPrepareStatCheckerPostFuncForBackend(
                                     getBackend(), comparator));
        runCompaction(vbid, 0, false);
    }

    {
        SCOPED_TRACE("Warmup");
        auto d = DiskChecker(vb, CollectionEntry::defaultC, std::equal_to<>());
        vb.reset();
        resetEngineAndWarmup();
        vb = store->getVBucket(vbid);
    }

    {
        SCOPED_TRACE("Prepare item round 2");
        // default collection disk size should _increase_ - prepares are
        // included

        StatChecker::PostFunc comparator = std::greater<>();
        if (isPitrEnabled()) {
            // With PiTR enabled we haven't purge the previous version so no
            // change is expected
            comparator = std::equal_to<>();
        }

        auto d = DiskChecker(vb,
                             CollectionEntry::defaultC,
                             getPrepareStatCheckerPostFuncForBackend(
                                     getBackend(), comparator));

        using namespace cb::durability;
        store_item(vbid,
                   key,
                   "value",
                   0 /* exptime */,
                   {cb::engine_errc::sync_write_pending} /* expected */,
                   PROTOCOL_BINARY_DATATYPE_JSON /* datatype */,
                   Requirements(Level::Majority, Timeout(10)));
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }

    {
        SCOPED_TRACE("Abort item");
        // default collection disk size should _decrease_ - aborts are
        // included so should decrease
        auto d = DiskChecker(vb,
                             CollectionEntry::defaultC,
                             getPrepareStatCheckerPostFuncForBackend(
                                     getBackend(), std::less<>()));

        vb->abort(key, vb->getHighSeqno(), {}, vb->lockCollections(key));
        KVBucketTest::flushVBucketToDiskIfPersistent(vbid);
    }
}

// Test to ensure we use the vbuckets manifest when passing a vbid to
// EventuallyPersistentEngine::get_scope_id()
TEST_P(CollectionsParameterizedTest, GetScopeIdForGivenKeyAndVbucket) {
    VBucketPtr vb = store->getVBucket(vbid);
    // Add the dairy collection to vbid(0)
    CollectionsManifest cmDairyVb;
    cmDairyVb.add(ScopeEntry::shop1)
            .add(CollectionEntry::dairy, ScopeEntry::shop1);
    vb->updateFromManifest(makeManifest(cmDairyVb));

    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(vbid, 2);

    StoredDocKey keyDairy{"dairy:milk", CollectionEntry::dairy};
    StoredDocKey keyMeat{"meat:beef", CollectionEntry::meat};

    auto result = engine->get_scope_id(*cookie, CollectionEntry::dairy, vbid);
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(cmDairyVb.getUid(), result.getManifestId());
    EXPECT_EQ(ScopeID(ScopeEntry::shop1), result.getScopeId());

    result = engine->get_scope_id(*cookie, CollectionEntry::meat, vbid);
    EXPECT_EQ(cb::engine_errc::unknown_collection, result.result);
    EXPECT_EQ(2, result.getManifestId());

    StoredDocKey keyFruit{"fruit:apple", CollectionEntry::fruit};
    // Add the meat collection to vbid(1)
    Vbid meatVbid(1);

    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(meatVbid, vbucket_state_replica));
    auto replicaVb = store->getVBucket(meatVbid);

    result = engine->get_scope_id(*cookie, CollectionEntry::dairy, meatVbid);
    EXPECT_EQ(cb::engine_errc::unknown_collection, result.result);
    EXPECT_EQ(0, result.getManifestId());

    replicaVb->checkpointManager->createSnapshot(
            0, 2, std::nullopt, CheckpointType::Memory, 2);
    replicaVb->replicaCreateScope(
            Collections::ManifestUid(1), ScopeUid::shop1, ScopeName::shop1, 1);
    replicaVb->replicaCreateCollection(
            Collections::ManifestUid(2),
            {ScopeUid::shop1, CollectionEntry::meat.getId()},
            CollectionEntry::meat.name,
            {},
            2);
    // Trigger a flush to disk. Flushes the dairy create event.
    flushVBucketToDiskIfPersistent(meatVbid, 2);

    result = engine->get_scope_id(*cookie, CollectionEntry::meat, meatVbid);
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(2, result.getManifestId());
    EXPECT_EQ(ScopeUid::shop1, result.getScopeId());

    result = engine->get_scope_id(*cookie, CollectionEntry::fruit, meatVbid);
    EXPECT_EQ(cb::engine_errc::unknown_collection, result.result);
    EXPECT_EQ(2, result.getManifestId());

    // check vbucket that doesnt exist
    result = engine->get_scope_id(*cookie, CollectionEntry::dairy, Vbid(10));
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, result.result);
}

TEST_P(CollectionsParameterizedTest, GetScopeIdForGivenKeyNoVbid) {
    VBucketPtr vb = store->getVBucket(vbid);

    CollectionsManifest manifest;
    manifest.add(ScopeEntry::shop1)
            .add(CollectionEntry::dairy, ScopeEntry::shop1);

    setCollections(cookie, manifest);
    flushVBucketToDiskIfPersistent(vbid, 2);

    StoredDocKey keyDefault{"default", CollectionEntry::defaultC};
    StoredDocKey keyDairy{"dairy:milk", CollectionEntry::dairy};
    StoredDocKey keyMeat{"meat:beef", CollectionEntry::meat};

    auto result = engine->get_scope_id(*cookie, CollectionEntry::defaultC, {});
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(0, result.getManifestId());
    EXPECT_EQ(ScopeUid::defaultS, result.getScopeId());

    result = engine->get_scope_id(*cookie, CollectionEntry::dairy, {});
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(2, result.getManifestId());
    EXPECT_EQ(ScopeUid::shop1, result.getScopeId());

    result = engine->get_scope_id(*cookie, CollectionEntry::meat, {});
    EXPECT_EQ(cb::engine_errc::unknown_collection, result.result);
    EXPECT_EQ(2, result.getManifestId());
}

static std::set<std::string> lastGetKeysResult;

bool getAllKeysResponseHandler(std::string_view key,
                               std::string_view extras,
                               std::string_view body,
                               uint8_t datatype,
                               cb::mcbp::Status status,
                               uint64_t cas,
                               const void* cookie) {
    lastGetKeysResult.clear();

    const char* strPtr = body.data();
    auto* sizePtr = reinterpret_cast<const uint16_t*>(strPtr);
    while (strPtr != (body.data() + body.size())) {
        uint16_t strLen = ntohs(*sizePtr);
        strPtr += sizeof(uint16_t);

        lastGetKeysResult.insert({strPtr, strLen});
        sizePtr = reinterpret_cast<const uint16_t*>(strPtr + strLen);
        strPtr += strLen;
    }
    return true;
}

static std::string makeCollectionEncodedString(std::string key,
                                               CollectionID cid) {
    auto storedDocKey = makeStoredDocKey(key, cid);
    return {reinterpret_cast<const char*>(storedDocKey.data()),
            storedDocKey.size()};
}

cb::engine_errc CollectionsTest::sendGetKeys(std::string startKey,
                                             std::optional<uint32_t> maxCount,
                                             const AddResponseFn& response) {
    using namespace cb::mcbp;
    lastGetKeysResult.clear();
    std::string exts;
    if (maxCount) {
        auto packLevelCount = htonl(*maxCount);
        exts = {reinterpret_cast<char*>(&packLevelCount), sizeof(uint32_t)};
    }

    auto request = createPacket(ClientOpcode::GetKeys, vbid, 0, exts, startKey);
    return engine->getAllKeys(cookie, *request, response);
}

std::set<std::string> CollectionsTest::generateExpectedKeys(
        std::string_view keyPrefix, size_t numOfItems, CollectionID cid) {
    std::set<std::string> generatedKeys;
    for (size_t i = 0; i < numOfItems; i++) {
        std::string currentKey(keyPrefix);
        currentKey += std::to_string(i);
        if (cookie->isCollectionsSupported()) {
            StoredDocKey sDocKey{currentKey, cid};
            generatedKeys.insert({reinterpret_cast<const char*>(sDocKey.data()),
                                  sDocKey.size()});
        } else {
            generatedKeys.insert(currentKey);
        }
    }

    return generatedKeys;
}

TEST_P(CollectionsPersistentParameterizedTest,
       GetAllKeysNonCollectionConnection) {
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Create and flush items for the default and meat collections
    ASSERT_TRUE(store_items(10,
                            vbid,
                            makeStoredDocKey("beef", CollectionEntry::meat),
                            "value"));
    flushVBucketToDiskIfPersistent(vbid, 10);

    ASSERT_TRUE(store_items(5, vbid, makeStoredDocKey("default"), "value"));
    flushVBucketToDiskIfPersistent(vbid, 5);

    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys("default0", {}, getAllKeysResponseHandler));
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");
    EXPECT_EQ(generateExpectedKeys("default", 5), lastGetKeysResult);
}

TEST_P(CollectionsPersistentParameterizedTest,
       GetAllKeysNonCollectionConnectionMaxCountTen) {
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Create and flush items for the default collection
    ASSERT_TRUE(store_items(20, vbid, makeStoredDocKey("default"), "value"));
    flushVBucketToDiskIfPersistent(vbid, 20);

    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys("default0", {10}, getAllKeysResponseHandler));
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");
    EXPECT_EQ(10, lastGetKeysResult.size());
}

TEST_P(CollectionsPersistentParameterizedTest, GetAllKeysStartHalfWay) {
    ASSERT_TRUE(store_items(4, vbid, makeStoredDocKey("default"), "value"));
    flushVBucketToDiskIfPersistent(vbid, 4);

    // All keys default2 and after from default collection
    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys("default2", {}, getAllKeysResponseHandler));
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");
    std::set<std::string> twoKeys;
    twoKeys.insert("default2");
    twoKeys.insert("default3");
    EXPECT_EQ(twoKeys, lastGetKeysResult);
}

TEST_P(CollectionsPersistentParameterizedTest,
       GetAllKeysStartHalfWayForCollection) {
    // Enable collections on mock connection
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_TRUE(store_items(
            4, vbid, makeStoredDocKey("meat", CollectionEntry::meat), "value"));
    flushVBucketToDiskIfPersistent(vbid, 4);

    // All keys meat2 and after from meat collection
    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys(makeCollectionEncodedString("meat2",
                                                      CollectionEntry::meat),
                          {},
                          getAllKeysResponseHandler));
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");
    std::set<std::string> twoKeys;
    twoKeys.insert("\bmeat2");
    twoKeys.insert("\bmeat3");
    EXPECT_EQ(twoKeys, lastGetKeysResult);
}

TEST_P(CollectionsPersistentParameterizedTest,
       GetAllKeysForCollectionEmptyKey) {
    // Enable collections on mock connection
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_TRUE(store_items(
            4, vbid, makeStoredDocKey("meat", CollectionEntry::meat), "value"));
    flushVBucketToDiskIfPersistent(vbid, 4);

    // All keys meat2 and after from meat collection
    EXPECT_EQ(
            cb::engine_errc::would_block,
            sendGetKeys(makeCollectionEncodedString("", CollectionEntry::meat),
                        {},
                        getAllKeysResponseHandler));
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");

    EXPECT_EQ(generateExpectedKeys("meat", 4, CollectionEntry::meat),
              lastGetKeysResult);
}

TEST_P(CollectionsPersistentParameterizedTest,
       GetAllKeysNonCollectionConnectionCidEncodeKey) {
    ASSERT_TRUE(store_items(5, vbid, makeStoredDocKey("default"), "value"));
    flushVBucketToDiskIfPersistent(vbid, 5);

    // Ensure we treat any key as part of th default collection on a
    // no collection enabled connection.
    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys(makeCollectionEncodedString("default4",
                                                      CollectionEntry::meat),
                          {},
                          getAllKeysResponseHandler));
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");

    // As the requested key does not match any of the stored keys we expect
    // to get all keys in the default collection back
    EXPECT_EQ(generateExpectedKeys("default", 5), lastGetKeysResult);
}

TEST_P(CollectionsPersistentParameterizedTest, GetAllKeysCollectionConnection) {
    // Enable collections on mock connection
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Create and flush items for the default and meat collections
    ASSERT_TRUE(store_items(10,
                            vbid,
                            makeStoredDocKey("beef", CollectionEntry::meat),
                            "value"));
    flushVBucketToDiskIfPersistent(vbid, 10);

    ASSERT_TRUE(store_items(5, vbid, makeStoredDocKey("default"), "value"));
    flushVBucketToDiskIfPersistent(vbid, 5);

    // Get the keys for default collection, in this case we should get all 5
    // keys
    std::string startKey =
            makeCollectionEncodedString("default0", CollectionEntry::defaultC);
    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));
    // as we got cb::engine_errc::would_block we need to manually call the
    // GetKeys task
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");
    EXPECT_EQ(generateExpectedKeys("default", 5), lastGetKeysResult);
    // Calling GetKeys again should return cb::engine_errc::success indicating
    // all keys have been fetched
    EXPECT_EQ(cb::engine_errc::success,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));

    startKey = makeCollectionEncodedString("beef0", CollectionEntry::meat);
    // Get the keys for meat collection, in this case we should get all 10 keys
    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));
    // as we got cb::engine_errc::would_block we need to manually call the
    // GetKeys task
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");
    EXPECT_EQ(generateExpectedKeys("beef", 10, CollectionEntry::meat),
              lastGetKeysResult);
    // Calling GetKeys again should return cb::engine_errc::success indicating
    // all keys have been fetched
    EXPECT_EQ(cb::engine_errc::success,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));
}

static bool wasKeyStatsResponseHandlerCalled = false;
bool getKeyStatsResponseHandler(std::string_view key,
                                std::string_view value,
                                const void* ctx) {
    wasKeyStatsResponseHandlerCalled = true;
    return true;
}

TEST_P(CollectionsParameterizedTest, TestGetKeyStatsBadVbids) {
    store_item(vbid,
               makeStoredDocKey("defKey", CollectionEntry::defaultC),
               "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    wasKeyStatsResponseHandlerCalled = false;
    std::string key("key-byid defKey -1");
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    key = "key-byid defKey asd";
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    key = "key-byid defKey 1000000";
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    key = "key-byid defKey 1";
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
}

TEST_P(CollectionsParameterizedTest, TestGetKeyStats) {
    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid,
               makeStoredDocKey("defKey", CollectionEntry::defaultC),
               "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, makeStoredDocKey("beef", CollectionEntry::meat), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    wasKeyStatsResponseHandlerCalled = false;
    // non collection style request with trailing whitespace
    std::string key("key-byid defKey 0   ");
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key-byid beef 0 0x8";
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key beef 0 _default.meat";
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key beef 0 _default.fruit";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key-byid beef 0 0x9";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key beef2 0 _default._default";
    auto ret = engine->getStats(cookie, key, {}, getKeyStatsResponseHandler);
    if (!isFullEviction() || isBloomFilterEnabled()) {
        EXPECT_EQ(cb::engine_errc::no_such_key, ret);
    } else {
        EXPECT_EQ(cb::engine_errc::would_block, ret);
        runBGFetcherTask();
        EXPECT_EQ(
                cb::engine_errc::no_such_key,
                engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    }
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
}

TEST_P(CollectionsPersistentParameterizedTest, TestGetVKeyStats) {
    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid,
               makeStoredDocKey("defKey", CollectionEntry::defaultC),
               "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, makeStoredDocKey("beef", CollectionEntry::meat), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    wasKeyStatsResponseHandlerCalled = false;
    // non collection style request with trailing whitespace
    std::string key("vkey defKey 0   ");
    EXPECT_EQ(cb::engine_errc::would_block,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
    // as we got cb::engine_errc::would_block we need to manually call the
    // VKeyStatBGFetchTask task
    runNextTask(
            *task_executor->getLpTaskQ()[READER_TASK_IDX],
            "Fetching item from disk for vkey stat: key{cid:0x0:defKey} vb:0");
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey-byid beef 0 0x8";
    EXPECT_EQ(cb::engine_errc::would_block,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
    // as we got cb::engine_errc::would_block we need to manually call the
    // VKeyStatBGFetchTask task
    runNextTask(
            *task_executor->getLpTaskQ()[READER_TASK_IDX],
            "Fetching item from disk for vkey stat: key{cid:0x8:beef} vb:0");
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey beef 0 _default.meat";
    EXPECT_EQ(cb::engine_errc::would_block,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
    // as we got cb::engine_errc::would_block we need to manually call the
    // VKeyStatBGFetchTask task
    runNextTask(
            *task_executor->getLpTaskQ()[READER_TASK_IDX],
            "Fetching item from disk for vkey stat: key{cid:0x8:beef} vb:0");
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey beef 0 _default.fruit";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey-byid beef 0 0x9";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey beef2 0 _default._default";
    auto ret = engine->getStats(cookie, key, {}, getKeyStatsResponseHandler);
    if (isFullEviction()) {
        EXPECT_EQ(cb::engine_errc::would_block, ret);
        runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                    "Fetching item from disk for vkey stat: key{cid:0x0:beef2} "
                    "vb:0");
        EXPECT_EQ(
                cb::engine_errc::no_such_key,
                engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    } else {
        EXPECT_EQ(cb::engine_errc::no_such_key, ret);
    }

    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
}

class CollectionsRbacTest : public CollectionsTest {
public:
    void SetUp() override {
        CollectionsTest::SetUp();
        MockCookie::setCheckPrivilegeFunction({});
        mock_set_privilege_context_revision(0);
    }
    std::set<CollectionID> noAccessCids;

    MockCookie::CheckPrivilegeFunction checkPriv =
            [this](const CookieIface&,
                   cb::rbac::Privilege priv,
                   std::optional<ScopeID> sid,
                   std::optional<CollectionID> cid)
            -> cb::rbac::PrivilegeAccess {
        if (cid && noAccessCids.find(*cid) != noAccessCids.end()) {
            return cb::rbac::PrivilegeAccessFailNoPrivileges;
        }
        return cb::rbac::PrivilegeAccessOk;
    };

    void setNoAccess(CollectionID noaccess) {
        noAccessCids.insert(noaccess);
        MockCookie::setCheckPrivilegeFunction(checkPriv);
        mock_set_privilege_context_revision(
                mock_get_privilege_context_revision() + 1);
    }

    void TearDown() override {
        MockCookie::setCheckPrivilegeFunction({});
        mock_set_privilege_context_revision(0);
        CollectionsTest::TearDown();
    }
};

TEST_F(CollectionsRbacTest, GetAllKeysRbacCollectionConnection) {
    // Enable collections for this mock connection and remove
    // privs to access the default and dairy collections
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
    setNoAccess(CollectionEntry::dairy);
    setNoAccess(CollectionEntry::defaultC);
    auto vb = store->getVBucket(vbid);

    // Add the meat and dairy collections
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::dairy);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Create and flush items for the default and meat collections
    ASSERT_TRUE(store_items(10,
                            vbid,
                            makeStoredDocKey("beef", CollectionEntry::meat),
                            "value"));
    flushVBucketToDiskIfPersistent(vbid, 10);

    ASSERT_TRUE(store_items(5, vbid, makeStoredDocKey("default"), "value"));
    flushVBucketToDiskIfPersistent(vbid, 5);

    // Try and access all keys from the default collection, in this case we
    // should be denied access
    std::string startKey =
            makeCollectionEncodedString("default0", CollectionEntry::defaultC);

    EXPECT_EQ(cb::engine_errc::unknown_collection,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));
    EXPECT_EQ(std::set<std::string>(), lastGetKeysResult);

    // Get the keys for meat collection, in this case we should get all 10 keys
    startKey = makeCollectionEncodedString("beef0", CollectionEntry::meat);
    EXPECT_EQ(cb::engine_errc::would_block,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));
    // as we got cb::engine_errc::would_block we need to manually call the
    // GetKeys task
    runNextTask(*task_executor->getLpTaskQ()[READER_TASK_IDX],
                "Running the ALL_DOCS api on vb:0");
    EXPECT_EQ(generateExpectedKeys("beef", 10, CollectionEntry::meat),
              lastGetKeysResult);
    // Calling GetKeys again should return cb::engine_errc::success indicating
    // all keys have been fetched
    EXPECT_EQ(cb::engine_errc::success,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));

    // Add an item to the dairy so we would get a key if RBAC failed
    store_item(
            vbid, makeStoredDocKey("cheese", CollectionEntry::dairy), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    // Try and access all keys from the dairy collection, in this case we
    // should be denied access
    startKey = makeCollectionEncodedString("cheese0", CollectionEntry::dairy);
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              sendGetKeys(startKey, {}, getAllKeysResponseHandler));
    EXPECT_EQ(std::set<std::string>(), lastGetKeysResult);
}

TEST_F(CollectionsRbacTest, TestKeyStats) {
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid,
               makeStoredDocKey("defKey", CollectionEntry::defaultC),
               "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, makeStoredDocKey("beef", CollectionEntry::meat), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    setNoAccess(CollectionEntry::defaultC);

    wasKeyStatsResponseHandlerCalled = false;
    std::string key("key-byid beef 0 0x8");
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key beef 0 _default.meat";
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key-byid defKey 0 0x0";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key defKey 0 _default._default";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "key defKey 0 rubbish_scope._default";
    EXPECT_EQ(cb::engine_errc::unknown_scope,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
}

TEST_F(CollectionsRbacTest, TestVKeyStats) {
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid,
               makeStoredDocKey("defKey", CollectionEntry::defaultC),
               "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, makeStoredDocKey("beef", CollectionEntry::meat), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    setNoAccess(CollectionEntry::defaultC);

    wasKeyStatsResponseHandlerCalled = false;
    std::string key("vkey-byid beef 0 0x8");
    EXPECT_EQ(cb::engine_errc::would_block,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    // as we got cb::engine_errc::would_block we need to manually call the
    // VKeyStatBGFetchTask task
    runNextTask(
            *task_executor->getLpTaskQ()[READER_TASK_IDX],
            "Fetching item from disk for vkey stat: key{cid:0x8:beef} vb:0");
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey beef 0 _default.meat";
    EXPECT_EQ(cb::engine_errc::would_block,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    // as we got cb::engine_errc::would_block we need to manually call the
    // VKeyStatBGFetchTask task
    runNextTask(
            *task_executor->getLpTaskQ()[READER_TASK_IDX],
            "Fetching item from disk for vkey stat: key{cid:0x8:beef} vb:0");
    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_TRUE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey-byid defKey 0 0x0";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey defKey 0 _default._default";
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);

    wasKeyStatsResponseHandlerCalled = false;
    key = "vkey defKey 0 rubbish_scope._default";
    EXPECT_EQ(cb::engine_errc::unknown_scope,
              engine->getStats(cookie, key, {}, getKeyStatsResponseHandler));
    EXPECT_FALSE(wasKeyStatsResponseHandlerCalled);
}

TEST_P(CollectionsPersistentParameterizedTest, SystemEventsDoNotCount) {
    // Run through some manifest changes and warmup a few times.
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::fruit).add(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 3);

    // Now get the engine warmed up
    resetEngineAndWarmup();
    { EXPECT_EQ(0, store->getVBucket(vbid)->getNumTotalItems()); }
    cm.remove(CollectionEntry::meat);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    resetEngineAndWarmup();
    { EXPECT_EQ(0, store->getVBucket(vbid)->getNumTotalItems()); }

    cm.remove(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    resetEngineAndWarmup();
    { EXPECT_EQ(0, store->getVBucket(vbid)->getNumTotalItems()); }

    cm.remove(CollectionEntry::defaultC);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    resetEngineAndWarmup();
    { EXPECT_EQ(0, store->getVBucket(vbid)->getNumTotalItems()); }
}

TEST_P(CollectionsParameterizedTest, ScopeIDIsValid) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    cm.add(ScopeEntry::shop1);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto& manager = getCollectionsManager();

    auto result = manager.isScopeIDValid(ScopeEntry::defaultS.getId());
    EXPECT_EQ(cb::engine_errc::success, result.result);

    result = manager.isScopeIDValid(ScopeEntry::shop1.getId());
    EXPECT_EQ(cb::engine_errc::success, result.result);

    result = manager.isScopeIDValid(ScopeEntry::shop2.getId());
    EXPECT_EQ(cb::engine_errc::unknown_scope, result.result);
}

static void append_stat(std::string_view key,
                        std::string_view value,
                        const void* ctx) {
}

TEST_P(CollectionsParameterizedTest, OneScopeStatsByIdParsing) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    cm.add(ScopeEntry::shop1);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto& manager = getCollectionsManager();
    auto kv = engine->getKVBucket();
    CBStatCollector cbcollector(append_stat, cookie);
    auto collector = cbcollector.forBucket("bucket-name");
    auto result = manager.doScopeStats(*kv, collector, "scopes-byid 0x0");
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(ScopeEntry::defaultS.getId(), result.getScopeId());
    EXPECT_EQ(cm.getUid(), result.getManifestId());

    result = manager.doScopeStats(*kv, collector, "scopes-byid 0x8");
    EXPECT_EQ(cb::engine_errc::success, result.result);
    EXPECT_EQ(ScopeEntry::shop1.getId(), result.getScopeId());
    EXPECT_EQ(cm.getUid(), result.getManifestId());

    result = manager.doScopeStats(*kv, collector, "scopes-byid 0x1");
    EXPECT_EQ(cb::engine_errc::invalid_arguments, result.result);

    result = manager.doScopeStats(*kv, collector, "scopes-byid 0x9");
    EXPECT_EQ(cb::engine_errc::unknown_scope, result.result);
    EXPECT_EQ(cm.getUid(), result.getManifestId());
}

// Test a specific issue spotted, in the case when a collection exists
// as dropped in storage and is newly dropped - we clean-up
TEST_P(CollectionsPersistentParameterizedTest, FlushDropCreateDropCleansUp) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit); // seq:1
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);
    cm.remove(CollectionEntry::fruit); // seq:2
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);
    cm.add(CollectionEntry::fruit); // seq:3
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);
    cm.remove(CollectionEntry::fruit); // seq:4
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    VBucketPtr vb = store->getVBucket(vbid);

    // Final flush should of removed making this invalid and we get an exception
    EXPECT_THROW(vb->getManifest().lock().getStatsForFlush(
                         CollectionEntry::fruit, 4),
                 std::logic_error);
}

/**
 * Test that a new non-prepare namespace doc added during the replay phase of
 * couchstore concurrent compaction updates the collection size.
 */
TEST_P(CollectionsCouchstoreParameterizedTest, ConcCompactReplayNewNonPrepare) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool runOnce = false;
    kvstore.setConcurrentCompactionPreLockHook([&runOnce, &vb, &cm, this](
                                                       auto& compactionKey) {
        if (runOnce) {
            return;
        }
        runOnce = true;

        StoredDocKey meatKey{"beef", CollectionEntry::meat};
        auto meatDoc = makeCommittedItem(meatKey, "value");
        EXPECT_EQ(cb::engine_errc::success, store->set(*meatDoc, cookie));

        auto d = DiskChecker(vb, CollectionEntry::meat, std::greater<>());
        flushVBucketToDiskIfPersistent(vbid, 1);
    });

    auto d = DiskChecker(vb, CollectionEntry::meat, std::greater<>());
    runCompaction(vbid, 0, false);
}

/**
 * Test that a change to a non-prepare namespace doc added during the replay
 * phase of couchstore concurrent compaction updates the collection size.
 */
TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactReplayChangeNonPrepare) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    StoredDocKey meatKey{"beef", CollectionEntry::meat};
    auto meatDoc = makeCommittedItem(meatKey, "BigValueToTestSizeChange");
    EXPECT_EQ(cb::engine_errc::success, store->set(*meatDoc, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool runOnce = false;
    kvstore.setConcurrentCompactionPreLockHook(
            [&runOnce, &vb, &cm, &meatKey, this](auto& compactionKey) {
                if (runOnce) {
                    return;
                }
                runOnce = true;

                auto meatDoc = makeCommittedItem(meatKey, "value");
                EXPECT_EQ(cb::engine_errc::success,
                          store->set(*meatDoc, cookie));

                auto d = DiskChecker(vb, CollectionEntry::meat, std::less<>());
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    auto d = DiskChecker(vb, CollectionEntry::meat, std::less<>());
    runCompaction(vbid, 0, false);
}

/**
 * Test that the delete of non-prepare namespace doc added during the repla
 * phase of couchstore concurrent compaction updates the collection size.
 */
TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactReplayDeleteNonPrepare) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    StoredDocKey meatKey{"beef", CollectionEntry::meat};
    auto meatDoc = makeCommittedItem(meatKey, "BigValueToTestSizeChange");
    EXPECT_EQ(cb::engine_errc::success, store->set(*meatDoc, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool runOnce = false;
    kvstore.setConcurrentCompactionPreLockHook(
            [&runOnce, &vb, &cm, &meatKey, this](auto& compactionKey) {
                if (runOnce) {
                    return;
                }
                runOnce = true;

                uint64_t cas = 0;
                mutation_descr_t mutation_descr;
                EXPECT_EQ(cb::engine_errc::success,
                          store->deleteItem(meatKey,
                                            cas,
                                            vbid,
                                            cookie,
                                            {},
                                            nullptr,
                                            mutation_descr));

                auto d = DiskChecker(vb, CollectionEntry::meat, std::less<>());
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    auto d = DiskChecker(vb, CollectionEntry::meat, std::less<>());
    runCompaction(vbid, 0, false);
}

/**
 * Test that the re-addition of a non-prepare namespace doc added during the
 * replay phase of couchstore concurrent compaction updates the collection size.
 */
TEST_P(CollectionsCouchstoreParameterizedTest,
       ConcCompactReplayUnDeleteNonPrepare) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    StoredDocKey meatKey{"beef", CollectionEntry::meat};

    auto meatDoc = makeCommittedItem(meatKey, "BigValueToTestSizeChange");
    EXPECT_EQ(cb::engine_errc::success, store->set(*meatDoc, cookie));

    flushVBucketToDiskIfPersistent(vbid, 1);

    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    EXPECT_EQ(cb::engine_errc::success,
              store->deleteItem(
                      meatKey, cas, vbid, cookie, {}, nullptr, mutation_descr));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool runOnce = false;
    kvstore.setConcurrentCompactionPreLockHook(
            [&runOnce, &vb, &cm, &meatKey, this](auto& compactionKey) {
                if (runOnce) {
                    return;
                }
                runOnce = true;

                auto meatDoc =
                        makeCommittedItem(meatKey, "BigValueToTestSizeChange");
                EXPECT_EQ(cb::engine_errc::success,
                          store->set(*meatDoc, cookie));

                auto d = DiskChecker(
                        vb, CollectionEntry::meat, std::greater<>());
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    auto d = DiskChecker(vb, CollectionEntry::meat, std::greater<>());
    runCompaction(vbid, 0, false);
}

/**
 * Test that a delete that is copied over a delete in a replay results in the
 * correct disk size
 */
TEST_P(CollectionsCouchstoreParameterizedTest, ConcCompactReplayDeleteDelete) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);

    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    StoredDocKey key{"beef", CollectionEntry::meat};
    store_item(vbid, key, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    store_deleted_item(vbid, key, "value++++");
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& kvstore =
            dynamic_cast<MockCouchKVStore&>(*store->getRWUnderlying(vbid));

    bool runOnce = false;
    kvstore.setConcurrentCompactionPreLockHook(
            [&runOnce, &vb, &key, this](auto& compactionKey) {
                if (runOnce) {
                    return;
                }
                runOnce = true;
                // set->delete and flush again
                store_item(vbid, key, "value");
                store_deleted_item(vbid, key, "value++++");
                flushVBucketToDiskIfPersistent(vbid, 1);
            });

    auto d = DiskChecker(vb, CollectionEntry::meat, std::equal_to<>());
    runCompaction(vbid, 0, false);
    auto handle = vb->getManifest().lock(CollectionEntry::meat);
    EXPECT_EQ(2, handle.getOpsStore());
    EXPECT_EQ(2, handle.getOpsDelete());
}

TEST_P(CollectionsEphemeralParameterizedTest, TrackSystemEventSize) {
    auto vb = store->getVBucket(vbid);

    // Add the meat collection
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest(makeManifest(cm));
    // No system event for default
    EXPECT_EQ(0, getCollectionMemUsed(vb, CollectionID::Default));
    EXPECT_NE(0, getCollectionMemUsed(vb, CollectionEntry::meat.getId()));
}

TEST_P(CollectionsPersistentParameterizedTest, TombstonePurge) {
    auto vb = store->getVBucket(vbid);
    // add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    auto compareDiskStatMemoryVsPersisted = [vb]() {
        auto kvs = vb->getShard()->getRWUnderlying();
        auto fruitSz =
                vb->getManifest().lock(CollectionEntry::fruit).getDiskSize();
        auto dairySz =
                vb->getManifest().lock(CollectionEntry::dairy).getDiskSize();
        auto stats =
                kvs->getCollectionStats(vb->getId(), CollectionEntry::fruit);
        EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, stats.first);
        EXPECT_EQ(stats.second.diskSize, fruitSz);
        stats = kvs->getCollectionStats(vb->getId(), CollectionEntry::dairy);
        EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, stats.first);

        EXPECT_EQ(stats.second.diskSize, dairySz);
    };

    flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x system */);
    const auto& manifest = vb->getManifest();
    auto c1_d1 = manifest.lock(CollectionEntry::fruit).getDiskSize();
    auto c2_d1 = manifest.lock(CollectionEntry::dairy).getDiskSize();
    EXPECT_GT(c1_d1, 0);
    EXPECT_GT(c2_d1, 0);
    compareDiskStatMemoryVsPersisted();

    // add some items
    store_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy}, "nice");
    store_item(vbid, StoredDocKey{"butter", CollectionEntry::dairy}, "lovely");
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "nice");
    store_item(vbid, StoredDocKey{"apricot", CollectionEntry::fruit}, "lovely");

    flushVBucketToDiskIfPersistent(vbid, 4);
    auto c1_d2 = manifest.lock(CollectionEntry::fruit).getDiskSize();
    auto c2_d2 = manifest.lock(CollectionEntry::dairy).getDiskSize();
    EXPECT_GT(c1_d2, c1_d1);
    EXPECT_GT(c2_d2, c2_d1);
    compareDiskStatMemoryVsPersisted();

    // MB-45132: delete the item twice so we can detect that the key/meta is
    // accounted
    store_deleted_item(
            vbid, StoredDocKey{"apple", CollectionEntry::fruit}, ".");
    store_deleted_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy}, ".");

    flushVBucketToDiskIfPersistent(vbid, 2);
    auto c1_d3 = manifest.lock(CollectionEntry::fruit).getDiskSize();
    auto c2_d3 = manifest.lock(CollectionEntry::dairy).getDiskSize();
    EXPECT_LT(c1_d3, c1_d2);
    EXPECT_LT(c2_d3, c2_d2);
    compareDiskStatMemoryVsPersisted();

    // Must store so that the delete can be success, but we don't flush the
    // store so KVStore only sees two deletes.
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "nice");
    store_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy}, "nice");
    delete_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit});
    delete_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy});
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto c1_d4 = manifest.lock(CollectionEntry::fruit).getDiskSize();
    auto c2_d4 = manifest.lock(CollectionEntry::dairy).getDiskSize();
    if (isMagma()) {
        EXPECT_EQ(c1_d4, c1_d3);
        EXPECT_EQ(c2_d4, c2_d3);
    } else {
        EXPECT_LT(c1_d4, c1_d3);
        EXPECT_LT(c2_d4, c2_d3);
    }
    compareDiskStatMemoryVsPersisted();

    // Now purge those tombstones
    runCompaction(vbid, 0, true);

    auto c1_d5 = manifest.lock(CollectionEntry::fruit).getDiskSize();
    auto c2_d5 = manifest.lock(CollectionEntry::dairy).getDiskSize();
    if (isMagma() || isPitrEnabled()) {
        // Magma decrements the collection disk size when we delete items rather
        // than when we purge the tombstones so no change is expected
        EXPECT_EQ(c1_d5, c1_d4);
    } else {
        EXPECT_LT(c1_d5, c1_d4);
    }

    // dairy is equal because we haven't purged the tombstone (it's the high
    // seqno which remains)
    EXPECT_EQ(c2_d5, c2_d4);
    compareDiskStatMemoryVsPersisted();
}

// Collection disk size tracking for delete -> delete is incorrect
// The second delete became an insert
TEST_P(CollectionsPersistentParameterizedTest, DeleteDelete) {
    auto vb = store->getVBucket(vbid);
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    const auto& manifest = vb->getManifest();
    auto c1_diskSize1 = manifest.lock(CollectionEntry::fruit).getDiskSize();
    EXPECT_GT(c1_diskSize1, 0);

    // add some items
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "nice");
    store_item(vbid, StoredDocKey{"apricot", CollectionEntry::fruit}, "lovely");
    flushVBucketToDiskIfPersistent(vbid, 2);
    auto c1_diskSize2 = manifest.lock(CollectionEntry::fruit).getDiskSize();
    // Increased
    EXPECT_GT(c1_diskSize2, c1_diskSize1);

    // Now delete a key, which really is an update in terms of disk, here we
    // deliberatley delete with a bigger value
    store_deleted_item(
            vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "nice+++");
    flushVBucketToDiskIfPersistent(vbid, 1);
    auto c1_diskSize3 = manifest.lock(CollectionEntry::fruit).getDiskSize();

    if (isMagma()) {
        // Magma decrements the entire document from the collection size at
        // deletion
        EXPECT_LT(c1_diskSize3, c1_diskSize2);
    } else {
        EXPECT_GT(c1_diskSize3, c1_diskSize2);
    }

    // Now delete again (first we store, but don't flush the store)
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "nice+++");
    store_deleted_item(
            vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "nice+++");
    flushVBucketToDiskIfPersistent(vbid, 1);
    auto c1_diskSize4 = manifest.lock(CollectionEntry::fruit).getDiskSize();

    // We've just replaced a delete with an identical delete. Before fixing
    // MB-45221 the disk increased as the new delete was treated as an insert
    EXPECT_EQ(c1_diskSize4, c1_diskSize3);
}

// MB-45899 occured as accumulateStats didn't check the result of a map.find
// which allows for a crash to occur if collection state is changing (collection
// dropped) whilst stats are gathered.
TEST_P(CollectionsParameterizedTest, MB_45899) {
    const auto vbid0 = vbid;
    const auto vbid1 = Vbid(vbid.get() + 1);
    setVBucketStateAndRunPersistTask(vbid1, vbucket_state_active);

    // Put one item in vb1 so we can detect that stats don't change at the end
    // of the test.
    store_item(vbid1, StoredDocKey{"key", CollectionID::Default}, "value");
    flushVBucketToDiskIfPersistent(vbid1, 1);

    // Remove the default collection from the manifest
    CollectionsManifest cm;
    cm.remove(CollectionEntry::defaultC);

    // Now poke accumulateStats, which requires a vector of entries we are
    // interested in.
    std::vector<Collections::CollectionEntry> collections;
    collections.push_back(
            {CollectionID::Default, "_default", {}, ScopeID::Default, true});
    Collections::Summary summary;
    auto vb0 = store->getVBucket(vbid0);
    auto vb1 = store->getVBucket(vbid1);

    // The following order of calls to accumulateStats and updateFromManifest
    // would previously expose an access violation via an invalid iterator
    EXPECT_EQ(0, summary.count(CollectionID::Default));
    vb0->lockCollections().accumulateStats(collections, summary);
    EXPECT_EQ(1, summary.count(CollectionID::Default));
    auto copyStats = summary.find(CollectionID::Default)->second;
    vb0->updateFromManifest(makeManifest(cm));
    vb1->updateFromManifest(makeManifest(cm));
    vb1->lockCollections().accumulateStats(collections, summary);

    // No changes made, collection doesn't exist in vb1
    EXPECT_EQ(copyStats, summary.find(CollectionID::Default)->second);
}

#ifdef EP_USE_MAGMA
/**
 * Test that we maintain the dropped collections meta correctly if we drop a
 * collection during a purge.
 *
 * Test only runs for magma as the completion callback is called under the
 * vBucket lock for couchstore (and would deadlock the test)
 */
TEST_P(CollectionsMagmaParameterizedTest, DropDuringPurge) {
    CollectionsManifest cm;
    auto vb = store->getVBucket(vbid);

    cm.add(CollectionEntry::fruit);
    cm.add(CollectionEntry::meat);

    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "value");
    store_item(vbid, StoredDocKey{"beef", CollectionEntry::meat}, "value");
    flushVBucketToDiskIfPersistent(vbid, 2);

    cm.remove(CollectionEntry::fruit);
    vb->updateFromManifest(makeManifest(cm));

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto& epBucket = dynamic_cast<MockEPBucket&>(*store);
    epBucket.mockMakeCompactionContext =
            [this, &cm, &epBucket, &vb](
                    std::shared_ptr<CompactionContext> ctx) {
                ctx->completionCallback = [this, &cm, &epBucket, &vb](
                                                  CompactionContext& ctx) {
                    // Fruit should be there
                    auto dropped =
                            store->getRWUnderlying(vbid)->getDroppedCollections(
                                    vbid);
                    EXPECT_TRUE(dropped.first);
                    EXPECT_EQ(1, dropped.second.size());

                    // Drop a collection and flush so that the dropped
                    // collections local doc gets updated
                    cm.remove(CollectionEntry::meat);
                    vb->updateFromManifest(makeManifest(cm));
                    flushVBucketToDiskIfPersistent(vbid, 1);

                    // Meat should be now too
                    dropped =
                            store->getRWUnderlying(vbid)->getDroppedCollections(
                                    vbid);
                    EXPECT_TRUE(dropped.first);
                    EXPECT_EQ(2, dropped.second.size());

                    // Now, hit the actual completion callback to set item
                    // counts
                    epBucket.publicCompactionCompletionCallback(ctx);
                };
                return ctx;
            };

    // Not using runCollectionsEraser(vbid) here as that asserts that dropped
    // collections is empty at the end (and it won't be in this test). Running
    // the compaction manually.
    store->scheduleCompaction(vbid, {}, nullptr, std::chrono::seconds(0));
    std::string task = "Compact DB file " + std::to_string(vbid.get());
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX], task);

    // Meat should still be there - before the fix dropped size would be 0
    auto dropped = store->getRWUnderlying(vbid)->getDroppedCollections(vbid);
    EXPECT_TRUE(dropped.first);
    EXPECT_EQ(1, dropped.second.size());

    EXPECT_EQ(1, vb->getNumTotalItems());

    epBucket.mockMakeCompactionContext =
            [](std::shared_ptr<CompactionContext> ctx) {
                // Use the default context from now on
                return ctx;
            };
    runCollectionsEraser(vbid);

    dropped = store->getRWUnderlying(vbid)->getDroppedCollections(vbid);
    EXPECT_TRUE(dropped.first);
    EXPECT_EQ(0, dropped.second.size());

    EXPECT_EQ(0, vb->getNumTotalItems());
}
#endif

// MB-48398 identified that we may warmup and push collection changes onto an
// active vbucket, these should be done against a new UUID to protect against
// the vbucket becoming a replica and joining to another active with the wrong
// state.
TEST_P(CollectionsPersistentParameterizedTest, WarmupWithANewUUID_MB_48398) {
    // VB must be flushed at least once as active
    store_item(vbid, StoredDocKey{"k1", CollectionID::Default}, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Set the manifest with the fruit collection, but do not flush!
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm); // seq 2
    store_item(vbid, StoredDocKey{"k1", CollectionEntry::fruit}, "v1");
    auto uuid = store->getVBucket(vbid)->failovers->getLatestUUID();

    // Forget all the collection change - warmup
    resetEngineAndWarmup();

    // VB is back to active and has a new UUID and warmup updated the collection
    // state
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(vbucket_state_active, vb->getState());
    EXPECT_NE(uuid, vb->failovers->getLatestUUID());
    EXPECT_EQ(cm.getUid(), vb->lockCollections().getManifestUid());
}

TEST_P(CollectionsParameterizedTest, WriteToScopeWithLimit) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1, 0); // 0 data limit
    cm.add(CollectionEntry::fruit, ScopeEntry::shop1);
    cm.add(ScopeEntry::shop2, 10000); // 10000  data limit
    cm.add(CollectionEntry::vegetable, ScopeEntry::shop2);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));

    // Cat write to vegetable
    store_item(vbid, StoredDocKey{"k1", CollectionEntry::vegetable}, "v1");

    // Cannot write to fruit
    auto item = make_item(
            vbid, StoredDocKey{"k1", CollectionEntry::fruit}, "value");
    EXPECT_EQ(cb::engine_errc::scope_size_limit_exceeded,
              store->set(item, cookie));
    EXPECT_EQ(cb::engine_errc::scope_size_limit_exceeded,
              store->add(item, cookie));
    EXPECT_EQ(cb::engine_errc::scope_size_limit_exceeded,
              store->replace(item, cookie));
    item.setCas(1);
    EXPECT_EQ(cb::engine_errc::scope_size_limit_exceeded,
              store->setWithMeta(item,
                                 1,
                                 nullptr,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 false,
                                 GenerateBySeqno::Yes,
                                 GenerateCas::No));
}

TEST_P(CollectionsParameterizedTest, PerCollectionMemUsedAndDeleteVbucket) {
    // Add a second vbucket
    const auto vbid0 = vbid;
    const auto vbid1 = Vbid(vbid.get() + 1);
    setVBucketStateAndRunPersistTask(vbid1, vbucket_state_active);
    const size_t htSize = 2;
    auto vb0 = store->getVBucket(vbid0);
    vb0->ht.resize(htSize);
    ASSERT_EQ(htSize, vb0->ht.getSize());
    auto vb1 = store->getVBucket(vbid1);
    vb1->ht.resize(htSize);
    ASSERT_EQ(htSize, vb1->ht.getSize());

    // Need the keys to chain to exercise the loops inside HashTable
    // These two keys will collide and chain
    auto k1 = StoredDocKey{"1", CollectionEntry::defaultC};
    auto k2 = StoredDocKey{"999", CollectionEntry::defaultC};
    // Plus one extra key which doesn't collide - means we exercise both loops
    // in the HashTable::clear_UNLOCKED code being tested
    auto k3 = StoredDocKey{"2", CollectionEntry::defaultC};

    auto getHashTableBucketNum = [&vb0](const DocKey& k) {
        return vb0->ht.getLockedBucket(k).getBucketNum();
    };

    EXPECT_EQ(getHashTableBucketNum(k1), getHashTableBucketNum(k2));
    EXPECT_NE(getHashTableBucketNum(k3), getHashTableBucketNum(k2));

    auto addItems = [this, &k1, &k2, &k3](VBucketPtr& vb, Vbid id) {
        // default collection memory usage should _increase_
        auto d = MemChecker(vb, CollectionEntry::defaultC, std::greater<>());

        store_item(id, k1, "v1");
        store_item(id, k2, "v1");
        store_item(id, k3, "v1");

        flushVBucketToDiskIfPersistent(id, 3);
    };

    SCOPED_TRACE("new item added to collection vb:0");
    addItems(vb0, vbid0);
    auto vb0size =
            engine->getEpStats().getCollectionMemUsed(CollectionID::Default);
    SCOPED_TRACE("new item added to collection vb:1");
    addItems(vb1, vbid1);

    vb1.reset(); // deleting this one next
    {
        SCOPED_TRACE("Delete vb:1");
        auto d = MemChecker(vb0, CollectionEntry::defaultC, std::less<>());
        EXPECT_EQ(cb::engine_errc::success, store->deleteVBucket(vbid1));
        // Run the deletion task
        if (ephemeral()) {
            runNextTask(*task_executor->getLpTaskQ()[NONIO_TASK_IDX]);
        } else {
            runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX]);
        }
    }

    EXPECT_EQ(vb0size,
              engine->getEpStats().getCollectionMemUsed(CollectionID::Default));
}

class CollectionsPersistentNoNexusParameterizedTest
    : public CollectionsParameterizedTest {};

// MB-50519: Test didn't replicate a negative item count like in the MB (other
// tests do that), but this test is useful to check that the item counts resolve
// even after warmup interrupts the create/drop/compact steps
// NoNexus as KVstore::getItemCount differs couchstore vs magma
TEST_P(CollectionsPersistentNoNexusParameterizedTest,
       update_into_new_generation) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(0, vb->getNumTotalItems());

    auto k1 = StoredDocKey{"k", CollectionEntry::fruit};
    ASSERT_TRUE(store_items(2, vbid, k1, "v1")); // store k0, k1
    flushVBucketToDiskIfPersistent(vbid, 2);
    EXPECT_EQ(2, vb->getNumTotalItems());

    // Now drop the collection (don't erase)
    cm.remove(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);
    // k1:v1 still counted until compaction driven erase
    EXPECT_EQ(2, vb->getNumTotalItems());

    // Add fruit back and "insert" k1, k1 still exists as a key but is logically
    // deleted.
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // k1:v1 is already counted in the items, this update should not affect
    // the bucket item count - however for the collection it is an insert
    ASSERT_TRUE(store_items(2, vbid, k1, "v2"));

    flushVBucketToDiskIfPersistent(vbid, 2);

    // magma counting old/new generations until compaction
    size_t expectedItems = isMagma() ? 4 : 2;
    EXPECT_EQ(expectedItems, vb->getNumTotalItems());
    auto rws = store->getRWUnderlying(vbid);
    ASSERT_TRUE(rws);
    // The disk item count always includes the system event
    // If we warmup we will subtract the system event count (1) from this number
    // to obtain the "total items"
    EXPECT_EQ(expectedItems + 1, rws->getItemCount(vbid));

    vb.reset();
    resetEngineAndWarmup();
    vb = store->getVBucket(vbid);
    rws = store->getRWUnderlying(vbid);
    ASSERT_TRUE(rws);
    EXPECT_EQ(expectedItems, vb->getNumTotalItems());
    EXPECT_EQ(expectedItems + 1, rws->getItemCount(vbid));

    // Now purge the old fruit
    runCompaction(vbid);
    EXPECT_EQ(2, vb->getNumTotalItems()); // magma/couchstore aligned

    // Both fruit system event + k0/k1 exist
    EXPECT_EQ(3, rws->getItemCount(vbid));
    vb.reset();

    resetEngineAndWarmup();

    vb = store->getVBucket(vbid);
    rws = store->getRWUnderlying(vbid);
    ASSERT_TRUE(rws);
    EXPECT_EQ(2, vb->getNumTotalItems()); // magma/couchstore aligned

    // Both fruit system event + k0/k1 exist
    EXPECT_EQ(3, rws->getItemCount(vbid));
}

INSTANTIATE_TEST_SUITE_P(CollectionsExpiryLimitTests,
                         CollectionsExpiryLimitTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(CollectionsEphemeralOrPersistent,
                         CollectionsParameterizedTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsEphemeralOrPersistentPitrEnabled,
                         CollectionsParameterizedTest,
                         STParameterizedBucketTest::pitrEnabledConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsEphemeralOnlyTests,
                         CollectionsEphemeralParameterizedTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsPersistent,
                         CollectionsPersistentParameterizedTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsPersistentPitrEnabled,
                         CollectionsPersistentParameterizedTest,
                         STParameterizedBucketTest::pitrEnabledConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsCouchstore,
                         CollectionsCouchstoreParameterizedTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#ifdef EP_USE_MAGMA
INSTANTIATE_TEST_SUITE_P(CollectionsMagma,
                         CollectionsMagmaParameterizedTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
#endif

INSTANTIATE_TEST_SUITE_P(
        CollectionsPersistentNoNexus,
        CollectionsPersistentNoNexusParameterizedTest,
        STParameterizedBucketTest::persistentNoNexusConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);
