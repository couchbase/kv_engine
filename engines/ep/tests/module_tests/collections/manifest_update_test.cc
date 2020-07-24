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

/**
 * Tests for Collection functionality in EPStore.
 */

#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "collections_test.h"
#include "warmup.h"

#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <utilities/test_manifest.h>

#include <fstream>

class CollectionsManifestUpdate : public CollectionsParameterizedTest {};

TEST_P(CollectionsManifestUpdate, update_epoch) {
    CollectionsManifest cm;
    setCollections(cookie, std::string{cm});
}

TEST_P(CollectionsManifestUpdate, update_add1) {
    CollectionsManifest cm, cm1;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(22));
    setCollections(cookie, std::string{cm});
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(22));

    // Finally, we cannot setCollections to something which is not a
    // successor (in future this would be allowed but by an explicit force)
    // Here we create a manifest which has an increased uid, but collection 22
    // switched name from fruit to woodwind - very odd and not a successor.
    cm1.add(CollectionEntry::Entry{"woodwind", 22});
    cm1.add(CollectionEntry::Entry{"brass", 23});
    setCollections(cookie,
                   std::string{cm1},
                   cb::engine_errc::cannot_apply_collections_manifest);

    // But now force it
    cm1.setForce(true);
    setCollections(cookie, std::string{cm1});

    // @todo: MB-39292 The force update has changed fruit to woodwind - but the
    // vbucket's only know about collection:22 and have done nothing about it -
    // we now have fruit mixed in with the woodwind.
}

TEST_P(CollectionsManifestUpdate, update_add1_warmup) {
    CollectionsManifest cm, cm1;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(22));
    setCollections(cookie, std::string{cm});
    // Check the current manifest is not a forced update
    EXPECT_FALSE(store->getCollectionsManager()
                         .getCurrentManifest()
                         .rlock()
                         ->isForcedUpdate());

    if (isPersistent()) {
        resetEngineAndWarmup();
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setVBucketState(vbid, vbucket_state_active));

        // Check the current manifest is still not a forced update
        EXPECT_FALSE(store->getCollectionsManager()
                             .getCurrentManifest()
                             .rlock()
                             ->isForcedUpdate());
    }
    // cm1 is default state - uid of 0, cannot go back
    setCollections(cookie,
                   std::string{cm1},
                   cb::engine_errc::cannot_apply_collections_manifest);
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(22));

    // Finally, we cannot setCollections to something which is not a
    // successor (in future this would be allowed but by an explicit force)
    // Here we create a manifest which has an increased uid, but collection 22
    // switched name from fruit to woodwind - very odd and not a successor.
    cm1.add(CollectionEntry::Entry{"woodwind", 22});
    cm1.add(CollectionEntry::Entry{"brass", 23});
    setCollections(cookie,
                   std::string{cm1},
                   cb::engine_errc::cannot_apply_collections_manifest);

    // But now force it
    cm1.setForce(true);
    setCollections(cookie, std::string{cm1});

    if (isPersistent()) {
        resetEngineAndWarmup();
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setVBucketState(vbid, vbucket_state_active));
    }

    // Check the current manifest is still a forced update
    EXPECT_TRUE(store->getCollectionsManager()
                        .getCurrentManifest()
                        .rlock()
                        ->isForcedUpdate());
}

TEST_P(CollectionsManifestUpdate, update_add1_move1_warmup) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    cm.add(ScopeEntry::shop1);
    EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(22));
    setCollections(cookie, std::string{cm});
    // Check the current manifest is not a forced update
    EXPECT_FALSE(store->getCollectionsManager()
                         .getCurrentManifest()
                         .rlock()
                         ->isForcedUpdate());

    if (isPersistent()) {
        resetEngineAndWarmup();
        EXPECT_EQ(ENGINE_SUCCESS,
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
    setCollections(cookie,
                   std::string{cm},
                   cb::engine_errc::cannot_apply_collections_manifest);

    // But now force it
    cm.setForce(true);
    setCollections(cookie, std::string{cm});

    // KV doesn't yet respond to this yet
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
              engine->set_collection_manifest(cookie, std::string{cm}));

    // Remove the datadir, persistence will fail and be detected, command then
    // fails
    cb::io::rmrf(test_dbname);

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    runNextTask(lpAuxioQ);

    auto mockCookie = cookie_to_mock_cookie(cookie);
    EXPECT_EQ(cb::engine_errc::cannot_apply_collections_manifest,
              cb::engine_errc(mockCookie->status));
}

TEST_P(CollectionsManifestUpdatePersistent, update_fail_warmup) {
    CollectionsManifest cm, cm1;
    cm.add(CollectionEntry::Entry{"fruit", 22});
    setCollections(cookie, std::string{cm});
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

INSTANTIATE_TEST_SUITE_P(CollectionsEphemeralOrPersistent,
                         CollectionsManifestUpdate,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsPersistent,
                         CollectionsManifestUpdatePersistent,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);