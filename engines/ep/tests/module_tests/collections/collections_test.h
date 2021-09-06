/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/kvstore.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"

/**
 * Simple Collections test class. Non-parameterized so that we can run
 * parameterized tests more simply where required.
 */
class CollectionsTest : virtual public SingleThreadedKVBucketTest {
public:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        CollectionsTest::internalSetUp();
    }

protected:
    // Do the specific setup for these tests
    void internalSetUp() {
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
    }

    Collections::KVStore::Manifest getManifest(Vbid vb) const {
        auto [status, persistedManifest] =
                store->getVBucket(vb)
                        ->getShard()
                        ->getRWUnderlying()
                        ->getCollectionsManifest(vbid);
        EXPECT_TRUE(status);
        return persistedManifest;
    }

    cb::engine_errc sendGetKeys(std::string startKey,
                                std::optional<uint32_t> maxCount,
                                const AddResponseFn& response);

    std::set<std::string> generateExpectedKeys(
            std::string_view keyPrefix,
            size_t numOfItems,
            CollectionID cid = CollectionID(0));
};

/**
 * Test for Collection functionality in both EventuallyPersistent and
 * Ephemeral buckets.
 */
class CollectionsParameterizedTest : public STParameterizedBucketTest,
                                     public CollectionsTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        CollectionsTest::internalSetUp();
    }
};

/**
 * Test for Collection functionality against persistent buckets types.
 */
class CollectionsPersistentParameterizedTest
    : public CollectionsParameterizedTest {};

class CollectionsCouchstoreParameterizedTest
    : public CollectionsParameterizedTest {
public:
    void ConcCompact(std::function<void()> concurrentFunc);
};

class CollectionsEphemeralParameterizedTest
    : public CollectionsParameterizedTest {};

class CollectionsMagmaParameterizedTest : public CollectionsParameterizedTest {
};