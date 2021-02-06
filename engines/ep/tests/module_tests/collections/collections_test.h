/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#pragma once

#include "collections/kvstore.h"
#include "kv_bucket.h"
#include "kvstore.h"
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
        return store->getVBucket(vb)
                ->getShard()
                ->getRWUnderlying()
                ->getCollectionsManifest(vbid);
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
    : public CollectionsParameterizedTest {};
