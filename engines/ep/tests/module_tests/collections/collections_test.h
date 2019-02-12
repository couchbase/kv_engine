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
                ->getCollectionsManifest_new(vbid);
    }
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
