/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Unit tests for the EPBucket class.
 */

#pragma once

#include "config.h"

#include "ep_bucket.h"
#include "ep_engine.h"
#include "item.h"

#include <memcached/engine.h>
#include <tests/mock/mock_synchronous_ep_engine.h>

#include <gtest/gtest.h>
#include <memory>

/**
 * Test fixture for KVBucket unit tests.
 *
 * Will create the appropriate subclass of KVBucket (EPBucket /
 * EphemeralBucket) based on the Configuration passed (specifically the
 * bucket_type parameter), defaulting to EPBucket if no bucket_type is
 * specified.
 */
class KVBucketTest : public ::testing::Test {
public:
    void SetUp() override;

    void TearDown() override;

    // Stores an item into the given vbucket. Returns the item stored.
    Item store_item(uint16_t vbid,
                    const StoredDocKey& key,
                    const std::string& value,
                    uint32_t exptime = 0,
                    const std::vector<cb::engine_errc>& expected =
                            {cb::engine_errc::success},
                    protocol_binary_datatype_t datatype =
                            PROTOCOL_BINARY_DATATYPE_JSON);

    /* Flush the given vbucket to disk, so any outstanding dirty items are
     * written (and are clean).
     */
    void flush_vbucket_to_disk(uint16_t vbid, int expected = 1);

    /* Delete the given item from the given vbucket, verifying it was
     * successfully deleted.
     */
    void delete_item(uint16_t vbid, const StoredDocKey& key);

    /* Evict the given key from memory according to the current eviction
     * strategy. Verifies it was successfully evicted.
     */
    void evict_key(uint16_t vbid, const StoredDocKey& key);

    /// Exposes the normally-protected getInternal method from the store.
    GetValue getInternal(const StoredDocKey& key,
                         uint16_t vbucket,
                         const void* cookie,
                         vbucket_state_t allowedState,
                         get_options_t options);

    /**
     * Creates the ItemPager task and adds it to the scheduler. Allows testing
     * of the item pager from subclasses, without KVBucket having to grant
     * friendship to many different test classes.
     */
    void createAndScheduleItemPager();

    void initializeExpiryPager();

    static const char test_dbname[];

    std::string config_string;

    const uint16_t vbid = 0;

    // The mock engine (needed to construct the store).
    std::unique_ptr<SynchronousEPEngine> engine;

    // The store under test. Wrapped in a mock to expose some normally
    // protected members. Uses a raw pointer as this is owned by the engine.
    KVBucket* store;

    // The (mock) server cookie.
    const void* cookie;
};

/**
 * Test fixture for EPBucket unit tests.
 */
class EPBucketTest : public KVBucketTest {
    // Note this class is currently identical to it's parent class as the
    // default bucket_type in configuration.json is EPBucket, therefore
    // KVBucketTest already defaults to creating EPBucket. Introducing this
    // subclass to just make the name more descriptive.
};
