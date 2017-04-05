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
 * Unit tests for the KVBucket class.
 *
 * These tests are instantiated with additional config strings to test over
 * ephemeral and value and full eviction persistent buckets.
 *
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

    /**
     * Store multiple items into the vbucket, the given key will have an
     * iteration appended to it.
     */
    ::testing::AssertionResult store_items(
            int nitems,
            uint16_t vbid,
            const DocKey& key,
            const std::string& value,
            uint32_t exptime = 0,
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

    /**
     * Create a *_with_meta packet with the key/body
     * Allows *_with_meta to be invoked via EventuallyPersistentEngine which
     * begins with a packet
     */
    static std::vector<char> buildWithMetaPacket(
            protocol_binary_command opcode,
            protocol_binary_datatype_t datatype,
            uint16_t vbucket,
            uint32_t opaque,
            uint64_t cas,
            ItemMetaData metaData,
            const std::string& key,
            const std::string& body,
            const std::vector<char>& emd = {},
            int options = 0);

    static bool addResponse(const void* k,
                            uint16_t keylen,
                            const void* ext,
                            uint8_t extlen,
                            const void* body,
                            uint32_t bodylen,
                            uint8_t datatype,
                            uint16_t status,
                            uint64_t pcas,
                            const void* cookie);

    /**
     * Create an XATTR document using the supplied string as the body
     * @returns string containing the new value
     */
    static std::string createXattrValue(const std::string& body);

    static protocol_binary_response_status getAddResponseStatus(
            protocol_binary_response_status newval =
                    PROTOCOL_BINARY_RESPONSE_SUCCESS);

    static protocol_binary_response_status addResponseStatus;

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
 * Test fixture for KVBucket unit tests.
 *
 * These tests are parameterized over an extra config string to allow them to
 * be run against ephemeral and value and full eviction persistent buckets.
 */
class KVBucketParamTest : public KVBucketTest,
                          public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        config_string += GetParam();
        KVBucketTest::SetUp();

        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};
