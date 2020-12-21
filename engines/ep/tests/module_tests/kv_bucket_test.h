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
                    const DocKey& key,
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
    void flush_vbucket_to_disk(uint16_t vbid, size_t expected = 1);

    /**
     * Flush the given vBucket to disk if the bucket is peristent, otherwise
     * do nothing.
     * @param vbid vBucket to flush
     * @param expected Expected number of items to be flushed.
     */
    void flushVBucketToDiskIfPersistent(uint16_t vbid, int expected = 1);

    void removeCheckpoint(VBucket& vb, int numItems);

    void flushAndRemoveCheckpoints(uint16_t vbid);

    /* Delete the given item from the given vbucket, verifying it was
     * successfully deleted.
     */
    void delete_item(uint16_t vbid, const DocKey& key);

    /**
     * Store and delete a given key
     *
     * @param vbid   vbucket id where the key needs to be stored
     * @param key    key that needs to be stored and deleted
     * @param value  value for the key
     */
    void storeAndDeleteItem(uint16_t vbid,
                            const DocKey& key,
                            std::string value);

    /* Evict the given key from memory according to the current eviction
     * strategy. Verifies it was successfully evicted.
     */
    void evict_key(uint16_t vbid, const DocKey& key);

    /// Exposes the normally-protected getInternal method from the store.
    GetValue getInternal(const DocKey& key,
                         uint16_t vbucket,
                         const void* cookie,
                         vbucket_state_t allowedState,
                         get_options_t options);

    /**
     * Get the meta data for a given key
     *
     * @param vbid     vbucket id where the key needs to be stored
     * @param key      key for which meta data needs to be retrieved
     * @param cookie   cookie for the connection
     * @param itemMeta meta data for the item
     * @param deleted  whether deleted or not
     * @param datatype datatype of the item
     *
     * @result engine error code signifying result of the operation
     */
    ENGINE_ERROR_CODE getMeta(uint16_t vbid,
                              const DocKey key,
                              const void* cookie,
                              ItemMetaData& itemMeta,
                              uint32_t& deleted,
                              uint8_t& datatype);

    /**
     * Schedules the ItemPager according to the current config. Allows testing
     * of the item pager from subclasses, without KVBucket having to grant
     * friendship to many different test classes.
     */
    void scheduleItemPager();

    void initializeExpiryPager();

    bool isItemFreqDecayerTaskSnoozed() const;

    /**
     * Convenience method to run the background fetcher task once (in the
     * current thread).
     */
    void runBGFetcherTask();

    /**
     * Effectively shutdown/restart. This destroys the test engine/store/cookie
     * and re-creates them.
     */
    void reinitialise(std::string config);

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

    static protocol_binary_response_status getAddResponseStatus(
            protocol_binary_response_status newval =
                    PROTOCOL_BINARY_RESPONSE_SUCCESS);

    static protocol_binary_response_status addResponseStatus;

    static const char test_dbname[];

    /**
     * The completeWarmup boolean is read by ::SetUp, if true the following
     * method(s) are called
     *  engine->getKVBucket()->getWarmup()->setComplete();
     *
     * The result is that it appears that warmup has completed, allowing tests
     * to call certain methods which are guarded around isWarmupComplete.
     *
     * By default this is true and tests which actually want to work with warmup
     * can disable this and genuinely warmup by invoking the appropriate methods
     * and tasks.
     */
    void setCompleteWarmup(bool value) {
        completeWarmup = value;
    }

    /**
     * set the random function used by KVBucket.
     * @param randFunction  The random function to be used by the KVBucket.
     */
    void setRandomFunction(std::function<long()>& randFunction);

private:
    /**
     * Initialise test objects - e.g. engine/store/cookie
     */
    void initialise(std::string config);

    /**
     * Destroy the test objects - e.g. engine/store/cookie
     */
    void destroy();

    bool completeWarmup = true;

public:
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
