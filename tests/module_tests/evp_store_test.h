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

/* Actual test fixture class */
class EPBucketTest : public ::testing::Test {
protected:
    void SetUp() override;

    void TearDown() override;

    // Creates an item with the given vbucket id, key and value.
    static Item make_item(uint16_t vbid, const StoredDocKey& key,
                          const std::string& value,
                          uint32_t exptime = 0,
                          protocol_binary_datatype_t datatype =
                                  PROTOCOL_BINARY_DATATYPE_JSON);

    // Stores an item into the given vbucket. Returns the item stored.
    Item store_item(uint16_t vbid, const StoredDocKey& key,
                    const std::string& value,
                    uint32_t exptime = 0,
                    protocol_binary_datatype_t datatype =
                            PROTOCOL_BINARY_DATATYPE_JSON);

    /* Flush the given vbucket to disk, so any outstanding dirty items are
     * written (and are clean).
     */
    void flush_vbucket_to_disk(uint16_t vbid);

    /* Delete the given item from the given vbucket, verifying it was
     * successfully deleted.
     */
    void delete_item(uint16_t vbid, const StoredDocKey& key);

    /* Evict the given key from memory according to the current eviction
     * strategy. Verifies it was successfully evicted.
     */
    void evict_key(uint16_t vbid, const StoredDocKey& key);

    static const char test_dbname[];

    std::string config_string;

    const uint16_t vbid = 0;

    // The mock engine (needed to construct the store).
    std::unique_ptr<SynchronousEPEngine> engine;

    // The store under test. Wrapped in a mock to expose some normally
    // protected members. Uses a raw pointer as this is owned by the engine.
    MockEPStore* store;

    // The (mock) server cookie.
    const void* cookie;
};
