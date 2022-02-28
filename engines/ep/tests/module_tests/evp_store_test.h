/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit tests for the EPBucket class.
 *
 * These are instantiated for value and full eviction persistent buckets.
 */

#pragma once

#include "evp_store_single_threaded_test.h"
#include "kv_bucket_test.h"

/**
 * Persistent bucket only tests
 */
class EPBucketTest : public STParameterizedBucketTest {
protected:
    void SetUp() override;

    EPBucket& getEPBucket();
};

// Full eviction only tests
class EPBucketFullEvictionTest : public EPBucketTest {
public:
    void compactionFindsNonResidentItem(bool dropCollection,
                                        bool switchToReplica);
};

// Full eviction only tests that run with bloom filters off
class EPBucketFullEvictionNoBloomFilterTest : public EPBucketFullEvictionTest {
protected:
    void SetUp() override;
};

class EPBucketBloomFilterParameterizedTest : public STParameterizedBucketTest {
public:
    static auto bloomFilterDisabledConfigValues() {
        using namespace std::string_literals;
        return ::testing::Values(
#ifdef EP_USE_ROCKSDB
                "bucket_type=persistent:"
                "backend=rocksdb:"
                "item_eviction_policy=value_only:"
                "bfilter_enabled=false"s,
                "bucket_type=persistent:"
                "backend=rocksdb:"
                "item_eviction_policy=full_eviction:"
                "bfilter_enabled=false"s,
#endif
#ifdef EP_USE_MAGMA
                "bucket_type=persistent:"
                "backend=magma:"
                "item_eviction_policy=value_only:"
                "bfilter_enabled=false"s,
                "bucket_type=persistent:"
                "backend=magma:"
                "item_eviction_policy=full_eviction:"
                "bfilter_enabled=false"s,
#endif
                "bucket_type=persistent:"
                "backend=couchstore:"
                "item_eviction_policy=value_only:"
                "bfilter_enabled=false"s,
                "bucket_type=persistent:"
                "backend=couchstore:"
                "item_eviction_policy=full_eviction:"
                "bfilter_enabled=false"s);
    }

protected:
    void SetUp() override;
};
