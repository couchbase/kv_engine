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

/**
 * Tests which we wish to parameterize based on eviction policy and bloom
 * filter configuration. Ideally we would just inherit from
 * STParameterizedBucketTest and add an additional bloom filter on/off config
 * parameter but that's not possible with the current GTest and my attempts to
 * hackily 'make it so' were not fruitful. Hopefully in the future they make it
 * easy to add parameters to already parameterized test suites. then we can rip
 * out this extra code.
 */
class EPBucketBloomFilterParameterizedTest
    : public SingleThreadedKVBucketTest,
      public ::testing::WithParamInterface<
              ::testing::tuple<std::string, std::string, bool>> {
public:
    static auto allConfigValues() {
        using namespace std::string_literals;
        return ::testing::Values(
#ifdef EP_USE_ROCKSDB
                std::make_tuple("persistent_rocksdb"s, "value_only"s, false),
                std::make_tuple("persistent_rocksdb"s, "value_only"s, true),
                std::make_tuple("persistent_rocksdb"s, "full_eviction"s, false),
                std::make_tuple("persistent_rocksdb"s, "full_eviction"s, true),
#endif
#ifdef EP_USE_MAGMA
                std::make_tuple("persistent_magma"s, "value_only"s, false),
                std::make_tuple("persistent_magma"s, "value_only"s, true),
                std::make_tuple("persistent_magma"s, "full_eviction"s, false),
                std::make_tuple("persistent_magma"s, "full_eviction"s, true),
#endif
                std::make_tuple("persistent_couchstore"s, "value_only"s, false),
                std::make_tuple("persistent_couchstore"s, "value_only"s, true),
                std::make_tuple(
                        "persistent_couchstore"s, "full_eviction"s, false),
                std::make_tuple(
                        "persistent_couchstore"s, "full_eviction"s, true));
    }

protected:
    void SetUp() override;

    bool fullEviction() const {
        return std::get<1>(GetParam()) == "full_eviction";
    }

    bool bloomFiltersEnabled() const {
        return std::get<2>(GetParam()) == true;
    }
};
