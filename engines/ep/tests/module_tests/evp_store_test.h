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
#include <utilities/test_manifest.h>

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
    void compactionFindsNonResidentItem();
};

// Full eviction only tests that run with bloom filters off
class EPBucketFullEvictionNoBloomFilterTest : public EPBucketFullEvictionTest {
protected:
    void SetUp() override;

    void MB_52067(bool forceCasMismatch);
};

class EPBucketBloomFilterParameterizedTest : public STParameterizedBucketTest {
public:
    static auto bloomFilterDisabledConfigValues() {
        return persistentAllBackendsNoNexusConfigValues() *
               config::Config{{"bfilter_enabled", "false"}};
    }

protected:
    void SetUp() override;
};

#ifdef EP_USE_MAGMA

/**
 * Test fixture for CDC tests - Magma only
 */
class EPBucketCDCTest : public EPBucketTest {
protected:
    void SetUp() override;

    CollectionsManifest manifest;
};

#endif // EP_USE_MAGMA