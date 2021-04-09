/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "evp_store_single_threaded_test.h"
#include "vbucket_test.h"

class BgFetcher;
class BGFetchItem;
class DiskDocKey;

/**
 * Fixture class for VBucket tests that require an EPEngine instance
 */
class EPVBucketTest : public ::testing::WithParamInterface<EvictionPolicy>,
                      public VBucketTestBase,
                      public SingleThreadedEPBucketTest {
public:
    EPVBucketTest()
        : VBucketTestBase(VBucketTestBase::VBType::Persistent, GetParam()) {
    }

    void SetUp() override;
    void TearDown() override;
    size_t public_queueBGFetchItem(const DocKey& key,
                                   std::unique_ptr<BGFetchItem> fetchItem,
                                   BgFetcher& bgFetcher);
};
