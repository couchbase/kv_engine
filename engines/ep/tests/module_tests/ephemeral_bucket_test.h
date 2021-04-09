/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

/**
 * Unit tests for the EphemeralBucket class.
 */
#include "stats_test.h"

/**
 * Ephemeral Bucket test fixture class for statistics tests.
 */
class EphemeralBucketStatTest : public StatTest {
protected:
    void SetUp() override {
        config_string += "bucket_type=ephemeral";
        StatTest::SetUp();
    }

    /// Add a number of documents to allow testing of sequence list stats.
    void addDocumentsForSeqListTesting(Vbid vbid);
};

/**
 * Test fixture for single-threaded Ephemeral tests.
 */
class SingleThreadedEphemeralTest : public SingleThreadedKVBucketTest {
protected:
    void SetUp() override {
        config_string += "bucket_type=ephemeral;ht_size=1";
        SingleThreadedKVBucketTest::SetUp();
    }
};
