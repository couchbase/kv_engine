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

#include "testapp.h"

/**
 * Test fixture for tests relating to bucket shutdown / startup. Overrides
 * the normal SetUp / TearDown so we start memcached for every test (instead of
 * once for the suite).
 */
class ShutdownTest : public TestappTest {
public:
    static void SetUpTestCase() {
        // Do nothing.
        //
        // If we don't provide a SetUpTestCase we'll get the one from
        // TestappTest which will start the server for us (which is
        // what we want, but in this test we're going to start try
        // to stop the server so we need to have each test case
        // start (and stop) the server for us...
    }

    void SetUp() override;

    void TearDown() override {
        TestappTest::TearDownTestCase();
    }

    static void TearDownTestCase() {
        // Empty
    }
};
