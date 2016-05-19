/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#pragma once

#include "testapp_client_test.h"

class StatsTest : public TestappClientTest {
public:
    void SetUp() {
        TestappClientTest::SetUp();
        // Let all tests start with an empty set of stats (There is
        // a special test case that tests that reset actually work)
        resetBucket();
    }

protected:
    void resetBucket() {
        MemcachedConnection& conn = getConnection();
        ASSERT_NO_THROW(conn.authenticate("_admin", "password", "PLAIN"));
        ASSERT_NO_THROW(conn.selectBucket("default"));
        ASSERT_NO_THROW(conn.stats("reset"));
        ASSERT_NO_THROW(conn.reconnect());
    }
};
