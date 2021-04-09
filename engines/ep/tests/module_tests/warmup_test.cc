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

/*
 * Unit tests for Warmup-related functions.
 *
 */

#include "evp_engine_test.h"

#include "ep_engine.h"

#include "../mock/mock_add_stat_fn.h"
#include <folly/portability/GTest.h>
#include <memcached/tracer.h>

class WarmupDisabledTest : public EventuallyPersistentEngineTest {

    void SetUp() override {
        config_string = "warmup=false";
        EventuallyPersistentEngineTest::SetUp();
    }
};

// Check we get the expected stats (i.e. none) when Warmup is disabled.
TEST_F(WarmupDisabledTest, Stats) {
    MockAddStat add_stat;
    cb::tracing::Traceable cookie;
    using ::testing::_;
    EXPECT_CALL(add_stat, callback("ep_warmup", _, _)).Times(0);
    EXPECT_EQ(
            cb::engine_errc::no_such_key,
            engine->getStats(&cookie, "warmup", {}, add_stat.asStdFunction()));
}
