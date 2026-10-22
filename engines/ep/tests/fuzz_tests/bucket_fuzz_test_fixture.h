/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "tests/module_tests/evp_store_single_threaded_test.h"

#include <fuzztest/googletest_fixture_adapter.h>

#include <type_traits>

namespace cb::fuzzing {

/**
 * Adapts a bucket test fixture for FUZZ_TEST_F, set up once and reused across
 * all iterations of the fuzz test.
 *
 * When an exception escapes an iteration, FuzzTest destroys the fixture
 * without calling TearDown, which would have purged the tasks of the engine.
 * Destroying the engine would then wait forever for those tasks, as nothing
 * runs them, and the exception would never be reported.
 */
template <typename Fixture>
class BucketFuzzTestFixture
    : public fuzztest::PerFuzzTestFixtureAdapter<Fixture> {
    static_assert(std::is_base_of_v<SingleThreadedKVBucketTest, Fixture>);

public:
    ~BucketFuzzTestFixture() override {
        if (this->engine) {
            this->shutdownAndPurgeTasks(this->engine.get());
        }
    }
};

} // namespace cb::fuzzing
