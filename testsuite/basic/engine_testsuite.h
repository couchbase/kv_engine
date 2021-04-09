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

#include <folly/portability/GTest.h>
#include <memory>
#include <string>

enum class BucketType : uint8_t;
struct EngineIface;

/**
 * Base class for unit tests for an engine
 */
class EngineTestsuite : public ::testing::Test {
public:
    /// Perform system wide initialization for the test
    ///   initialize sockets, memory allocation hooks, # file descriptors etc
    static void SetUpTestCase();

    /// System wide shutdown (clean up after all of the engines etc)
    static void TearDownTestCase();

protected:
    /**
     * Create a new bucket
     *
     * @param bucketType the type of the bucket to create
     * @param cfg a configuration for the bucket
     * @return a handle to the newly created bucket
     * @throws cb::engine_error for bucket initialization failures
     *         std::bad_alloc for memory allocation failures
     */
    static std::unique_ptr<EngineIface> createBucket(BucketType bucketType,
                                                     const std::string& cfg);
};
