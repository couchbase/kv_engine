/*
 *     Copyright 2019 Couchbase, Inc
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
     * @param name the name of the bucket (for logging)
     * @param cfg a configuration for the bucket
     * @return a handle to the newly created bucket
     * @throws cb::engine_error for bucket initialization failures
     *         std::bad_alloc for memory allocation failures
     */
    static std::unique_ptr<EngineIface> createBucket(BucketType bucketType,
                                                     const std::string& name,
                                                     const std::string& cfg);
};
