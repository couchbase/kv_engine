/*
 *     Copyright 2020 Couchbase, Inc
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

#include "clustertest.h"

namespace cb::test {
/// The UpgradeTest class create a 3 node cluster and a bucket without
/// replication set up to allow for mocking that
class UpgradeTest : public ClusterTest {
public:
    static void SetUpTestCase();

    static void TearDownTestCase() {
        bucket.reset();
        ClusterTest::TearDownTestCase();
    }

protected:
    static std::shared_ptr<Bucket> bucket;
};
} // namespace cb::test
