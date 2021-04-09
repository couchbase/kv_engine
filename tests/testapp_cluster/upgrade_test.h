/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
