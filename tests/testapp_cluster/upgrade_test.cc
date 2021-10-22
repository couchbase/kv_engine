/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "upgrade_test.h"
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/dcp_replicator.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

std::shared_ptr<cb::test::Bucket> cb::test::UpgradeTest::bucket;

void cb::test::UpgradeTest::SetUpTestCase() {
    try {
        bucket = cluster->createBucket(
                "default",
                {{"replicas", 2}, {"max_vbuckets", 1}, {"max_num_shards", 1}},
                {},
                false /*No replication*/);
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}

void cb::test::UpgradeTest::TearDownTestCase() {
    bucket.reset();
    cluster->deleteBucket("default");
}

using UpgradeTest = cb::test::UpgradeTest;
TEST_F(UpgradeTest, ExpiryOpcodeDoesntEnableDeleteV2) {
    // MB-38390: Check that DcpProducers respect the includeDeleteTime flag
    // sent at dcpOpen, and "enable_expiry_opcode" did not erroneously cause the
    // producer to send deletion times.

    // Set up replication from node 0 to node 1 with flags==0, specifically so
    // includeDeleteTime is disabled.
    bucket->setupReplication({{0, 1, false, 0}, {0, 2, false, 0}});

    // store and delete a document on the active
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());
    auto info = conn->store("foo", Vbid(0), "value");
    EXPECT_NE(0, info.cas);
    info = conn->remove("foo", Vbid(0));
    EXPECT_NE(0, info.cas);

    // make sure that the delete is replicated to all replicas
    const auto nrep = bucket->getVbucketMap()[0].size() - 1;
    for (std::size_t rep = 0; rep < nrep; ++rep) {
        conn = bucket->getConnection(Vbid(0), vbucket_state_replica, rep);
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());

        // Wait for persistence of our item
        ObserveInfo observeInfo;
        do {
            observeInfo = conn->observeSeqno(Vbid(0), info.vbucketuuid);
        } while (observeInfo.lastPersistedSeqno != info.seqno);
    }

    // Done! The consumer side dcp_deletion_validator would throw if
    // the producer sent a V2 deletion despite the flag being unset.
}
