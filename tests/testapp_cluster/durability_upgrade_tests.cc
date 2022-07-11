/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"
#include "upgrade_test.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/dcp_replicator.h>
#include <cluster_framework/node.h>
#include <nlohmann/json.hpp>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

namespace cb::test {

class DurabilityUpgradeTest : public UpgradeTest {};

/**
 * Test that when we receive a Disk snapshot from a non-MH node that we do not
 * throw any assertions when we attempt to replicate it to another node due to
 * having an uninitialized HCS value in a Disk Checkpoint.
 */
TEST_F(DurabilityUpgradeTest, DiskHCSFromNonSyncRepNode) {
    // 1) Do a few sets, we need to cycle through enough checkpoints to force
    // some to be removed from memory so that we backfill from disk when we set
    // up DCP.
    {
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());

        // Force a new checkpoint everytime an item is queued
        const auto resp = conn->execute(BinprotSetParamCommand(
                cb::mcbp::request::SetParamPayload::Type::Checkpoint,
                "checkpoint_max_size",
                "1"));
        EXPECT_TRUE(resp.isSuccess());

        for (int i = 0; i < 10; i++) {
            auto info = conn->store("foo", Vbid(0), "value");
            EXPECT_NE(0, info.cas);
            EXPECT_NE(0, info.vbucketuuid);

            // Wait for persistence of our item
            ObserveInfo observeInfo;
            do {
                observeInfo = conn->observeSeqno(Vbid(0), info.vbucketuuid);
            } while (observeInfo.lastPersistedSeqno != info.seqno);
        }

        ASSERT_GT(conn->stats("")["ep_items_rm_from_checkpoints"].get<size_t>(),
                  0);

        conn->close();
    }

    // 2) Set up replication from node 0 to node 1 without sync repl enabled
    // (i.e. no consumer name which in turn will mean that we don't send the
    // HCS when we send a disk snapshot marker). This is to mock the behaviour
    // of a legacy producer that would send a V1 snapshot marker.
    bucket->setupReplication({{0, 1, false}});

    // 3) Get replica on node 1 to ensure that we have replicated our document.
    auto conn = bucket->getConnection(Vbid(0), vbucket_state_replica, 0);
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());
    getReplica(*conn.get(), Vbid(0), "foo");

    // 4) Now hook up DCP from node 1 to node 2. This mocks the behaviour we
    // have when we fully upgrade this cluster and stream our original items to
    // a new MadHatter consumer (i.e. sync repl enabled). This should send a HCS
    // value that is non-zero to the replica.
    conn->setVbucket(Vbid(0),
                     vbucket_state_active,
                     {{"topology", nlohmann::json::array({{"n_1", "n_2"}})}});
    bucket->setupReplication({{1, 2, true}});

    // 5) Verify the replication stream works by doing another get replica.
    conn = bucket->getConnection(Vbid(0), vbucket_state_replica, 1);
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());
    getReplica(*conn.get(), Vbid(0), "foo");

    conn->close();
}

} // namespace cb::test
