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

#include "bucket.h"
#include "cluster.h"
#include "clustertest.h"
#include "dcp_replicator.h"
#include "node.h"

#include <nlohmann/json.hpp>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

namespace cb {
namespace test {

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

        for (int i = 0; i < 10; i++) {
            auto info = conn->store("foo", Vbid(0), "value");
            EXPECT_NE(0, info.cas);
            EXPECT_NE(0, info.vbucketuuid);

            // Wait for persistence of our item
            ObserveInfo observeInfo;
            do {
                observeInfo = conn->observeSeqno(Vbid(0), info.vbucketuuid);
            } while (observeInfo.lastPersistedSeqno != info.seqno);

            // Force a new checkpoint
            BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::CreateCheckpoint);
            cmd.setVBucket(Vbid(0));
            BinprotResponse rsp;
            rsp = conn->execute(cmd);
            EXPECT_TRUE(rsp.isSuccess());
        }
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

} // namespace test
} // namespace cb
