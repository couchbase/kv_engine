/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

class CrossBucketQuotaSharingTest : public cb::test::ClusterTest {
public:
    void SetUp() override {
        // cross_bucket_ht_quota_sharing is not a default configuration for any
        // type of deployment (yet) so we have to enable it manually for these
        // tests. We don't care about replicas so we will disable those here,
        // and we need to set the freq_counter_increment_factor to a lower value
        // than the default to trigger the ItemFreqDecayer more quickly in the
        // tests that care about that. Setting it to 0 means that we always
        // increment the frequency counter.
        bucket1 =
                cluster->createBucket("bucket1",
                                      {{"replicas", 0},
                                       {"max_vbuckets", 8},
                                       {"cross_bucket_ht_quota_sharing", true},
                                       {"freq_counter_increment_factor", 0}});
        bucket2 =
                cluster->createBucket("bucket2",
                                      {{"replicas", 0},
                                       {"max_vbuckets", 8},
                                       {"cross_bucket_ht_quota_sharing", true},
                                       {"freq_counter_increment_factor", 0}});
    }

protected:
    /**
     * Stores an item, saturates it's frequency count so that the decayer task
     * is made to run at least once, then deletes the item.
     *
     * Note that the decayer task may be made to run more than once --
     * we do this on purpose to detect raciness in waking up the per-bucket
     * ItemFreqDecayerTasks.
     */
    int triggerDecayerWithTempItem(MemcachedConnection& conn,
                                   const std::string& key,
                                   Vbid vbid) {
        auto initFreqDecayerRuns =
                conn.stats("")["ep_num_freq_decayer_runs"].get<size_t>();
        // The freq counter is uint8_t and with an increment factor of 0, which
        // means the freq counter is always incremented. It shouldn't take more
        // than std::numeric_limits<uint8_t>::max() increments to saturate it
        // and trigger the ItemFreqDecayer (it will take 250 if the default
        // frequency count is 4).
        conn.store(key, Vbid(0), "value");
        // Overestimate of ops needed to saturate the frequency counter.
        // This can potentially wakeup the BucketItemFreqDecayerTask multiple
        // times and help catch raciness in scheduling.
        int getOps = std::numeric_limits<uint8_t>::max() + 50;
        for (auto i = 0; i < getOps; i++) {
            BinprotGetCommand command{key, vbid};
            conn.sendCommand(command);
        }
        for (auto i = 0; i < getOps; i++) {
            BinprotResponse response;
            conn.recvResponse(response,
                              cb::mcbp::ClientOpcode::Get,
                              std::chrono::milliseconds(1000));
        }
        conn.remove(key, vbid);

        int triggeredRuns;
        do {
            // Wait for the decayer to be triggered.
            triggeredRuns =
                    conn.stats("")["ep_num_freq_decayer_runs"].get<size_t>() -
                    initFreqDecayerRuns;
        } while (triggeredRuns == 0);

        return triggeredRuns;
    }

    std::shared_ptr<cb::test::Bucket> bucket1;
    std::shared_ptr<cb::test::Bucket> bucket2;
};

TEST_F(CrossBucketQuotaSharingTest, ItemFreqDecayerScheduledForAllBuckets) {
    auto conn1 = bucket1->getConnection(Vbid(0));
    conn1->authenticate("@admin", "password", "PLAIN");
    conn1->selectBucket(bucket1->getName());

    auto conn2 = bucket2->getConnection(Vbid(0));
    conn2->authenticate("@admin", "password", "PLAIN");
    conn2->selectBucket(bucket2->getName());

    auto bucket1FreqDecayerRuns =
            conn1->stats("")["ep_num_freq_decayer_runs"].get<size_t>();
    auto bucket2FreqDecayerRuns =
            conn2->stats("")["ep_num_freq_decayer_runs"].get<size_t>();
    ASSERT_EQ(bucket1FreqDecayerRuns, bucket2FreqDecayerRuns);

    // Trigger two decayer task runs for *bucket1*
    int triggeredRuns = triggerDecayerWithTempItem(*conn1, "key", Vbid(0));
    triggeredRuns += triggerDecayerWithTempItem(*conn1, "key", Vbid(0));

    EXPECT_EQ(bucket1FreqDecayerRuns + triggeredRuns,
              conn1->stats("")["ep_num_freq_decayer_runs"].get<size_t>());
    // Make sure we've ran the decayer task on *bucket2* also.
    EXPECT_EQ(bucket1FreqDecayerRuns + triggeredRuns,
              conn2->stats("")["ep_num_freq_decayer_runs"].get<size_t>());
}
