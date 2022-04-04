/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "clustertest.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>

class PiTR_Test : public cb::test::ClusterTest {
protected:
};

/// See https://issues.couchbase.com/browse/MB-51007 for details
TEST_F(PiTR_Test, DISABLED_MB51007) {
    auto bucket = cluster->createBucket("MB51007",
                                        {{"pitr_enabled", "true"},
                                         {"pitr_granularity", 1},
                                         {"max_vbuckets", 8},
                                         {"replicas", 2}});
    auto conn = bucket->getAuthedConnection(Vbid{0});
    // The (initial) sync write may take longer time than the default timeout
    // so lets bump the timeout
    conn->setReadTimeout(std::chrono::seconds{1000});
    auto info = conn->store(
            "MB51007",
            Vbid{0},
            bucket->getCollectionManifest().dump(),
            cb::mcbp::Datatype::JSON,
            0,
            []() {
                FrameInfoVector ret;
                ret.emplace_back(std::make_unique<DurabilityFrameInfo>(
                        cb::durability::Level::MajorityAndPersistOnMaster));
                return ret;
            });

    // Wait for the persistence of our item
    ObserveInfo observeInfo;
    do {
        observeInfo = conn->observeSeqno(Vbid(0), info.vbucketuuid);
    } while (observeInfo.lastPersistedSeqno != info.seqno);

    // Let the test run for 10 seconds
    const auto timeLimit = std::chrono::seconds{10};
    const auto timeout = std::chrono::steady_clock::now() + timeLimit;
    // Create a thread which constantly compact the database
    int num_compaction = 0;
    std::thread compaction_thread{
            [&bucket, &timeout, &num_compaction, &timeLimit]() {
                auto conn = bucket->getAuthedConnection(Vbid{0});
                conn->setReadTimeout(timeLimit);
                do {
                    auto rsp = conn->execute(BinprotCompactDbCommand{});
                    if (rsp.isSuccess()) {
                        // @TODO MB-51037:
                        // Compaction of older headers may not find a
                        // vbucket_state and that currently causes a compaction
                        // to fail. We should probably handle that better in
                        // the future.
                        ++num_compaction;
                    }
                } while (std::chrono::steady_clock::now() < timeout);
            }};

    int num_store = 0;
    do {
        conn->store(
                "MB51007",
                Vbid{0},
                bucket->getCollectionManifest().dump(),
                cb::mcbp::Datatype::JSON,
                0,
                []() {
                    FrameInfoVector ret;
                    ret.emplace_back(std::make_unique<DurabilityFrameInfo>(
                            cb::durability::Level::MajorityAndPersistOnMaster));
                    return ret;
                });
        ++num_store;
    } while (std::chrono::steady_clock::now() < timeout);
    compaction_thread.join();
    ASSERT_LE(10, num_compaction) << "Expected at least 10 compactions";
    ASSERT_LE(10, num_store) << "Expected at least 10 store";
}
