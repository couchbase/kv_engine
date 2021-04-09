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

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <thread>

class OutOfOrderClusterTest : public cb::test::ClusterTest {
protected:
    enum class Reorder { None, Half, All };

    static void testGetReorderWithAndWithoutReorder(Reorder reorder) {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        conn->setFeature(cb::mcbp::Feature::UnorderedExecution, true);

        const std::string prefix = "testGetReorderWithAndWithoutReorder-";
        const std::size_t numDocs = 100;

        std::vector<std::pair<const std::string, Vbid>> keys;
        for (size_t ii = 0; ii < numDocs; ++ii) {
            keys.emplace_back(
                    std::make_pair(prefix + std::to_string(ii), Vbid(0)));
        }

        // Create a bunch of documents
        for (const auto& k : keys) {
            conn->store(k.first, k.second, "value");
        }

        // evict half of them
        for (size_t ii = 0; ii < numDocs; ++ii) {
            if (ii & 1) {
                // evict this one
                conn->evict(keys[ii].first, keys[ii].second);
            }
        }

        bool unordered = false;
        size_t expected = 0;
        size_t counter = 0;
        conn->mget(
                keys,
                [&unordered, &expected, &keys](
                        std::unique_ptr<Document>& doc) -> void {
                    ASSERT_TRUE(expected < keys.size());
                    if (doc->info.id != keys[expected].first) {
                        unordered = true;
                    }
                    ++expected;
                },
                [](const std::string& key,
                   const cb::mcbp::Response& response) -> void {
                    FAIL() << "Failed to get " << key << ": "
                           << to_string(response.getStatus());
                },
                [&reorder, &counter]() -> FrameInfoVector {
                    FrameInfoVector ret;
                    switch (reorder) {
                    case Reorder::None:
                        ret.emplace_back(std::make_unique<BarrierFrameInfo>());
                        break;
                    case Reorder::Half:
                        if (counter & 1) {
                            ret.emplace_back(
                                    std::make_unique<BarrierFrameInfo>());
                        }
                        break;
                    case Reorder::All:
                        break;
                    }
                    ++counter;
                    return ret;
                });

        if (reorder == Reorder::All) {
            ASSERT_TRUE(unordered) << "I didn't get any unordered responses";
        } else {
            ASSERT_FALSE(unordered) << "We received documents out of order";
        }
    }
};

/**
 * Verify that if we send a pipeline with get commands with Barrier bit
 * set for the individual command that we receive the responses in the right
 * sequence even if we need to page them in from disk
 */
TEST_F(OutOfOrderClusterTest, GetSequenceAllBarriers) {
    testGetReorderWithAndWithoutReorder(Reorder::None);
}

/**
 * Verify that if we send a pipeline with get commands without the Barrier bit
 * set for the individual command that we receive the responses out of order
 * as some of the documents needs to be fetched from disk
 */
TEST_F(OutOfOrderClusterTest, GetSequenceNoBarriers) {
    testGetReorderWithAndWithoutReorder(Reorder::All);
}

/**
 * Verify that I don't reorder a command which allows reordering with one
 * which doesn't allow reordering (current == reorder, next == no-reorder)
 */
TEST_F(OutOfOrderClusterTest, OnlyReorderReordableCommands) {
    testGetReorderWithAndWithoutReorder(Reorder::Half);
}
