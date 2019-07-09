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

#include "clustertest.h"

#include "bucket.h"
#include "cluster.h"

#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <condition_variable>
#include <cstdlib>
#include <string>

class DurabilityTest : public cb::test::ClusterTest {};

TEST_F(DurabilityTest, StoreWithDurability) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());

    auto info = conn->store(
            "StoreWithDurability",
            Vbid(0),
            "value",
            cb::mcbp::Datatype::Raw,
            []() -> FrameInfoVector {
                FrameInfoVector ret;
                ret.emplace_back(std::make_unique<DurabilityFrameInfo>(
                        cb::durability::Level::Majority));
                return ret;
            });
    EXPECT_NE(0, info.cas);
}

/**
 * MB-34780 - Bucket delete fails if we've got pending sync writes
 *
 * As part of bucket deletion all of the DCP streams get torn down so
 * a sync write will _never_ complete. This caused bucket deletion to
 * block as the cookie was in an ewouldblock state
 */
TEST_F(DurabilityTest, MB34780) {
    cluster->deleteBucket("default");
    std::mutex mutex;
    std::condition_variable cond;
    bool prepare_seen = false;

    auto bucket = cluster->createBucket(
            "default",
            {{"replicas", 2}, {"max_vbuckets", 8}},
            [&mutex, &cond, &prepare_seen](const std::string& source,
                                           const std::string& destination,
                                           std::vector<uint8_t>& packet) {
                if (prepare_seen) {
                    // Swallow everything..
                    packet.clear();
                    return;
                }

                const auto* h = reinterpret_cast<const cb::mcbp::Header*>(
                        packet.data());
                if (h->isRequest()) {
                    const auto& req = h->getRequest();
                    if (req.getClientOpcode() ==
                        cb::mcbp::ClientOpcode::DcpPrepare) {
                        std::lock_guard<std::mutex> guard(mutex);
                        prepare_seen = true;
                        cond.notify_all();
                        packet.clear();
                    }
                }
            });
    ASSERT_TRUE(bucket) << "Failed to create bucket default";

    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());

    BinprotMutationCommand command;
    command.setKey("MB34780");
    command.setMutationType(MutationType::Set);
    DurabilityFrameInfo frameInfo(cb::durability::Level::Majority);
    command.addFrameInfo(frameInfo);
    conn->sendCommand(command);

    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&prepare_seen]() { return prepare_seen; });

    // At this point we've sent the prepare, it is registered in the
    // durability manager.. We should be able to delete the bucket
    cluster->deleteBucket("default");
}
