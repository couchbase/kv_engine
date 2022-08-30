/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "serverless_test.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/node.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <platform/timeutils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

using namespace std::string_literals;

namespace cb::test {

auto getBucketInformation(MemcachedConnection& adminConn,
                          std::string_view bucketName) {
    nlohmann::json stats;
    adminConn.stats(
            [&stats](auto k, auto v) { stats = nlohmann::json::parse(v); },
            "bucket_details "s.append(bucketName));
    return stats;
}

// Test that we can pause a bucket ready for hibernation, and then resume it.
// Based on testapp_test PauseResumeTest::Basic but under full serverless config
// (Magma backend, multiple buckets, multiple nodes).
TEST(PauseResumeServerlessTest, PauseResume) {
    auto writeDoc = [](MemcachedConnection& conn) {
        Document doc;
        doc.info.id = "mydoc";
        doc.value = "This is the value";
        conn.mutate(doc, Vbid{0}, MutationType::Set);
    };

    // Setup admin connections to all nodes so we can issue pause / resume
    // requests.
    std::vector<std::unique_ptr<MemcachedConnection>> adminConns;
    cluster->iterateNodes([&adminConns](const Node& node) {
        auto conn = node.getConnection();
        conn->authenticate("@admin", "password");
        adminConns.push_back(std::move(conn));
    });

    // store a document to bucket-0
    auto bucket0 = adminConns.at(0)->clone();
    bucket0->authenticate("bucket-0", "bucket-0");
    bucket0->selectBucket("bucket-0");
    writeDoc(*bucket0);

    // Pause bucket-0.
    // As part of this all established connections will be closed; this is
    // expected but our DcpPipe test harness doesn't like having its
    // connections closed unexpectedly. Therefore we need to explicitly
    // shutdown replication before calling pause(). Note that in the full stack
    // ns_server will shutdown replication before pausing us.
    cluster->getBucket("bucket-0")->shutdownReplication();
    // Now issue pause() request to all nodes.
    for (auto& conn : adminConns) {
        auto rsp = conn->execute(BinprotPauseBucketCommand{"bucket-0"});
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus())
                << rsp.getDataView();
    }

    // Verify bucket status post-pause.
    for (auto& conn : adminConns) {
        EXPECT_EQ("paused", getBucketInformation(*conn, "bucket-0")["state"]);
    }

    // writing a document shouldn't be possible after pause
    try {
        writeDoc(*bucket0);
        FAIL() << "Should not be able to store a document after pause";
    } catch (const ConnectionError& error) {
        FAIL() << "Connection should have been closed instead of an error: "
               << error.what();
    } catch (const std::runtime_error& e) {
        // Expected
    }

    // Succeeds to store a document in bucket which is not paused (bucket-1)
    auto bucket1 = adminConns.at(0)->clone();
    bucket1->authenticate("bucket-1", "bucket-1");
    bucket1->selectBucket("bucket-1");
    EXPECT_EQ("ready",
              getBucketInformation(*adminConns.at(0), "bucket-1")["state"]);
    writeDoc(*bucket1);

    // unpause bucket-0 (admin connections should still be established)
    for (auto& conn : adminConns) {
        auto rsp = conn->execute(BinprotResumeBucketCommand{"bucket-0"});
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus())
                << rsp.getDataView();
        EXPECT_EQ("ready", getBucketInformation(*conn, "bucket-0")["state"]);
    }

    // succeed to store a document to bucket-0
    // (must reconnect as old connection was disconnected on pause).
    cluster->getBucket("bucket-0")->setupReplication();
    bucket0 = adminConns.at(0)->clone();
    bucket0->authenticate("bucket-0", "bucket-0");
    bucket0->selectBucket("bucket-0");

    writeDoc(*bucket0);
}

} // namespace cb::test
