/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"

#include <memcached/limits.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <utilities/json_utilities.h>

#include <fmt/format.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>

class BucketTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        auto config = generate_config();
        config["threads"] = 1;
        TestappTest::doSetUpTestCaseWithConfiguration(config);
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         BucketTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(BucketTest, TestCreateBucketAlreadyExists) {
    try {
        adminConnection->createBucket(bucketName, {}, BucketType::Couchbase);
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.getReason();
    }
}

TEST_P(BucketTest, TestDeleteNonexistingBucket) {
    try {
        adminConnection->deleteBucket("ItWouldBeSadIfThisBucketExisted");
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.getReason();
    }
}

/**
 * Delete a bucket with a 5 second timeout
 *
 * @param conn The connection to send the delete bucket over
 * @param name The name of the bucket to delete
 * @param stateCallback A callback function called _every_ time we fetch the
 *                      state for the bucket during bucket deletion
 */
static void deleteBucket(
        MemcachedConnection& conn,
        const std::string& name,
        const std::function<void(const std::string&)>& stateCallback) {
    auto clone = conn.clone();
    clone->authenticate("@admin");
    const auto timeout =
            std::chrono::system_clock::now() + std::chrono::seconds{5};
    conn.sendCommand(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::DeleteBucket, name});

    bool found;
    do {
        // Avoid busy-wait ;-)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        auto details = clone->stats("bucket_details");
        auto bucketDetails = details["bucket details"];
        found = false;
        for (const auto& bucket : bucketDetails["buckets"]) {
            auto nm = bucket.find("name");
            if (nm != bucket.end()) {
                if (nm->get<std::string>() == name) {
                    if (stateCallback) {
                        stateCallback(bucket["state"].get<std::string>());
                    }
                    found = true;
                }
            }
        }
    } while (found && std::chrono::system_clock::now() < timeout);

    if (found) {
        throw std::runtime_error("Timed out waiting for bucket '" + name +
                                 "' to be deleted");
    }

    // read out the delete response
    BinprotResponse rsp;
    conn.recvResponse(rsp);
    ASSERT_TRUE(rsp.isSuccess());
    ASSERT_EQ(cb::mcbp::ClientOpcode::DeleteBucket, rsp.getOp());
}

// Unit test to verify that a connection currently sending a command to the
// server won't block bucket deletion (the server don't wait for the client
// send all the data, but shut down the connection immediately)
TEST_P(BucketTest, DeleteWhileClientSendCommand) {
    mcd_env->getTestBucket().createBucket("bucket", {}, *adminConnection);
    auto& second_conn = getConnection();
    second_conn.authenticate("Luke");
    second_conn.selectBucket("bucket");

    // We need to get the second connection sitting the `conn_read_packet_body`
    // state in memcached - i.e. waiting to read a variable-amount of data from
    // the client. Simplest is to perform a GET where we don't send the full key
    // length, by only sending a partial frame
    auto frame =
            second_conn.encodeCmdGet("dummy_key_which_we_will_crop", Vbid(0));
    second_conn.sendPartialFrame(frame, frame.payload.size() - 1);
    adminConnection->deleteBucket("bucket");
}

// Test delete of a bucket while we've got a client connected to the bucket
// which is currently running a background operation in the engine (the engine
// returned EWB and started a long-running task which would complete some
// time in the future).
//
// To simulate this we'll instruct ewb engine to monitor the existence of
// a file and the removal of the file simulates that the background task
// completes and the cookie should be signalled.
TEST_P(BucketTest, DeleteWhileClientConnectedAndEWouldBlocked) {
    mcd_env->getTestBucket().setUpBucket("bucket", {}, *adminConnection);

    std::vector<std::unique_ptr<MemcachedConnection>> connections;
    std::vector<std::string> lockfiles;

    auto& conn = getConnection();
    for (int jj = 0; jj < 5; ++jj) {
        connections.emplace_back(conn.clone());
        auto& c = connections.back();
        c->authenticate("Luke");
        c->selectBucket("bucket");

        auto testfile =
                std::filesystem::current_path() / cb::io::mktemp("lockfile");

        // Configure so that the engine will return
        // cb::engine_errc::would_block and not process any operation given
        // to it.  This means the connection will remain in a blocked state.
        c->configureEwouldBlockEngine(EWBEngineMode::BlockMonitorFile,
                                      cb::engine_errc::would_block /* unused */,
                                      jj,
                                      testfile.generic_string());
        lockfiles.emplace_back(testfile.generic_string());
        c->sendCommand(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, "mykey"});
    }

    deleteBucket(
            *adminConnection, "bucket", [&lockfiles](const std::string& state) {
                if (lockfiles.empty()) {
                    return;
                }
                if (state == "destroying") {
                    for (const auto& f : lockfiles) {
                        std::filesystem::remove_all(f);
                    }

                    lockfiles.clear();
                }
            });
}

static int64_t getTotalSent(MemcachedConnection& conn, intptr_t id) {
    const auto stats = conn.stats("connections " + std::to_string(id));
    if (stats.empty()) {
        throw std::runtime_error("getConnectionStats(): nothing returned");
    }

    if (stats.size() != 1) {
        throw std::runtime_error(
                "getConnectionStats(): Expected a single entry");
    }

    return stats.front()["total_send"].get<int64_t>();
}

/**
 * Verify that we nuke connections stuck in sending the data back to
 * the client due to the client not draining their socket buffer
 *
 * The test tries to store a 20MB document in the cache, then
 * tries to fetch that document until the socket buffer is full
 * (because we never try to read the data)
 */
TEST_P(BucketTest, DeleteWhileSendDataAndFullWriteBuffer) {
    mcd_env->getTestBucket().setUpBucket("bucket", {}, *adminConnection);
    auto& conn = getConnection();
    conn.authenticate("Luke");
    const auto id = conn.getServerConnectionId();
    conn.selectBucket("bucket");

    // Store the document I want to fetch
    Document document;
    document.info.id = name;
    document.info.flags = 0xdeadbeef;
    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.datatype = cb::mcbp::Datatype::Raw;
    // Store a 20MB value in the cache
    document.value.assign(20_MiB, 'b');

    const auto info = conn.mutate(document, Vbid(0), MutationType::Set);
    EXPECT_NE(0, info.cas);

    BinprotGetCommand cmd(name);

    std::atomic_bool blocked{false};

    // I've seen cases where send() is being blocked due to the
    // clients receive buffer is full...
    std::thread client{[&conn, &blocked, &cmd]() {
        // Fill up the send buffer on the memcached server:
        try {
            do {
                conn.sendCommand(cmd);
            } while (!blocked.load());
        } catch (const std::exception& e) {
            std::cerr << "DeleteWhileSendDataAndFullWriteBuffer: Failed to "
                         "send data to the server: "
                      << e.what()
                      << " we might have deleted the bucket already and been "
                         "disconnected"
                      << std::endl;
        }
    }};

    adminConnection->executeInBucket("bucket", [&](auto& c) {
        // Wait until the server filled up all of the socket buffers in the
        // kernel so we don't make any progress when trying to send more data.
        do {
            const auto totalSend = getTotalSent(c, id);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            if (totalSend == getTotalSent(c, id)) {
                blocked.store(true);
            }
        } while (!blocked);
    });

    // The socket is blocked so we may delete the bucket
    deleteBucket(*adminConnection, "bucket", {});
    client.join();
}

TEST_P(BucketTest, TestListBucket) {
    auto buckets = adminConnection->listBuckets();
    EXPECT_EQ(1, buckets.size());
    EXPECT_EQ(bucketName, buckets[0]);
}

TEST_P(BucketTest, TestListBucket_not_authenticated) {
    auto& conn = getConnection();
    try {
        conn.listBuckets();
        FAIL() << "unauthenticated users should not be able to list buckets";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

/// Smith only has access to a bucket named rbac_test (and not the
/// default bucket) so when we authenticate as smith we shouldn't be put
/// into rbac_test, but be in no_bucket
TEST_P(BucketTest, TestNoAutoSelectOfBucketForNormalUser) {
    mcd_env->getTestBucket().createBucket("rbac_test", {}, *adminConnection);

    auto& conn = getConnection();
    conn.authenticate("smith");
    auto response = conn.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, name});
    EXPECT_EQ(cb::mcbp::Status::NoBucket, response.getStatus());

    adminConnection->deleteBucket("rbac_test");
}

TEST_P(BucketTest, TestListSomeBuckets) {
    mcd_env->getTestBucket().createBucket("bucket-1", {}, *adminConnection);
    mcd_env->getTestBucket().createBucket("bucket-2", {}, *adminConnection);
    mcd_env->getTestBucket().createBucket("rbac_test", {}, *adminConnection);

    const std::vector<std::string> all_buckets = {
            bucketName, "bucket-1", "bucket-2", "rbac_test"};
    EXPECT_EQ(all_buckets, adminConnection->listBuckets());

    // Reconnect and authenticate as a user with access to only one of them
    auto& conn = getConnection();
    conn.authenticate("smith");
    const std::vector<std::string> expected = {"rbac_test"};
    EXPECT_EQ(expected, conn.listBuckets());

    adminConnection->deleteBucket("bucket-1");
    adminConnection->deleteBucket("bucket-2");
    adminConnection->deleteBucket("rbac_test");
}

/// Test that one bucket don't leak information into another bucket
/// and that we can create up to the maximum number of buckets
/// allowd
TEST_P(BucketTest, TestBucketIsolationAndMaxBuckets) {
    size_t totalBuckets = cb::limits::TotalBuckets;
    if (folly::kIsSanitize) {
        // We don't need to test _all_ buckets when running under sanitizers
        totalBuckets = 5;
    }

    for (std::size_t ii = 1; ii < totalBuckets; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        GetTestBucket().createBucket(ss.str(), "", *adminConnection);
    }

    if (totalBuckets == cb::limits::TotalBuckets) {
        try {
            GetTestBucket().createBucket(
                    "BucketShouldFail", "", *adminConnection);
            FAIL() << "It should not be possible to test more than "
                   << cb::limits::TotalBuckets << "buckets";
        } catch (ConnectionError&) {
        }
    }

    // I should be able to select each bucket and the same document..
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = "TestBucketIsolationBuckets";
    doc.value = memcached_cfg.dump();

    for (std::size_t ii = 1; ii < totalBuckets; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        const auto name = ss.str();
        adminConnection->selectBucket(name);
        adminConnection->mutate(doc, Vbid(0), MutationType::Add);
    }

    adminConnection->unselectBucket();
    // Delete all buckets
    for (std::size_t ii = 1; ii < totalBuckets; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        adminConnection->deleteBucket(ss.str());
    }
}

/// Verify that we can delete the currently selected bucket
TEST_P(BucketTest, DeleteSelectedBucket) {
    mcd_env->getTestBucket().createBucket("bucket", {}, *adminConnection);
    adminConnection->selectBucket("bucket");
    deleteBucket(*adminConnection, "bucket", [](const std::string&) {});
}
