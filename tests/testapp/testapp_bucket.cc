/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "testapp_client_test.h"

#include <memcached/limits.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <utilities/json_utilities.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>

class BucketTest : public TestappClientTest {};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         BucketTest,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(BucketTest, TestNameTooLong) {
    auto& connection = getAdminConnection();
    std::string name;
    name.resize(101);
    std::fill(name.begin(), name.end(), 'a');

    try {
        connection.createBucket(name, "", BucketType::Memcached);
        FAIL() << "Invalid bucket name is not refused";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
    }
}

TEST_P(BucketTest, TestMaxNameLength) {
    auto& connection = getAdminConnection();
    std::string name;
    name.resize(100);
    std::fill(name.begin(), name.end(), 'a');

    connection.createBucket(name, "", BucketType::Memcached);
    connection.deleteBucket(name);
}

TEST_P(BucketTest, TestEmptyName) {
    auto& connection = getAdminConnection();

    try {
        connection.createBucket("", "", BucketType::Memcached);
        FAIL() << "Empty bucket name is not refused";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
    }
}

TEST_P(BucketTest, TestInvalidCharacters) {
    auto& connection = getAdminConnection();

    std::string name("a ");

    for (int ii = 1; ii < 256; ++ii) {
        name.at(1) = char(ii);
        bool legal = true;

        // According to DOC-107:
        // "The bucket name can only contain characters in range A-Z, a-z, 0-9 as well as
        // underscore, period, dash and percent symbols"
        if (!(isupper(ii) || islower(ii) || isdigit(ii))) {
            switch (ii) {
            case '_':
            case '-':
            case '.':
            case '%':
                break;
            default:
                legal = false;
            }
        }

        if (legal) {
            connection.createBucket(name, "", BucketType::Memcached);
            connection.deleteBucket(name);
        } else {
            try {
                connection.createBucket(name, "", BucketType::Memcached);
                FAIL() <<
                       "I was able to create a bucket with character of value " <<
                       ii;
            } catch (ConnectionError& error) {
                EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
            }
        }
    }
}

TEST_P(BucketTest, TestMultipleBuckets) {
    auto& connection = getAdminConnection();
    std::size_t ii;
    try {
        for (ii = 1; ii < cb::limits::TotalBuckets; ++ii) {
            std::string name = "bucket-" + std::to_string(ii);
            connection.createBucket(name, "", BucketType::Memcached);
        }
    } catch (ConnectionError&) {
        FAIL() << "Failed to create more than " << ii << " buckets";
    }

    for (--ii; ii > 0; --ii) {
        std::string name = "bucket-" + std::to_string(ii);
        connection.deleteBucket(name);
    }
}

TEST_P(BucketTest, TestCreateBucketAlreadyExists) {
    auto& conn = getAdminConnection();
    try {
        conn.createBucket("default", "", BucketType::Memcached);
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.getReason();
    }
}

TEST_P(BucketTest, TestDeleteNonexistingBucket) {
    auto& conn = getAdminConnection();
    try {
        conn.deleteBucket("ItWouldBeSadIfThisBucketExisted");
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
        std::function<void(const std::string&)> stateCallback) {
    auto clone = conn.clone();
    clone->authenticate("@admin", "password", "PLAIN");
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
}

// Unit test to verify that a connection currently sending a command to the
// server won't block bucket deletion (the server don't wait for the client
// send all of the data, but shut down the connection immediately)
TEST_P(BucketTest, DeleteWhileClientSendCommand) {
    auto& conn = getAdminConnection();
    conn.createBucket("bucket", "", BucketType::Memcached);

    auto second_conn = conn.clone();
    second_conn->authenticate("@admin", "password", "PLAIN");
    second_conn->selectBucket("bucket");

    // We need to get the second connection sitting the `conn_read_packet_body`
    // state in memcached - i.e. waiting to read a variable-amount of data from
    // the client. Simplest is to perform a GET where we don't send the full key
    // length, by only sending a partial frame
    Frame frame =
            second_conn->encodeCmdGet("dummy_key_which_we_will_crop", Vbid(0));
    second_conn->sendPartialFrame(frame, frame.payload.size() - 1);
    conn.deleteBucket("bucket");
}

// Test delete of a bucket while we've got a client connected to the bucket
// which is currently running a backround operation in the engine (the engine
// returned EWB and started a longrunning task which would complete some
// time in the future).
//
// To simulate this we'll instruct ewb engine to monitor the existence of
// a file and the removal of the file simulates that the background task
// completes and the cookie should be signalled.
TEST_P(BucketTest, DeleteWhileClientConnectedAndEWouldBlocked) {
    auto& conn = getAdminConnection();
    conn.createBucket("bucket", "default_engine.so", BucketType::EWouldBlock);
    auto second_conn = conn.clone();
    second_conn->authenticate("@admin", "password", "PLAIN");
    second_conn->selectBucket("bucket");
    auto connection = conn.clone();
    connection->authenticate("@admin", "password", "PLAIN");

    auto cwd = cb::io::getcwd();
    auto testfile = cwd + "/" + cb::io::mktemp("lockfile");

    // Configure so that the engine will return ENGINE_EWOULDBLOCK and
    // not process any operation given to it.  This means the connection
    // will remain in a blocked state.
    second_conn->configureEwouldBlockEngine(EWBEngineMode::BlockMonitorFile,
                                            ENGINE_EWOULDBLOCK /* unused */,
                                            0,
                                            testfile);

    // Send the get operation, however we will not get a response from the
    // engine, and so it will block indefinitely.
    second_conn->sendCommand(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Get, "dummy_key_where_never_return"});

    deleteBucket(conn, "bucket", [&testfile](const std::string& state) {
        if (testfile.empty()) {
            return;
        }
        if (state == "destroying") {
            cb::io::rmrf(testfile);
            testfile.clear();
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
    auto& conn = getAdminConnection();
    const auto id = conn.getServerConnectionId();
    conn.createBucket("bucket",
                      "cache_size=67108864;item_size_max=22020096",
                      BucketType::Memcached);
    conn.selectBucket("bucket");

    auto second_conn = conn.clone();
    second_conn->authenticate("@admin", "password", "PLAIN");
    second_conn->selectBucket("bucket");

    // Store the document I want to fetch
    Document document;
    document.info.id = name;
    document.info.flags = 0xdeadbeef;
    document.info.cas = mcbp::cas::Wildcard;
    document.info.datatype = cb::mcbp::Datatype::Raw;
    // Store a 20MB value in the cache
    document.value.assign(20 * 1024 * 1024, 'b');

    const auto info = conn.mutate(document, Vbid(0), MutationType::Set);
    EXPECT_NE(0, info.cas);

    BinprotGetCommand cmd;
    cmd.setKey(name);

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
                      << e.what() << std::endl;
            exit(EXIT_FAILURE);
        }
    }};

    // Wait until the server filled up all of the socket buffers in the
    // kernel so we don't make any progress when trying to send more data.
    do {
        const auto totalSend = getTotalSent(*second_conn, id);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        if (totalSend == getTotalSent(*second_conn, id)) {
            blocked.store(true);
        }
    } while (!blocked);

    // The socket is blocked so we may delete the bucket
    deleteBucket(*second_conn, "bucket", {});
    client.join();
}

TEST_P(BucketTest, TestListBucket) {
    auto& conn = getAdminConnection();
    auto buckets = conn.listBuckets();
    EXPECT_EQ(1, buckets.size());
    EXPECT_EQ(std::string("default"), buckets[0]);
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

TEST_P(BucketTest, TestNoAutoSelectOfBucketForNormalUser) {
    auto& conn = getAdminConnection();
    conn.createBucket("rbac_test", "", BucketType::Memcached);

    conn = getConnection();
    conn.authenticate("smith", "smithpassword", "PLAIN");
    BinprotGetCommand cmd;
    cmd.setKey(name);
    conn.sendCommand(cmd);
    BinprotResponse response;
    conn.recvResponse(response);
    EXPECT_EQ(cb::mcbp::Status::NoBucket, response.getStatus());

    conn = getAdminConnection();
    conn.deleteBucket("rbac_test");
}

TEST_P(BucketTest, TestListSomeBuckets) {
    auto& conn = getAdminConnection();
    conn.createBucket("bucket-1", "", BucketType::Memcached);
    conn.createBucket("bucket-2", "", BucketType::Memcached);
    conn.createBucket("rbac_test", "", BucketType::Memcached);

    const std::vector<std::string> all_buckets = {"default", "bucket-1",
                                                  "bucket-2", "rbac_test"};
    EXPECT_EQ(all_buckets, conn.listBuckets());

    // Reconnect and authenticate as a user with access to only one of them
    conn = getConnection();
    conn.authenticate("smith", "smithpassword", "PLAIN");
    const std::vector<std::string> expected = {"rbac_test"};
    EXPECT_EQ(expected, conn.listBuckets());

    conn = getAdminConnection();
    conn.deleteBucket("bucket-1");
    conn.deleteBucket("bucket-2");
    conn.deleteBucket("rbac_test");
}

TEST_P(BucketTest, TestBucketIsolationBuckets)
{
    auto& connection = getAdminConnection();

    for (std::size_t ii = 1; ii < cb::limits::TotalBuckets; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        connection.createBucket(ss.str(), "", BucketType::Memcached);
    }

    // I should be able to select each bucket and the same document..
    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = "TestBucketIsolationBuckets";
    doc.value = memcached_cfg.dump();

    for (std::size_t ii = 1; ii < cb::limits::TotalBuckets; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        const auto name = ss.str();
        connection.selectBucket(name);
        connection.mutate(doc, Vbid(0), MutationType::Add);
    }

    connection = getAdminConnection();
    // Delete all buckets
    for (std::size_t ii = 1; ii < cb::limits::TotalBuckets; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        connection.deleteBucket(ss.str());
    }
}

TEST_P(BucketTest, TestMemcachedBucketBigObjects)
{
    auto& connection = getAdminConnection();

    const size_t item_max_size = 2 * 1024 * 1024; // 2MB
    std::string config = "item_size_max=" + std::to_string(item_max_size);

    ASSERT_NO_THROW(connection.createBucket(
            "mybucket_000", config, BucketType::Memcached));
    connection.selectBucket("mybucket_000");

    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.datatype = cb::mcbp::Datatype::Raw;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    // Unfortunately the item_max_size is the full item including the
    // internal headers (this would be the key and the hash_item struct).
    doc.value.resize(item_max_size - name.length() - 100);

    connection.mutate(doc, Vbid(0), MutationType::Add);
    connection.get(name, Vbid(0));
    connection.deleteBucket("mybucket_000");
}

TEST_P(BucketTest, SelectNoBucket) {
    auto& connection = getAdminConnection();
    connection.selectBucket("default");
    connection.selectBucket("@no bucket@");
    try {
        connection.get("foo", Vbid(0));
        FAIL() << "We should get " + to_string(cb::mcbp::Status::NoBucket);
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::NoBucket, error.getReason());
    }
}
