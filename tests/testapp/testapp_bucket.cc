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
#include "testapp_bucket.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <thread>

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        BucketTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(BucketTest, TestNameTooLong) {
    auto& connection = getConnection();
    std::string name;
    name.resize(101);
    std::fill(name.begin(), name.end(), 'a');

    try {
        connection.createBucket(name, "", Greenstack::BucketType::Memcached);
        FAIL() << "Invalid bucket name is not refused";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
    }
}

TEST_P(BucketTest, TestMaxNameLength) {
    auto& connection = getConnection();
    std::string name;
    name.resize(100);
    std::fill(name.begin(), name.end(), 'a');

    connection.createBucket(name, "", Greenstack::BucketType::Memcached);
    connection.deleteBucket(name);
}

TEST_P(BucketTest, TestEmptyName) {
    auto& connection = getConnection();

    if (connection.getProtocol() == Protocol::Greenstack) {
        // libgreenstack won't allow us to send such packets
        return;
    }

    try {
        connection.createBucket("", "", Greenstack::BucketType::Memcached);
        FAIL() << "Empty bucket name is not refused";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
    }
}

TEST_P(BucketTest, TestInvalidCharacters) {
    auto& connection = getConnection();

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
            connection.createBucket(name, "", Greenstack::BucketType::Memcached);
            connection.deleteBucket(name);
        } else {
            try {
                connection.createBucket(name, "",
                                        Greenstack::BucketType::Memcached);
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
    auto& connection = getConnection();

    int ii;
    try {
        for (ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
            std::string name = "bucket-" + std::to_string(ii);
            connection.createBucket(name, "", Greenstack::BucketType::Memcached);
        }
    } catch (ConnectionError& ex) {
        FAIL() << "Failed to create more than " << ii << " buckets";
    }

    for (--ii; ii > 0; --ii) {
        std::string name = "bucket-" + std::to_string(ii);
        connection.deleteBucket(name);
    }
}

TEST_P(BucketTest, TestCreateBucketAlreadyExists) {
    auto& conn = getConnection();
    try {
        conn.createBucket("default", "", Greenstack::BucketType::Memcached);
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.getReason();
    }
}

TEST_P(BucketTest, TestDeleteNonexistingBucket) {
    auto& conn = getConnection();
    try {
        conn.deleteBucket("ItWouldBeSadIfThisBucketExisted");
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.getReason();
    }
}

// Regression test for MB-19756 - if a bucket delete is attempted while there
// is connection in the conn_nread state, then delete will hang.
TEST_P(BucketTest, MB19756TestDeleteWhileClientConnected) {
    auto& conn = getConnection();
    conn.createBucket("bucket", "", Greenstack::BucketType::Memcached);

    auto second_conn = conn.clone();
    second_conn->selectBucket("bucket");

    // We need to get the second connection sitting the `conn_nread` state in
    // memcached - i.e. waiting to read a variable-amount of data from the
    // client. Simplest is to perform a GET where we don't send the full key
    // length, by only sending a partial frame
    Frame frame = second_conn->encodeCmdGet("dummy_key_which_we_will_crop", 0);
    second_conn->sendPartialFrame(frame, frame.payload.size() - 1);

    // Once we call deleteBucket below, it will hang forever (if the bug is
    // present), so we need a watchdog thread which will send the remainder
    // of the GET frame to un-stick bucket deletion. If the watchdog fires
    // the test has failed.
    std::mutex cv_m;
    std::condition_variable cv;
    std::atomic<bool> bucket_deleted{false};
    std::atomic<bool> watchdog_fired{false};
    std::thread watchdog{
        [&second_conn, &frame, &cv_m, &cv, &bucket_deleted,
         &watchdog_fired]() {
            std::unique_lock<std::mutex> lock(cv_m);
            cv.wait_for(lock, std::chrono::seconds(5),
                        [&bucket_deleted](){return bucket_deleted == true;});
            watchdog_fired = true;
            second_conn->sendFrame(frame);
        }
    };

    conn.deleteBucket("bucket");
    // Check that the watchdog didn't fire.
    EXPECT_FALSE(watchdog_fired) <<
            "Bucket deletion (with connected client in conn_nread) only "
            "completed after watchdog fired";

    // Cleanup - stop the watchdog (if it hasn't already fired).
    bucket_deleted = true;
    cv.notify_one();
    watchdog.join();
}

// Regression test for MB-19981 - if a bucket delete is attempted while there
// is connection in the conn_nread state.  And that connection is currently
// blocked waiting for a response from the server; the connection will not
// have an event registered in libevent.  Therefore a call to updateEvent
// will fail.

// Note before the fix, if the event_active function call is removed from the
// signalIfIdle function the test will hang.  The reason the test works with
// the event_active function call in place is that the event_active function
// can be invoked regardless of whether the event is registered
// (i.e. in a pending state) or not.
TEST_P(BucketTest, MB19981TestDeleteWhileClientConnectedAndEWouldBlocked) {
    auto& conn = getConnection();
    conn.createBucket("bucket", "default_engine.so",
                      Greenstack::BucketType::EWouldBlock);
    auto second_conn = conn.clone();
    second_conn->authenticate("_admin", "password", "PLAIN");
    second_conn->selectBucket("bucket");
    auto connection = conn.clone();
    connection->authenticate("_admin", "password", "PLAIN");

    auto cwd = cb::io::getcwd();
    auto testfile = cwd + "/" + cb::io::mktemp("lockfile");

    // Configure so that the engine will return ENGINE_EWOULDBLOCK and
    // not process any operation given to it.  This means the connection
    // will remain in a blocked state.
    second_conn->configureEwouldBlockEngine(EWBEngineMode::BlockMonitorFile,
                                            ENGINE_EWOULDBLOCK /* unused */,
                                            0,
                                            testfile);

    Frame frame = second_conn->encodeCmdGet("dummy_key_where_never_return", 0);

    // Send the get operation, however we will not get a response from the
    // engine, and so it will block indefinately.
    second_conn->sendFrame(frame);
    std::thread resume{
        [&connection, &testfile]() {
            // wait until we've started to delete the bucket
            bool deleting = false;
            while (!deleting) {
                usleep(10);  // Avoid busy-wait ;-)
                auto details = connection->stats("bucket_details");
                auto* obj = cJSON_GetObjectItem(details.get(), "bucket details");
                unique_cJSON_ptr buckets(cJSON_Parse(obj->valuestring));
                for (auto* b = buckets->child->child; b != nullptr; b = b->next) {
                    auto *name = cJSON_GetObjectItem(b, "name");
                    if (name != nullptr) {
                        if (std::string(name->valuestring) == "bucket") {
                            auto *state = cJSON_GetObjectItem(b, "state");
                            if (std::string(state->valuestring) == "destroying") {
                                deleting = true;
                            }
                        }
                    }
                }
            }

            // resume the connection
            cb::io::rmrf(testfile);
        }
    };

    // On a different connection we now instruct the bucket to be deleted.
    // The connection that is currently blocked needs to be sent a fake
    // event to allow the connection to be closed.
    conn.deleteBucket("bucket");

    resume.join();
}

// Strictly speaking this test /should/ work on Windows, however the
// issue we hit is that the memcached connection send buffer on
// Windows is huge (256MB in my testing) and so we timeout long before
// we manage to fill the buffer with the tiny DCP packets we use (they
// have to be small so we totally fill it).
// Therefore disabling this test for now.
#if !defined(WIN32)
TEST_P(BucketTest, MB19748TestDeleteWhileConnShipLogAndFullWriteBuffer) {
    // TODO: Remove this whenhttps://issues.couchbase.com/browse/MB-22413 is
    // resolved.
    if (getenv("TESTAPP_SKIP_HANGING_TEST") && *getenv("TESTAPP_SKIP_HANGING_TEST")) {
        std::cerr <<
                "Skipping MB19748TestDeleteWhileConnShipLogAndFullWriteBuffer (might hang!)" <<
                std::endl;
        return;
    }
    auto& conn = getConnection();

    auto second_conn = conn.clone();
    second_conn->authenticate("_admin", "password", "PLAIN");
    auto* mcbp_conn = dynamic_cast<MemcachedBinprotConnection*>(second_conn.get());


    conn.createBucket("bucket", "default_engine.so", Greenstack::BucketType::EWouldBlock);
    second_conn->selectBucket("bucket");

    // We need to get into the `conn_ship_log` state, and then fill up the
    // connections' write (send) buffer.

    BinprotDcpOpenCommand dcp_open_command("ewb_internal", 0,
                                           DCP_OPEN_PRODUCER);
    mcbp_conn->sendCommand(dcp_open_command);

    BinprotDcpStreamRequestCommand dcp_stream_request_command;
    mcbp_conn->sendCommand(dcp_stream_request_command);

    // Now need to wait for the for the write (send) buffer of
    // second_conn to fill in memcached. There's no direct way to
    // check this from second_conn itself; and even if we examine the
    // connections' state via a `connections` stats call there isn't
    // any explicit state we can measure - basically the "kernel sendQ
    // full" state is indistinguishable from "we have /some/ amount of
    // data outstanding". We also can't get access to the current
    // sendQ size in any portable way. Therefore we 'infer' the sendQ
    // is full by sampling the "total_send" statistic and when it
    // stops changing we assume the buffer is full.

    // This isn't foolproof (a really slow machine would might look
    // like it's full), but it is the best I can think of :/

    // Assume that we'll see traffic at least every 500ms.
    for (int previous_total_send = -1;
         ;
         std::this_thread::sleep_for(std::chrono::milliseconds(500))) {
        // Get stats for all connections, then locate this connection
        // - should be the one with dcp:true.
        auto all_stats = conn.stats("connections");
        unique_cJSON_ptr my_conn_stats;
        for (size_t ii{0}; my_conn_stats.get() == nullptr; ii++) {
            auto* conn_stats = cJSON_GetObjectItem(all_stats.get(),
                                                   std::to_string(ii).c_str());
            if (conn_stats == nullptr) {
                // run out of connections.
                break;
            }
            // Each value is a string containing escaped JSON.
            unique_cJSON_ptr conn_json{cJSON_Parse(conn_stats->valuestring)};
            auto* dcp_flag = cJSON_GetObjectItem(conn_json.get(), "dcp");
            if (dcp_flag != nullptr && dcp_flag->type == cJSON_True) {
                my_conn_stats.swap(conn_json);
            }
        }

        if (my_conn_stats.get() == nullptr) {
            // Connection isn't in DCP state yet (we are racing here with
            // processing messages on second_conn). Retry on next iteration.
            continue;
        }

        // Check how many bytes have been sent and see if it is
        // unchanged from the previous sample.
        auto* total_send = cJSON_GetObjectItem(my_conn_stats.get(),
                                               "total_send");
        ASSERT_NE(nullptr, total_send)
            << "Missing 'total_send' field in connection stats";

        if (total_send->valueint == previous_total_send) {
            // Unchanged - assume sendQ is now full.
            break;
        }

        previous_total_send = total_send->valueint;
    };

    // Once we call deleteBucket below, it will hang forever (if the bug is
    // present), so we need a watchdog thread which will write more data to
    // the connection; triggering a READ event in libevent and hence causing
    // the connection's state machine to be advanced (and connection closed).
    std::mutex cv_m;
    std::condition_variable cv;
    std::atomic<bool> bucket_deleted{false};
    std::atomic<bool> watchdog_fired{false};
    std::thread watchdog{
        [&second_conn, &cv_m, &cv, &bucket_deleted,
         &watchdog_fired]() {
            std::unique_lock<std::mutex> lock(cv_m);
            cv.wait_for(lock, std::chrono::seconds(5),
                        [&bucket_deleted](){return bucket_deleted == true;});
            watchdog_fired = true;
            auto frame = second_conn->encodeCmdGet("wakeup_conn", 0);
            try {
                second_conn->sendFrame(frame);
            } catch (std::runtime_error& ) {
                // It is ok for sendFrame to fail - the connection might have
                // been closed by the server due to the bucket deletion.
            }
        }
    };

    conn.deleteBucket("bucket");

    // Check that the watchdog didn't fire.
    EXPECT_FALSE(watchdog_fired)
        << "Bucket deletion (with connected client in conn_ship_log and full "
           "sendQ) only completed after watchdog fired";

    // Cleanup - stop the watchdog (if it hasn't already fired).
    bucket_deleted = true;
    cv.notify_one();
    watchdog.join();
}
#endif

TEST_P(BucketTest, TestListBucket) {
    auto& conn = getConnection();
    auto buckets = conn.listBuckets();
    EXPECT_EQ(1, buckets.size());
    EXPECT_EQ(std::string("default"), buckets[0]);
}


TEST_P(BucketTest, TestBucketIsolationBuckets)
{
    auto& connection = getAdminConnection();

    for (int ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        connection.createBucket(ss.str(), "", Greenstack::BucketType::Memcached);
    }

    // I should be able to select each bucket and the same document..
    Document doc;
    doc.info.cas = Greenstack::CAS::Wildcard;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Raw;
    doc.info.flags = 0xcaffee;
    doc.info.id = "TestBucketIsolationBuckets";
    char* ptr = cJSON_Print(memcached_cfg.get());
    std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
    cJSON_Free(ptr);

    for (int ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        std::stringstream ss;
        ss << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        const auto name = ss.str();
        connection.selectBucket(name);
        connection.mutate(doc, 0, Greenstack::MutationType::Add);
    }

    connection = getAdminConnection();
    // Delete all buckets
    for (int ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
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

    ASSERT_NO_THROW(connection.createBucket("mybucket_000",
                                            config,
                                            Greenstack::BucketType::Memcached));
    connection.selectBucket("mybucket_000");

    Document doc;
    doc.info.cas = Greenstack::CAS::Wildcard;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Raw;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    // Unfortunately the item_max_size is the full item including the
    // internal headers (this would be the key and the hash_item struct).
    doc.value.resize(item_max_size - name.length() - 100);

    connection.mutate(doc, 0, Greenstack::MutationType::Add);
    connection.get(name, 0);
    connection.deleteBucket("mybucket_000");
}

MemcachedConnection& BucketTest::getConnection() {
    auto& conn = TestappClientTest::getConnection();
    conn.authenticate("_admin", "password", "PLAIN");
    return conn;
}
