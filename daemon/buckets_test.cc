/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "buckets.h"
#include "enginemap.h"
#include "front_end_thread.h"
#include "memcached.h"
#include "stats.h"
#include "tests/mcbp/mcbp_mock_connection.h"
#include "utilities/testing_hook.h"
#include <boost/thread/barrier.hpp>
#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>
#include <future>

using namespace std::string_view_literals;

TEST(BucketTest, Reset) {
    // Attempt to spot when new members are added to Bucket and the reset()
    // method and/or this test have not been updated.
    // Bucket size varies depending on arch / platform ABI alignment rules,
    // check the main ones we run against.
    static constexpr size_t expectedBucketSize =
#if defined(__linux) && defined(__x86_64__)
            5784;
#elif defined(__APPLE__)
            5880;
#else
            0;
#endif
    if (expectedBucketSize) {
        ASSERT_EQ(expectedBucketSize, sizeof(Bucket))
                << "Bucket size changed, the reset test must be updated with "
                   "the new members";
    }

    class MockBucket : public Bucket {
    public:
        void testReset() {
            throttle_gauge.increment(5);
            throttle_limit = 1;
            num_throttled = 1;
            throttle_wait_time = 1;
            num_commands = 1;
            num_commands_with_metered_units = 1;
            num_metered_dcp_messages = 1;
            num_rejected = 1;
            bucket_quota_exceeded = true;
            pause_cancellation_source = folly::CancellationSource{};

            reset();
            EXPECT_EQ(std::numeric_limits<std::size_t>::max(), throttle_limit);
            EXPECT_EQ(0, num_throttled);
            EXPECT_EQ(0, throttle_wait_time);
            EXPECT_EQ(0, num_commands);
            EXPECT_EQ(0, num_commands_with_metered_units);
            EXPECT_EQ(0, num_metered_dcp_messages);
            EXPECT_EQ(0, num_rejected);
            EXPECT_EQ(0, throttle_gauge.getValue());
            EXPECT_FALSE(bucket_quota_exceeded);
            EXPECT_FALSE(pause_cancellation_source.canBeCancelled());
        }
    } bucket;

    bucket.testReset();
}

class BucketManagerTest : public ::testing::Test, public BucketManager {
public:
    void SetUp() override {
        for (std::size_t ii = 1; ii < all_buckets.size(); ++ii) {
            all_buckets[ii].reset();
        }
    }

    cb::engine_errc public_resume(std::string_view cid, std::string_view name) {
        return BucketManager::resume(cid, name);
    }

    void testPauseBucketCancellable(bool threaded,
                                    std::string_view expectedPhase);

protected:
    void bucketTypeChangeListener(Bucket& bucket, BucketType type) override {
        bucketTypeChangeListenerFunc(bucket, type);
    }
    void bucketStateChangeListener(Bucket& bucket,
                                   Bucket::State state) override {
        bucketStateChangeListenerFunc(bucket, state);
    }

    void bucketPausingListener(std::string_view bucket,
                               std::string_view phase) override {
        bucketPausingListenerFunc(bucket, phase);
    }

    TestingHook<Bucket&, BucketType> bucketTypeChangeListenerFunc;
    TestingHook<Bucket&, Bucket::State> bucketStateChangeListenerFunc;
    TestingHook<std::string_view, std::string_view> bucketPausingListenerFunc;
};

TEST_F(BucketManagerTest, AllocateBucket) {
    // Allocate a bucket
    auto validateAllocatedBucket = [](std::string_view name, Bucket* bucket) {
        ASSERT_NE(nullptr, bucket);
        EXPECT_EQ(name, bucket->name);
        EXPECT_EQ(BucketType::Unknown, bucket->type);
        EXPECT_EQ(Bucket::State::Creating, bucket->state);
    };

    auto [err, bucket] = allocateBucket("foo");
    ASSERT_EQ(cb::engine_errc::success, err);
    validateAllocatedBucket("foo"sv, bucket);

    // If we try to allocate a new one we would get temporary failure
    // as we already have a call running
    {
        auto [err2, bucket2] = allocateBucket("foo");
        EXPECT_EQ(cb::engine_errc::temporary_failure, err2);
        EXPECT_EQ(nullptr, bucket2);
    }

    // But if we progress the management command we would an error that it
    // already exists
    {
        bucket->management_operation_in_progress = false;
        bucket->state = Bucket::State::Ready;

        auto [err2, bucket2] = allocateBucket("foo");
        EXPECT_EQ(cb::engine_errc::key_already_exists, err2);
        EXPECT_EQ(nullptr, bucket2);
    }

    // But if it is a ClusterConfigOnly bucket it should be allowed to
    // grab that slot
    {
        bucket->type = BucketType::ClusterConfigOnly;
        auto [err2, bucket2] = allocateBucket("foo");
        EXPECT_EQ(cb::engine_errc::success, err2);
        EXPECT_EQ(bucket, bucket2);
        // And the state should not have been changed
        EXPECT_EQ(Bucket::State::Ready, bucket->state);
    }

    // We should be able to create up to the max number of buckets:
    // Slot 0 == no bucket and slot 1 is what I created above
    for (std::size_t ii = 2; ii < all_buckets.size(); ++ii) {
        auto [e, b] = allocateBucket(std::to_string(ii));
        ASSERT_EQ(cb::engine_errc::success, e);
        validateAllocatedBucket(std::to_string(ii), b);
    }

    // The bucket array is full and trying to allocate a new slot should
    // fail
    auto [e, b] = allocateBucket("fail");
    ASSERT_EQ(cb::engine_errc::too_big, e);
    ASSERT_EQ(nullptr, b);
}

/// Verify that we set bucket transitions
TEST_F(BucketManagerTest, CreateBucket) {
    bool creating = false;
    bool initializing = false;
    bool ready = false;
    bucketStateChangeListenerFunc = [&creating, &initializing, &ready](
                                            Bucket& bucket,
                                            Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Pausing:
        case Bucket::State::Paused:
        case Bucket::State::Destroying:
            FAIL() << "Unexpected callback";
        case Bucket::State::Creating:
            creating = true;
            EXPECT_EQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::Unknown, bucket.type);
            EXPECT_EQ(Bucket::State::None, bucket.state);
            EXPECT_FALSE(bucket.hasEngine());
            EXPECT_EQ(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supportedFeatures.empty());
            EXPECT_TRUE(bucket.management_operation_in_progress);
            break;
        case Bucket::State::Initializing:
            initializing = true;
            EXPECT_EQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::Unknown, bucket.type);
            EXPECT_EQ(Bucket::State::Creating, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_NE(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supportedFeatures.empty());
            EXPECT_TRUE(bucket.management_operation_in_progress);
            break;
        case Bucket::State::Ready:
            ready = true;
            EXPECT_EQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::NoBucket, bucket.type);
            EXPECT_EQ(Bucket::State::Initializing, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_NE(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supports(cb::engine::Feature::Collections));
            EXPECT_FALSE(bucket.management_operation_in_progress);
            break;
        }
    };
    auto err = create(1, "mybucket", {}, BucketType::NoBucket);
    EXPECT_EQ(cb::engine_errc::success, err);
    EXPECT_TRUE(creating) << "Expected callback for state Creating";
    EXPECT_TRUE(initializing) << "Expected callback for state Initializing";
    EXPECT_TRUE(ready) << "Expected callback for state Ready";
}

TEST_F(BucketManagerTest, AssociateBucket) {
    EXPECT_EQ(cb::engine_errc::success,
              create(1, "mybucket", {}, BucketType::NoBucket));

    auto& bucket = at(1);
    EXPECT_EQ("mybucket", bucket.name);
    // Associate with the bucket
    EXPECT_TRUE(tryAssociateBucket(&bucket.getEngine()));
    auto future = std::async(std::launch::async, [&]() {
        // Wait to get in destroying state.
        while (bucket.state != Bucket::State::Destroying) {
            std::this_thread::yield();
        }
        // Then assert that we are waiting for something for more than 10ms
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (bucket.state != Bucket::State::Destroying) {
            throw std::runtime_error("Test failed");
        }
        // Finally, disassociate the bucket to allow it to be destroyed.
        disassociateBucket(&bucket);
    });
    EXPECT_EQ(cb::engine_errc::success,
              destroy("1", "mybucket", /* force */ true, {}));
    future.wait();
}

/// Verify the that everything is good to go before trying to set the bucket
/// type when promoting a cluster config only bucket to a real bucket
TEST_F(BucketManagerTest, PromoteClusterConfigOnlyBucket) {
    auto [e, b] = allocateBucket("mybucket");
    ASSERT_EQ(cb::engine_errc::success, e);
    ASSERT_NE(nullptr, b);
    b->type = BucketType::ClusterConfigOnly;
    b->state = Bucket::State::Ready;
    b->management_operation_in_progress = false;

    bool typecallback = false;
    bucketTypeChangeListenerFunc = [&typecallback](Bucket& bucket,
                                                   BucketType type) {
        typecallback = true;
        EXPECT_EQ("mybucket", bucket.name);
        EXPECT_EQ(Bucket::State::Ready, bucket.state);
        EXPECT_TRUE(bucket.hasEngine());
        EXPECT_NE(nullptr, bucket.getDcpIface());
        EXPECT_EQ(default_max_item_size, bucket.max_document_size);
        EXPECT_TRUE(bucket.supports(cb::engine::Feature::Collections));
        EXPECT_TRUE(bucket.management_operation_in_progress);
        EXPECT_EQ(BucketType::ClusterConfigOnly, bucket.type);
    };

    bool ready = false;
    bucketStateChangeListenerFunc = [&ready](Bucket& bucket,
                                             Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Destroying:
        case Bucket::State::Creating:
        case Bucket::State::Initializing:
        case Bucket::State::Pausing:
        case Bucket::State::Paused:
            FAIL() << "Unexpected callback";
            break;
        case Bucket::State::Ready:
            ready = true;
            EXPECT_EQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::NoBucket, bucket.type);
            EXPECT_EQ(Bucket::State::Ready, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_NE(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supports(cb::engine::Feature::Collections));
            EXPECT_FALSE(bucket.management_operation_in_progress);
            break;
        }
    };
    auto err = create(1, "mybucket", {}, BucketType::NoBucket);
    EXPECT_EQ(cb::engine_errc::success, err);
    EXPECT_TRUE(ready) << "Expected a callback for enter ready state";
    EXPECT_TRUE(typecallback) << "Expected callback for setting type";
}

/// Verify that we roll a failed bucket creation into a bucket deletion
TEST_F(BucketManagerTest, FailingPromoteClusterConfigOnlyBucket) {
    auto [e, b] = allocateBucket("mybucket");
    ASSERT_EQ(cb::engine_errc::success, e);
    ASSERT_NE(nullptr, b);
    b->type = BucketType::ClusterConfigOnly;
    b->state = Bucket::State::Ready;
    b->management_operation_in_progress = false;

    bucketTypeChangeListenerFunc = [](Bucket& bucket, BucketType type) {
        throw std::bad_alloc();
    };

    bool destroying = false;
    bucketStateChangeListenerFunc = [&destroying](Bucket& bucket,
                                                  Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Creating:
        case Bucket::State::Initializing:
        case Bucket::State::Ready:
        case Bucket::State::Pausing:
        case Bucket::State::Paused:
            FAIL() << "Unexpected callback";
            break;
        case Bucket::State::Destroying:
            destroying = true;
            break;
        }
    };
    auto err = create(1, "mybucket", {}, BucketType::NoBucket);
    EXPECT_EQ(cb::engine_errc::not_stored, err);
    EXPECT_TRUE(destroying) << "Expected a state callback to destroying";
}

TEST_F(BucketManagerTest, DestroyEnoent) {
    EXPECT_EQ(cb::engine_errc::no_such_key,
              destroy("<none>", "DestroyEnoent", true, {}));
}

TEST_F(BucketManagerTest, DestroyEinprogress) {
    auto [e, bucket] = allocateBucket("DestroyEinprogress");
    ASSERT_EQ(cb::engine_errc::success, e);
    ASSERT_NE(nullptr, bucket);
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              destroy("<none>", "DestroyEinprogress", true, {}));
    bucket->management_operation_in_progress = false;
    bucket->state = Bucket::State::None;
}

TEST_F(BucketManagerTest, DestroyInvalidState) {
    auto [e, bucket] = allocateBucket("DestroyInvalidState");
    ASSERT_EQ(cb::engine_errc::success, e);
    ASSERT_NE(nullptr, bucket);
    bucket->management_operation_in_progress = false;
    EXPECT_EQ(Bucket::State::Creating, bucket->state);
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              destroy("<none>", "DestroyInvalidState", true, {}));
    bucket->state = Bucket::State::None;
}

/// Verify that we perform correct transitions when pausing a bucket.
TEST_F(BucketManagerTest, PauseBucket) {
    // Require a bucket type which supports pause() - i.e. Memcached or
    // Couchbase; using the former as simpler to spin up.
    auto err = create(1, "mybucket", {}, BucketType::Memcached);
    ASSERT_EQ(cb::engine_errc::success, err);

    bool pausing = false;
    bool paused = false;
    bucketStateChangeListenerFunc = [&pausing, &paused](Bucket& bucket,
                                                        Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Initializing:
        case Bucket::State::Ready:
        case Bucket::State::Destroying:
        case Bucket::State::Creating:
            FAIL() << "Unexpected callback for state:" << to_string(state);
            break;
        case Bucket::State::Pausing:
            pausing = true;
            EXPECT_EQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::Memcached, bucket.type);
            EXPECT_EQ(Bucket::State::Ready, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_TRUE(bucket.management_operation_in_progress);
            break;
        case Bucket::State::Paused:
            paused = true;
            EXPECT_EQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::Memcached, bucket.type);
            EXPECT_EQ(Bucket::State::Pausing, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_FALSE(bucket.management_operation_in_progress);
            break;
        }
    };
    err = pause("1", "mybucket");
    EXPECT_EQ(cb::engine_errc::success, err);
    EXPECT_TRUE(pausing) << "Expected callback for state Pausing";
    EXPECT_TRUE(paused) << "Expected callback for state Paused";

    // Cleanup
    bucketStateChangeListenerFunc.reset();
    ASSERT_EQ(cb::engine_errc::success, destroy("1", "mybucket", false, {}));
    shutdown_all_engines();
}

/**
 * Verify that a pause() operation can be cancelled by calling resume() while
 * in the pausing state, before setting up the cancellation callback. This
 * exercises the case where the cancellationCallback's ctor is run inline on
 * the calling thread (as cancellation has already been requested).
 */
TEST_F(BucketManagerTest, PauseBucketCancellableBeforeCallback) {
    testPauseBucketCancellable(false, "before_cancellation_callback");
}

/// Multi-threaded version of previous test - the resume occurs in a different
/// thread, so simulate a different client connection issuing it.
TEST_F(BucketManagerTest, PauseBucketCancellableBeforeCallbackThreaded) {
    testPauseBucketCancellable(true, "before_disconnect");
}

/// As above, but cancelling the pause just before we wait for connections
/// to disconnect.
TEST_F(BucketManagerTest, PauseBucketCancellableBeforeDisconnect) {
    testPauseBucketCancellable(false, "before_disconnect");
}

/// As above, but cancelling the pause just before we wait for connections
/// to disconnect, threaded version.
TEST_F(BucketManagerTest, PauseBucketCancellableBeforeDisconnectThreaded) {
    testPauseBucketCancellable(true, "before_disconnect");
}

/// As above, but cancelling the pause just before we call into the engine.
TEST_F(BucketManagerTest, PauseBucketEngineCancellable) {
    testPauseBucketCancellable(false, "before_engine_pause");
}

/// As above, but cancelling the pause just before we call into the engine.
TEST_F(BucketManagerTest, PauseBucketEngineCancellableThreaded) {
    testPauseBucketCancellable(true, "before_engine_pause");
}

void BucketManagerTest::testPauseBucketCancellable(
        bool threaded, std::string_view expectedPhase) {
    // Require a bucket type which supports pause() - i.e. Memcached or
    // Couchbase; using the former as simpler to spin up.
    auto err = create(1, "mybucket", {}, BucketType::Memcached);
    ASSERT_EQ(cb::engine_errc::success, err);

    FrontEndThread thread;
    McbpMockConnection conn{thread};

    // When cancelling the pause before disconnect, we want at least one
    // client connection associated with the bucket being paused - that
    // exercises the code which checks for cancellation inside
    // waitForEveryoneToDisconnect().
    // However, due to limitations in the BucketManagerTest harness, if we
    // associate a connection and _don't_ cancel before getting further into
    // waitForEveryoneToDisconnect() then the test will hang (as there's no
    // front-end thread to actually disconnect - as such only associate
    // if we know we are going to cancel before we properly wait.
    if (expectedPhase == "before_disconnect") {
        associate_bucket(conn, "mybucket");
    }

    bool pausing = false;
    bool ready = true;
    bucketStateChangeListenerFunc = [&pausing, &ready](Bucket& bucket,
                                                       Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Initializing:
        case Bucket::State::Destroying:
        case Bucket::State::Creating:
            FAIL() << "Unexpected callback for state:" << to_string(state);
            break;
        case Bucket::State::Pausing:
            pausing = true;
            break;
        case Bucket::State::Paused:
            FAIL() << "Unexpected callback for 'Paused': bucket should have "
                      "resumed instead of changing to paused after resume()";
            break;
        case Bucket::State::Ready:
            ready = true;
        }
    };

    // In threaded mode we use a background thread to perform the resume.
    // Prepare it here - we want it to already exist before pause() is initially
    // called.
    folly::Baton baton1;
    std::thread resumeThread{[threaded, &baton1, &testFixture = *this]() {
        if (!threaded) {
            return;
        }
        baton1.wait();
        testFixture.public_resume("2", "mybucket");
    }};

    bucketPausingListenerFunc = [&testFixture = *this,
                                 threaded,
                                 expectedPhase,
                                 &baton1,
                                 &resumeThread](std::string_view bucket,
                                                std::string_view phase) {
        // Ignore if not the expected phase.
        if (phase != expectedPhase) {
            return;
        }
        // When changes to Pausing, issue a resume() request which
        // should
        // cancel the pause.
        if (threaded) {
            // Wake waiting bg thread to perform the resume, then block
            // until thread has performed resume.
            baton1.post();
            resumeThread.join();
        } else {
            testFixture.public_resume("2", "mybucket");
        }
    };

    err = pause("1", "mybucket");
    EXPECT_EQ(cb::engine_errc::cancelled, err);
    EXPECT_TRUE(pausing) << "Expected callback for state Pausing";
    EXPECT_TRUE(ready)
            << "Expected callback for state Ready (after resume() called)";

    // Cleanup
    if (!threaded) {
        // (In threaded mode we already joined the resumeThread in pausing
        // listener.)
        resumeThread.join();
    }
    bucketStateChangeListenerFunc.reset();
    if (expectedPhase == "before_disconnect") {
        disassociate_bucket(conn);
    }
    destroy("1", "mybucket", false, {});
    shutdown_all_engines();
}

/// Basic smoke test to see what happens if two threads both attempt to pause/
/// resume simulataneously....
TEST_F(BucketManagerTest, PauseResumeFight) {
    // Require a bucket type which supports pause() - i.e. Memcached or
    // Couchbase; using the former as simpler to spin up.
    auto err = create(1, "mybucket", {}, BucketType::Memcached);
    ASSERT_EQ(cb::engine_errc::success, err);

    FrontEndThread thread;
    McbpMockConnection conn{thread};

    // Use a barrier which both threads must rendezvous via to maximise the
    // contention we get between threads.
    boost::barrier barrier{2};
    const int iterations = 1000;
    auto pauseResumeNTimes = [&](std::string_view connectionId) {
        barrier.count_down_and_wait();
        for (int i = 0; i < iterations; i++) {
            // Yield thread between each operation to attempt to get more
            // interleaving with the other thread.
            pause(connectionId, "mybucket");
            std::this_thread::yield();
            public_resume(connectionId, "mybucket");
            std::this_thread::yield();
        }
    };

    std::thread worker1{pauseResumeNTimes, "1"};
    std::thread worker2{pauseResumeNTimes, "2"};

    worker1.join();
    worker2.join();

    destroy("1", "mybucket", false, {});
    shutdown_all_engines();
}
