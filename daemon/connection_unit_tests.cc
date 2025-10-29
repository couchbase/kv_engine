/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "connection_libevent.h"
#include "daemon/external_auth_manager_thread.h"
#include "enginemap.h"
#include "front_end_thread.h"
#include "log_macros.h"
#include "memcached.h"

#include <daemon/cookie.h>
#include <platform/socket.h>

#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>

/// A mock connection
class MockConnection : public LibeventConnection {
public:
    explicit MockConnection(FrontEndThread& frontEndThread)
        : LibeventConnection(frontEndThread) {
        constexpr auto options = BEV_OPT_UNLOCK_CALLBACKS |
                                 BEV_OPT_CLOSE_ON_FREE |
                                 BEV_OPT_DEFER_CALLBACKS;
        bev.reset(bufferevent_socket_new(
                frontEndThread.eventBase.getLibeventBase(), -1, options));
        bufferevent_setcb(bev.get(),
                          LibeventConnection::rw_callback,
                          LibeventConnection::rw_callback,
                          LibeventConnection::event_callback,
                          this);

        bufferevent_enable(bev.get(), EV_READ);
    }

    /// Simulates a connection reset.
    void mockConnReset() {
        EVUTIL_SET_SOCKET_ERROR(ECONNRESET);
        event_callback(BEV_EVENT_EOF | BEV_EVENT_ERROR);
    }

    /// Allocates a cookie.
    Cookie& allocateCookie() {
        cookies.emplace_back(std::make_unique<Cookie>(*this));
        return *cookies.back();
    }

    /// Returns the current state of the connection.
    auto getState() const {
        return state;
    }
};

class MockFrontEndThread : public FrontEndThread {
public:
    /// Adds a mock connection to the front end thread.
    MockConnection& addMockConnection() {
        auto connection = std::make_unique<MockConnection>(*this);
        auto* conn = connection.get();
        add_connection(std::move(connection));
        return *conn;
    }

    void removeMockConnection(MockConnection& connection) {
        destroy_connection(connection);
    }
};

class ConnectionUnitTests : public ::testing::Test {
public:
    static void SetUpTestCase() {
        cb::logger::createBlackholeLogger();
        // Initialize the RBAC subsystem.
        cb::rbac::initialize();
        if (!externalAuthManager) {
            // Initialize the external authentication manager.
            externalAuthManager = std::make_unique<ExternalAuthManagerThread>();
        }
        // Make sure that we have the buckets submodule initialized and
        // have no-bucket available.
        BucketManager::instance();
    }

    void SetUp() override {
        frontEndThread = std::make_unique<MockFrontEndThread>();
        connection = &frontEndThread->addMockConnection();
    }

    void TearDown() override {
        try {
            frontEndThread->removeMockConnection(*connection);
        } catch (const std::logic_error&) {
            // Mock connection was already removed.
        }
        connection = nullptr;
        frontEndThread.reset();
    }

    static void TearDownTestCase() {
        cleanup_buckets();
    }

    void resetConnectionTest(bool emptySendQueue);

protected:
    std::unique_ptr<MockFrontEndThread> frontEndThread;
    MockConnection* connection{nullptr};
};

TEST_F(ConnectionUnitTests, MB43374) {
    EXPECT_STREQ("unknown:0", connection->getConnectionId().data());
    // Verify that the client can't mess up the output by providing "
    connection->setConnectionId(R"(This "is" my life)");
    auto msg = fmt::format("{}", connection->getConnectionId().data());
    EXPECT_EQ("This  is  my life", msg);
}

TEST_F(ConnectionUnitTests, CpuTime) {
    using namespace std::chrono_literals;
    connection->addCpuTime(40ns);
    connection->addCpuTime(30ns);
    connection->addCpuTime(10ns);
    connection->addCpuTime(20ns);

    nlohmann::json json = *connection;
    EXPECT_EQ("10", json["min_sched_time"]);
    EXPECT_EQ("40", json["max_sched_time"]);
    EXPECT_EQ("100", json["total_cpu_time"]);
}

TEST_F(ConnectionUnitTests, NotificationOrder) {
    // Test mimicking what would happen if a frontend thread was the last
    // owner of a ConnHandler. There may be pending callbacks from
    // scheduleDcpStep, and these must execute _before_ the callback scheduled
    // by ~ConnHandler in cookieIface->release(*cookie).

    // Connection::executeCommandsCallback may record stats,
    // make sure the appropriate histogram exists.
    scheduler_info.resize(1);
    // start a simplified frontend thread
    auto& evb = frontEndThread->eventBase;
    std::thread fakeFrontendThread([&] { evb.loopForever(); });

    auto cookie = std::make_unique<Cookie>(*connection);
    // fake the engine creating a ConnHandler and reserving the cookie
    cookie->reserve();

    folly::Baton<> eventBaseThreadBusy;
    folly::Baton<> callbackOneScheduled;
    folly::Baton<> callbackOneComplete;

    // set up some work running in the event base thread
    evb.runInEventBaseThread([&] {
        eventBaseThreadBusy.post();
        // Wait for the main test thread to schedule callback 1.
        // That way, 1 was _definitely_ scheduled before the cookie
        // release call below.
        callbackOneScheduled.wait();

        // Inside release(), a callback is provided to be run in the event
        // base thread.
        // The expectation is that this will be executed _after_ any previously
        // queued callbacks - e.g., scheduleDcpStep captures a Connection by
        // reference, and this is only safe as long as the cookie refcount
        // is above zero.

        // If this ordering expectation is not met, the cookie could be
        // released, and the connection destroyed, while other queued callbacks
        // still reference the connection.

        // MB-50967: cookie release previously called
        //   eventBase.runInEventBaseThread(...)
        // Normally this function will enqueue the callback in a
        // NotificationQueue.
        // _However_, if the calling thread _is_ the event base thread,
        // the queue may be short circuited and the callback triggered sooner.
        // This would break the expected ordering, and could make later
        // callbacks unsafe.
        // Instead:
        //   eventBase.runInEventBaseThreadAlwaysEnqueue(...)
        // can be called. This will not short circuit if in the event base
        // thread.

        // *Callback 2* queued inside release
        ASSERT_TRUE(evb.inRunningEventBaseThread());
        cookie->release();
    });

    // wait for the event base thread to have started draining the
    // NotificationQueue. This splices out the pending notifications to work on.
    // Anything queued after that will be run in a later loop iteration.
    eventBaseThreadBusy.wait();

    // now schedule a callback which we expect will be executed _before_ the
    // cookie refcount is decreased
    // *Callback 1*
    ASSERT_FALSE(evb.inRunningEventBaseThread());
    evb.runInEventBaseThread([&] {
        // MB-50967: the callback inside `cookie->release()` skipped the queue
        // and was executed before this one, meaning the refcount was zero
        // (and the connection could potentially be destroyed).
        EXPECT_EQ(1, cookie->getRefcount());
        callbackOneComplete.post();
    });
    callbackOneScheduled.post();

    callbackOneComplete.wait();
    evb.terminateLoopSoon();
    fakeFrontendThread.join();

    // confirm that the cookie _was_ actually released
    EXPECT_EQ(0, cookie->getRefcount());
}

/**
 * Tests that a connection is closed promptly when the connection is reset.
 *
 * Bug: When the connection is reset with non-empty send queue, we did not
 * trigger a libevent callback to fully close the connection.
 *
 * @param emptySendQueue Whether the send queue should be empty.
 */
void ConnectionUnitTests::resetConnectionTest(bool emptySendQueue) {
    if (!emptySendQueue) {
        connection->copyToOutputStream(std::string_view("hello"));
    }

    auto& cookie = connection->allocateCookie();
    // Reserve the cookie to ensure that the connection reset happens with
    // a blocked command (could be DCP also).
    cookie.reserve();
    cookie.setEwouldblock();

    auto& evb = frontEndThread->eventBase;

    bool releaseCalled = false;
    evb.runInEventBaseThread([&] {
        // Send CONNRESET to the connection while we have a blocked cookie.
        ASSERT_EQ(Connection::State::running, connection->getState());
        connection->mockConnReset();
        ASSERT_EQ(Connection::State::pending_close, connection->getState());

        // Clear the blocked state, and release the cookie.
        cookie.clearEwouldblock();
        // The blocked cookie has only 1 reference, since we called .reserve().
        // We call release() to drop the reference, however note that this
        // schedules a callback to be run later on the front-end thread.
        ASSERT_EQ(1, cookie.getRefcount());
        cookie.release();
        // Expect no change in the refcount, since the callback has not been
        // run yet.
        ASSERT_EQ(1, cookie.getRefcount());
        releaseCalled = true;
        // The connection is still pending close, since the callback has not
        // been run yet.
        EXPECT_EQ(Connection::State::pending_close, connection->getState());
    });

    EXPECT_FALSE(releaseCalled);
    // The first loop iteration will run our callback above, which will
    // trigger the connection reset and call cookie.release().
    evb.loopOnce(EVLOOP_NONBLOCK);
    EXPECT_TRUE(releaseCalled);
    // The second loop iteration is needed to run the connection callback
    // libevent read callback, which will move the connection to
    // immediate_close.
    evb.loopOnce(EVLOOP_NONBLOCK);

    // The connection should be closed and the object will be deleted.
    // If we try to delete the connection object again, it should throw an
    // exception since it was already deleted.
    EXPECT_THROW(frontEndThread->destroy_connection(*connection),
                 std::logic_error);
}

TEST_F(ConnectionUnitTests, MB_67796_emptySendQueue) {
    resetConnectionTest(true);
}

TEST_F(ConnectionUnitTests, MB_67796_nonEmptySendQueue) {
    resetConnectionTest(false);
}
