/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "connection.h"
#include "enginemap.h"
#include "front_end_thread.h"
#include "log_macros.h"
#include "memcached.h"

#include <daemon/cookie.h>
#include <memcached/server_cookie_iface.h>
#include <platform/socket.h>

#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>

/// A mock connection which doesn't own a socket and isn't bound to libevent
class MockConnection : public Connection {
public:
    explicit MockConnection(FrontEndThread& frontEndThread)
        : Connection(frontEndThread) {
        const auto options = BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE |
                             BEV_OPT_DEFER_CALLBACKS;
        bev.reset(bufferevent_socket_new(
                frontEndThread.eventBase.getLibeventBase(), -1, options));
        bufferevent_setcb(bev.get(),
                          Connection::read_callback,
                          Connection::write_callback,
                          Connection::event_callback,
                          static_cast<void*>(this));

        bufferevent_enable(bev.get(), EV_READ);
    }
};

class ConnectionUnitTests : public ::testing::Test {
public:
    ConnectionUnitTests()
        : frontEndThread(std::make_unique<FrontEndThread>()),
          connection(*frontEndThread) {
    }

    static void SetUpTestCase() {
        cb::logger::createBlackholeLogger();
        // Make sure that we have the buckets submodule initialized and
        // have no-bucket available.
        BucketManager::instance();
    }

    static void TearDownTestCase() {
        cleanup_buckets();
    }

protected:
    std::unique_ptr<FrontEndThread> frontEndThread;
    MockConnection connection;
};

TEST_F(ConnectionUnitTests, MB43374) {
    EXPECT_STREQ("unknown:0", connection.getConnectionId().data());
    // Verify that the client can't mess up the output by providing "
    connection.setConnectionId(R"(This "is" my life)");
    auto msg = fmt::format("{}", connection.getConnectionId().data());
    EXPECT_EQ("This  is  my life", msg);
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

    auto cookie = std::make_unique<Cookie>(connection);
    // fake the engine creating a ConnHandler and reserving the cookie
    auto* cookieIface = get_server_api()->cookie;
    ASSERT_TRUE(cookieIface);
    cookieIface->reserve(*cookie);

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
        cookieIface->release(*cookie);
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
