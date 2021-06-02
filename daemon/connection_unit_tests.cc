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

#include <folly/portability/GTest.h>

/// A mock connection which doesn't own a socket and isn't bound to libevent
class MockConnection : public Connection {
public:
    explicit MockConnection(FrontEndThread& frontEndThread)
        : Connection(frontEndThread) {
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
