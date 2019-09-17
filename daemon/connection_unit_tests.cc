/*
 *     Copyright 2019 Couchbase, Inc.
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

    std::vector<iovec>& getIov() {
        return iov;
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
        initialize_engine_map();
        initialize_buckets();
    }

    static void TearDownTestCase() {
        cleanup_buckets();
        shutdown_engine_map();
    }

protected:
    std::unique_ptr<FrontEndThread> frontEndThread;
    MockConnection connection;
};

TEST_F(ConnectionUnitTests, AddIov) {
    // Verify that we extend the previous entry if this is a continuation
    // of the same segment
    connection.addMsgHdr(true);
    connection.addIov(nullptr, 0x100);
    connection.addIov(reinterpret_cast<const void*>(0x100), 0x100);
    ASSERT_EQ(1, connection.getIovUsed());
    auto iov = connection.getIov();
    ASSERT_EQ(nullptr, iov.front().iov_base);
    ASSERT_EQ(0x200, iov.front().iov_len);

    // Verify that we create a new entry if the entry we add isn't a
    // continuation of the segment
    connection.addMsgHdr(true);
    connection.addIov(nullptr, 0x100);
    connection.addIov(reinterpret_cast<const void*>(0x200), 0x200);
    ASSERT_EQ(2, connection.getIovUsed());
    iov = connection.getIov();
    ASSERT_EQ(nullptr, iov[0].iov_base);
    ASSERT_EQ(0x100, iov[0].iov_len);
    ASSERT_EQ(reinterpret_cast<const void*>(0x200), iov[1].iov_base);
    ASSERT_EQ(0x200, iov[1].iov_len);
}
