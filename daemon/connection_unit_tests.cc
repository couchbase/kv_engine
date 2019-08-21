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
};

class ConnectionUnitTests : public ::testing::Test {
public:
    ConnectionUnitTests()
        : frontEndThread(std::make_unique<FrontEndThread>()),
          connection(*frontEndThread) {
    }

    static void SetUpTestCase() {
        cb::logger::createBlackholeLogger();
        initialize_buckets();
    }

    static void TearDownTestCase() {
        cleanup_buckets();
    }

protected:
    std::unique_ptr<FrontEndThread> frontEndThread;
    MockConnection connection;
};

