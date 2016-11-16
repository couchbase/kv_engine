/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string.h>
#include <system_error>
#include "testapp.h"

static const int idle_time = 2;
static const int wait_time = 3;

/**
 * The connection timeout tests tries to verify that the idle-timer
 * don't kill admin connections. Note that we don't have explicit tests
 * for DCP and TAP connections, but that is primarily because we don't
 * have support for DCP and TAP in the engine we're testing in the
 * backend. The timer code is the same for admin/tap/dcp connections,
 * so as long as the connection correctly identifies itself as DCP and TAP
 * it should be tested by this code.
 */
class ConnectionTimeoutTest : public TestappTest {
public:

    static void SetUpTestCase() {
        memcached_cfg.reset(generate_config(0));
        cJSON_AddNumberToObject(memcached_cfg.get(),
                                "connection_idle_time",
                                idle_time);

        start_memcached_server(memcached_cfg.get());

        if (HasFailure()) {
            server_pid = reinterpret_cast<pid_t>(-1);
        } else {
            CreateTestBucket();
        }

        ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid);
    }

protected:

    void testit(bool admin) {
        auto& conn = connectionMap.getConnection(Protocol::Memcached, false);
        conn.reconnect();

        if (admin) {
            ASSERT_NO_THROW(conn.authenticate("_admin", "password", "PLAIN"));
        }

        unique_cJSON_ptr json;
        EXPECT_NO_THROW(json = conn.stats(""));
        EXPECT_NE(nullptr, json.get());

        wait();

        try {
            json = conn.stats("");
            if (admin) {
                EXPECT_NE(nullptr, json.get());
            } else {
                FAIL() << "The connection should have timed out";
            }
        } catch (const std::system_error& e) {
            if (admin) {
                FAIL() << "Admin connection should not time out";
            }

            EXPECT_EQ(std::system_category(), e.code().category());
            EXPECT_EQ(ECONNRESET, e.code().value());
        }
    }


    void wait() {
        std::mutex mutex;
        std::condition_variable cond;
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait_for(lock, std::chrono::seconds(wait_time));
    }
};

TEST_F(ConnectionTimeoutTest, TimeoutNormalConnections) {
    testit(false);
}

TEST_F(ConnectionTimeoutTest, NoTimeoutForAdmin) {
    testit(true);
}
