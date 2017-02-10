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
#include <string.h>
#include <cerrno>
#include "testapp.h"

class ShutdownTest : public TestappTest {
public:
    static void SetUpTestCase() {
        // Do nothing.
        //
        // If we don't provide a SetUpTestCase we'll get the one from
        // TestappTest which will start the server for us (which is
        // what we want, but in this test we're going to start try
        // to stop the server so we need to have each test case
        // start (and stop) the server for us...
    }

    virtual void SetUp() {
        TestappTest::SetUpTestCase();
        ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid)
            << "Terminate test execution"
            << std::endl
            << (exit(1), "");

        auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getAdminConnection());

        BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN);
        cmd.setExtrasValue(token);
        conn.sendCommand(cmd);

        BinprotResponse rsp;
        conn.recvResponse(rsp);

        ASSERT_TRUE(rsp.isSuccess())
                        << "Failed to set control token: "
                        << rsp.getStatus()
                        << std::endl
                        << (exit(1), "");
    }

    virtual void TearDown() {
        TestappTest::TearDownTestCase();
    }

    static void TearDownTestCase() {
        // Empty
    }

protected:
};

TEST_F(ShutdownTest, ShutdownAllowed) {
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getAdminConnection());
    BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_SHUTDOWN);
    cmd.setCas(token);
    conn.sendCommand(cmd);

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess());
    waitForShutdown();
}
