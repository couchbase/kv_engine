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
#include "testapp_shutdown.h"

void ShutdownTest::SetUp() {
    TestappTest::SetUpTestCase();
    if (server_pid == pid_t(-1)) {
        std::cerr << "memcached not running. Terminate test execution"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    auto& conn = getAdminConnection();
    auto rsp = conn.execute(BinprotSetControlTokenCommand{ntohll(token), 0ull});
    if (!rsp.isSuccess()) {
        std::cerr << "Failed to set control token: " << rsp.getStatus()
                  << rsp.getResponse().toJSON(false) << std::endl
                  << "Exit program";
        exit(EXIT_FAILURE);
    }
}

TEST_F(ShutdownTest, ShutdownAllowed) {
    auto& conn = getAdminConnection();
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Shutdown);
    cmd.setCas(token);
    conn.sendCommand(cmd);

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess());
    waitForShutdown();
}
