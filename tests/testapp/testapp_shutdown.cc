/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_shutdown.h"
#include <platform/process_monitor.h>

void ShutdownTest::SetUp() {
    TestappTest::SetUpTestCase();
    if (!memcachedProcess->isRunning()) {
        std::cerr << "memcached not running. Terminate test execution"
                  << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }

    auto& conn = getAdminConnection();
    auto rsp = conn.execute(BinprotSetControlTokenCommand{token, 0ull});
    if (!rsp.isSuccess()) {
        std::cerr << "Failed to set control token: " << rsp.getStatus()
                  << rsp.getResponse().to_json(false) << std::endl
                  << "Exit program";
        mcd_env->terminate(EXIT_FAILURE);
    }
}

TEST_F(ShutdownTest, ShutdownAllowed) {
    expectMemcachedTermination.store(true);
    auto& conn = getAdminConnection();
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Shutdown);
    cmd.setCas(token);
    conn.sendCommand(cmd);

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess());
    waitForShutdown();
}
