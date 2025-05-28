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
#include <platform/dirutils.h>
#include <platform/process_monitor.h>

void ShutdownTest::SetUp() {
    createUserConnection = true;
    auto config = generate_config();
    config["threads"] = 4;
    doSetUpTestCaseWithConfiguration(config);

    if (!memcachedProcess->isRunning()) {
        std::cerr << "memcached not running. Terminate test execution"
                  << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }

    // The CleanOrUnclean shutdown tests fails unless we run at least
    // one op here.. (investigate as a separate task)
    auto& conn = getAdminConnection();
    conn.execute(BinprotGenericCommand(cb::mcbp::ClientOpcode::Noop));
}

TEST_F(ShutdownTest, ShutdownAllowed) {
    expectMemcachedTermination.store(true);
    auto& conn = getAdminConnection();
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Shutdown);
    conn.sendCommand(cmd);

    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess());
    waitForShutdown();
}

TEST_F(ShutdownTest, VerifyShutdownWithBlockedConnections) {
    if (mcd_env->getTestBucket().isFullEviction()) {
        GTEST_SKIP();
    }

    auto testfile =
            std::filesystem::current_path() / cb::io::mktemp("lockfile");
    rebuildUserConnection(false);

    // Configure so that the engine will return
    // cb::engine_errc::would_block and not process any operation given
    // to it.  This means the connection will remain in a blocked state.
    userConnection->configureEwouldBlockEngine(EWBEngineMode::BlockMonitorFile,
                                               cb::engine_errc::would_block,
                                               0,
                                               testfile.generic_string());
    userConnection->sendCommand(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, "mykey"});
    // The connection should be blocked inside the engine. Now try to
    // initiate shutdown
    expectMemcachedTermination.store(true);

    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Shutdown});
    EXPECT_TRUE(rsp.isSuccess());

    adminConnection.reset();

    // Wait to make sure shutdown is starting
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // and try to connect a few times to make sure we dispatch a request
    // which previously would cause the thread to stop
    for (int ii = 0; ii < memcached_cfg["threads"].get<int>(); ++ii) {
        try {
            auto c = userConnection->clone();
        } catch (const std::exception&) {
        }
    }

    // At this time the front end thread would have been shut down,
    // but sleep a little bit more to make sure it would have had a chance
    // to run
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Now nuke the lockfile which should cause the command to resume
    // and delete (and shutdown) to complete
    remove(testfile);
    waitForShutdown();
}
