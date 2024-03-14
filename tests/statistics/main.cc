/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <daemon/external_auth_manager_thread.h>
#include <daemon/mcaudit.h>
#include <folly/portability/GTest.h>
#include <logger/logger.h>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    cb::logger::createBlackholeLogger();

    externalAuthManager = std::make_unique<ExternalAuthManagerThread>();
    initialize_audit();

    int ret = RUN_ALL_TESTS();

    externalAuthManager->shutdown();
    externalAuthManager->waitForState(Couchbase::ThreadState::Zombie);
    externalAuthManager.reset();
    shutdown_audit();
    cb::logger::shutdown();
    return ret;
}
