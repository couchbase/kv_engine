/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "environment.h"
#include "external_auth_manager_thread.h"

#include <daemon/enginemap.h>
#include <executor/executorpool.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <getopt.h>
#include <logger/logger.h>
#include <iostream>

int main(int argc, char** argv) {
    setenv("MEMCACHED_UNIT_TESTS", "true", 1);
    ::testing::InitGoogleTest(&argc, argv);
    bool verbose = false;

    int cmd;
    while ((cmd = getopt(argc, argv, "v")) != EOF) {
        switch (cmd) {
        case 'v':
            verbose = true;
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-v]" << std::endl
                      << std::endl
                      << "  -v Verbose - Print verbose memcached output "
                      << "to stderr." << std::endl;
            return EXIT_FAILURE;
        }
    }

    if (verbose) {
        cb::logger::createConsoleLogger();
    } else {
        cb::logger::createBlackholeLogger();
    }

    environment.max_file_descriptors = 2048;
    environment.engine_file_descriptors = 1024;

    ExecutorPool::create(ExecutorPool::Backend::Folly,
                         0,
                         ThreadPoolConfig::ThreadCount::Default,
                         ThreadPoolConfig::ThreadCount::Default,
                         ThreadPoolConfig::AuxIoThreadCount::Default,
                         ThreadPoolConfig::NonIoThreadCount::Default,
                         ThreadPoolConfig::IOThreadsPerCore::Default);

    externalAuthManager = std::make_unique<ExternalAuthManagerThread>();
    externalAuthManager->start();

    auto ret = RUN_ALL_TESTS();

    externalAuthManager->shutdown();
    externalAuthManager->waitForState(Couchbase::ThreadState::Zombie);
    externalAuthManager.reset();

    // Ensure engines are scrubbed (memcached) and destroyed.
    // Avoids static destruction order issues with threads
    // attempting to deregister with Phosphor if EngineManager
    // is allowed to be destroyed normally.
    shutdown_all_engines();
    cb::logger::shutdown();
    ExecutorPool::shutdown();

    return ret;
}
