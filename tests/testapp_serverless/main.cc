/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "serverless_test.h"

#include <event2/thread.h>
#include <folly/portability/Stdlib.h>
#include <platform/cbassert.h>
#include <platform/platform_socket.h>
#include <platform/socket.h>
#include <csignal>
#include <filesystem>
#include <string>

int main(int argc, char** argv) {
    setupWindowsDebugCRTAssertHandling();
    cb::net::initialize();

#if defined(EVTHREAD_USE_WINDOWS_THREADS_IMPLEMENTED)
    const auto failed = evthread_use_windows_threads() == -1;
#elif defined(EVTHREAD_USE_PTHREADS_IMPLEMENTED)
    const auto failed = evthread_use_pthreads() == -1;
#else
#error "No locking mechanism for libevent available!"
#endif

    if (failed) {
        std::cerr << "Failed to enable libevent locking. Terminating program"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    setenv("MEMCACHED_UNIT_TESTS", "true", 1);

    auto pwdb = std::filesystem::path{SOURCE_ROOT} / "tests" /
                "testapp_serverless" / "pwdb.json";
    setenv("CBSASL_PWFILE", pwdb.generic_string().c_str(), 1);

    auto rbac = std::filesystem::path{SOURCE_ROOT} / "tests" /
                "testapp_serverless" / "rbac.json";
    setenv("MEMCACHED_RBAC", rbac.generic_string().c_str(), 1);

#ifndef WIN32
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        std::cerr << "Fatal: failed to ignore SIGPIPE" << std::endl;
        return 1;
    }
#endif
    ::testing::InitGoogleTest(&argc, argv);

    cb::test::ServerlessTest::StartCluster();
    const auto ret = RUN_ALL_TESTS();
    cb::test::ServerlessTest::ShutdownCluster();

    return ret;
}
