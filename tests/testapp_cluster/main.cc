/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "cluster_framework/cluster.h"
#include "clustertest.h"
#include <event2/thread.h>
#include <platform/cbassert.h>
#include <platform/command_line_options_parser.h>
#include <platform/dirutils.h>
#include <platform/platform_socket.h>
#include <array>
#include <csignal>
#include <cstdlib>
#include <string>

using cb::test::BucketPersistenceBackend;
using namespace std::string_view_literals;

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

    // We need to set MEMCACHED_UNIT_TESTS to enable the use of
    // the ewouldblock engine..
    static std::array<char, 80> envvar;
    snprintf(envvar.data(), envvar.size(), "MEMCACHED_UNIT_TESTS=true");
    putenv(envvar.data());

    const auto isasl_file_name = cb::io::sanitizePath(
            SOURCE_ROOT "/tests/testapp_cluster/cbsaslpw.json");

    // Add the file to the exec environment
    static std::array<char, 1024> isasl_env_var;
    snprintf(isasl_env_var.data(),
             isasl_env_var.size(),
             "CBSASL_PWFILE=%s",
             isasl_file_name.c_str());
    putenv(isasl_env_var.data());

#ifndef WIN32
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        std::cerr << "Fatal: failed to ignore SIGPIPE" << std::endl;
        return 1;
    }
#endif
    ::testing::InitGoogleTest(&argc, argv);
    auto backend = BucketPersistenceBackend::Couchstore;

    cb::getopt::CommandLineOptionsParser parser;
    using cb::getopt::Argument;
    parser.addOption(
            {[&backend](auto value) {
                 if (value == "magma"sv) {
                     backend = BucketPersistenceBackend::Magma;
                 } else if (value == "couchstore"sv) {
                     backend = BucketPersistenceBackend::Couchstore;
                 } else {
                     std::cerr << "backend must be 'couchstore' or 'magma'"
                               << std::endl;
                     exit(EXIT_FAILURE);
                 }
             },
             "backend",
             Argument::Required,
             "magma",
             "The name of the backend"});

    parser.addOption({[&parser](auto) {
                          std::cout << std::endl
                                    << std::endl
                                    << parser << std::endl;
                      },
                      "help",
                      "This help text"});

    parser.parse(argc, argv, [&parser]() { std::exit(EXIT_FAILURE); });

    cb::test::ClusterTest::StartCluster(backend);
    const auto ret = RUN_ALL_TESTS();
    cb::test::ClusterTest::ShutdownCluster();

    return ret;
}
