/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <getopt.h>
#include <logger/logger.h>
#include <utilities/openssl_utils.h>
#include <iostream>

int main(int argc, char** argv) {
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
    initialize_openssl();

    if (verbose) {
        cb::logger::createConsoleLogger();
    } else {
        cb::logger::createBlackholeLogger();
    }

    auto ret = RUN_ALL_TESTS();
    cb::logger::shutdown();
    shutdown_openssl();

    return ret;
}
