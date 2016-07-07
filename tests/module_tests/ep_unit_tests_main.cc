/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Main function & globals for the ep_unit_test target.
 */
#include "config.h"

#include <memcached/extension_loggers.h>
#include "programs/engine_testapp/mock_server.h"

#include <getopt.h>
#include <gtest/gtest.h>

#include "configuration.h"
#include "stored-value.h"


int main(int argc, char **argv) {
    bool log_to_stderr = false;
    // Parse command-line options.
    int cmd;
    bool invalid_argument = false;
    while (!invalid_argument &&
           (cmd = getopt(argc, argv, "v")) != EOF) {
        switch (cmd) {
        case 'v':
            log_to_stderr = true;
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-v] [gtest_options...]" << std::endl
                      << std::endl
                      << "  -v Verbose - Print verbose output to stderr."
                      << std::endl << std::endl;
            invalid_argument = true;
            break;
        }
    }

    init_mock_server(log_to_stderr);
    get_mock_server_api()->log->set_level(EXTENSION_LOG_DEBUG);

    if (memcached_initialize_stderr_logger(get_mock_server_api) != EXTENSION_SUCCESS) {
        std::cerr << argv[0] << ": Failed to initialize log system" << std::endl;
        return 1;
    }


    // Default number of hashtable locks is too large for TSan to
    // track. Use the value in configuration.json (47 at time of
    // writing).
    HashTable::setDefaultNumLocks(Configuration().getHtLocks());

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
