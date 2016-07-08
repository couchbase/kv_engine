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

#include <memcached/extension_loggers.h>
#include "programs/engine_testapp/mock_server.h"

#include <getopt.h>
#include <gtest/gtest.h>

#include "configuration.h"
#include "stored-value.h"

/* static storage for environment variable set by putenv(). */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    putenv(allow_no_stats_env);

    init_mock_server(false);
    mock_init_alloc_hooks();

    // Default number of hashtable locks is too large for TSan to
    // track. Use the value in configuration.json (47 at time of
    // writing).
    HashTable::setDefaultNumLocks(Configuration().getHtLocks());

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
