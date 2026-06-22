/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Main function & globals for the ep_unit_test target.
 */

#include "programs/engine_testapp/mock_server.h"

#include "bucket_logger.h"
#include "ep_time.h"
#include "unit_test_server_core.h"
#include <cblogger/logger.h>
#include <folly/portability/GMock.h>
#include <folly/portability/Stdlib.h>
#include <fuzzing/init.h>
#include <getopt.h>
#include <memcached/config_parser.h>
#include <memcached/server_core_iface.h>
#include <memcached/unit_test_mode.h>
#include <phosphor/phosphor.h>
#include <platform/cb_arena_malloc.h>
#include <platform/cb_time.h>
#include <platform/cbassert.h>
#include <utilities/crl_policy.h>
#include <array>

using namespace std::chrono_literals;

/* static storage for environment variable set by putenv(). */
static std::array<char, 28> allow_no_stats_env{
        {"ALLOW_NO_STATS_UPDATE=yeah\0"}};

int main(int argc, char **argv) {
    setupWindowsDebugCRTAssertHandling();

    spdlog::level::level_enum spd_log_level =
            spdlog::level::level_enum::critical;

    // Initialise GoogleMock (and GoogleTest), consuming any cmd-line arguments
    // it owns before we check our own.
    ::testing::InitGoogleMock(&argc, argv);

    cb::fuzzing::initialize(argc, argv);

    // Parse command-line options.
    int cmd;
    bool invalid_argument = false;

    // ep-engine unit tests run without thread-caching for a mem_used that is
    // more testable, but occasionally turning it on is useful
    bool threadCacheEnabled = false;
    while (!invalid_argument && (cmd = getopt(argc, argv, "vt")) != EOF) {
        switch (cmd) {
        case 'v':
            // Maximum of 3 levels of verbose logging (info, debug, trace),
            // initially only show critical messages.
            switch (spd_log_level) {
            case spdlog::level::level_enum::critical:
                spd_log_level = spdlog::level::level_enum::info;
                break;
            case spdlog::level::level_enum::info:
                spd_log_level = spdlog::level::level_enum::debug;
                break;
            case spdlog::level::level_enum::debug:
                spd_log_level = spdlog::level::level_enum::trace;
                break;
            default:
                // Cannot increase further.
                break;
            }
            break;
        case 't':
            threadCacheEnabled = true;
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-v] [gtest_options...]"
                      << std::endl
                      << std::endl
                      << "  -v Verbose - Print verbose output to stderr. Use "
                         "multiple times to increase verbosity\n"
                      << "  -t Alloc Thread Cache On - Use thread-caching "
                      << "in malloc/calloc etc...\n"
                      << std::endl;
            invalid_argument = true;
            break;
        }
    }

    putenv(allow_no_stats_env.data());
    setUnitTestMode(true);
    // MB-69547: Allow bypassing control negotiation for testing purposes
    //           as a lot of the old DCP tests don't drive the state
    //           machine to complete control negotiation.
    // https://jira.issues.couchbase.com/browse/MB-69547
    setenv("MB69547_BYPASS_CONTROL_NEGOTIATION", "true", 1);

    cb::ArenaMalloc::setTCacheEnabled(threadCacheEnabled);

    // Create a blackhole logger to prevent Address sanitizer error when
    // calling mock_init_alloc_hooks
    cb::logger::createBlackholeLogger();
    init_mock_server();

    // Create the console logger for test case output
    cb::logger::createConsoleLogger();
    // Set the logging level
    cb::logger::setLogLevels(spd_log_level);

    // Need to initialize ep_real_time and friends.
    UnitTestServerCore unitTestServerCore;
    initialize_time_functions(&unitTestServerCore);

    // Ensure phosphor TraceLog singleton is initialised before we run any
    // tests - specifically before we create the ExecutorPool singleton and
    // its background threads. If TraceLog is *not* initialised before
    // ExecutorPool, then it will also be destroyed before ExecutorPool; which
    // then results in ExecutorPool crashing when it attempts to unregister
    // worker threads from phosphor.
    phosphor::TraceLog::getInstance();

    auto ret = RUN_ALL_TESTS();

    return ret;
}
