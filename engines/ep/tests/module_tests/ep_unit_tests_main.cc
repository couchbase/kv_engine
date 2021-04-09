/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "configuration.h"
#include "ep_time.h"
#include "hash_table.h"
#include <engines/ep/src/environment.h>
#include <folly/portability/GMock.h>
#include <getopt.h>
#include <logger/logger.h>
#include <memcached/config_parser.h>
#include <memcached/server_core_iface.h>
#include <memcached/server_log_iface.h>
#include <platform/cb_arena_malloc.h>
#include <platform/cbassert.h>

/* static storage for environment variable set by putenv(). */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

/**
 * Implementation of ServerCoreIface for unit tests.
 *
 * In unit tests time stands still, to give deterministic behaviour.
 */
class UnitTestServerCore : public ServerCoreIface {
public:
    rel_time_t get_current_time() override {
        // Return a fixed time of '0'.
        return 0;
    }

    rel_time_t realtime(rel_time_t exptime) override {
        throw std::runtime_error(
                "UnitTestServerCore::realtime() not implemented");
    }

    time_t abstime(rel_time_t reltime) override {
        return get_current_time() + reltime;
    }

    time_t limit_abstime(time_t t, std::chrono::seconds limit) override {
        throw std::runtime_error(
                "UnitTestServerCore::limit_abstime() not implemented");
    }

    int parse_config(const char* str,
                     config_item* items,
                     FILE* error) override {
        return ::parse_config(str, items, error);
    }

    ThreadPoolConfig getThreadPoolSizes() override {
        return {};
    }

    size_t getMaxEngineFileDescriptors() override {
        return 0;
    }

    bool isCollectionsEnabled() const override {
        return true;
    }
};

int main(int argc, char **argv) {
    setupWindowsDebugCRTAssertHandling();

    spdlog::level::level_enum spd_log_level =
            spdlog::level::level_enum::critical;

    // Initialise GoogleMock (and GoogleTest), consuming any cmd-line arguments
    // it owns before we check our own.
    ::testing::InitGoogleMock(&argc, argv);

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

    putenv(allow_no_stats_env);

    cb::ArenaMalloc::setTCacheEnabled(threadCacheEnabled);

    // Create a blackhole logger to prevent Address sanitizer error when
    // calling mock_init_alloc_hooks
    cb::logger::createBlackholeLogger();
    mock_init_alloc_hooks();
    init_mock_server();

    // Create the console logger for test case output
    cb::logger::createConsoleLogger();
    // Set the logging level in the api then setup the BucketLogger
    get_mock_server_api()->log->get_spdlogger()->set_level(spd_log_level);
    BucketLogger::setLoggerAPI(get_mock_server_api()->log);

    // Need to initialize ep_real_time and friends.
    UnitTestServerCore unitTestServerCore;
    initialize_time_functions(&unitTestServerCore);

    // Need to set engine file descriptors as tests using CouchKVStore will use
    // a file cache that requires a fixed limit
    {
        // Set to 2 x the number of reserved file descriptors (i.e. the minimum
        // number of file descriptors required). This number will then be split
        // between all the backends compiled in (couchstore/rocks/magma). This
        // number won't be particularly high, but should be fine for unit
        // testing.
        auto& env = Environment::get();
        env.engineFileDescriptors = env.reservedFileDescriptors * 2;
    }

    auto ret = RUN_ALL_TESTS();

    globalBucketLogger.reset();

    return ret;
}
