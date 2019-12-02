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

#include "programs/engine_testapp/mock_server.h"

#include "bucket_logger.h"
#include "configuration.h"
#include "ep_time.h"
#include "hash_table.h"
#include <folly/portability/GMock.h>
#include <getopt.h>
#include <logger/logger.h>
#include <memcached/config_parser.h>
#include <memcached/server_core_iface.h>
#include <memcached/server_log_iface.h>

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

    void shutdown() override {
        throw std::runtime_error(
                "UnitTestServerCore::shutdown() not implemented");
    }

    size_t get_max_item_iovec_size() override {
        return 1;
    }

    void trigger_tick() override {
        throw std::runtime_error(
                "UnitTestServerCore::trigger_tick() not implemented");
    }

    ThreadPoolConfig getThreadPoolSizes() override {
        return {};
    }

    bool isCollectionsEnabled() const override {
        return true;
    }
};

int main(int argc, char **argv) {
    bool verbose_logging = false;

    // Initialise GoogleMock (and GoogleTest), consuming any cmd-line arguments
    // it owns before we check our own.
    ::testing::InitGoogleMock(&argc, argv);

    // Parse command-line options.
    int cmd;
    bool invalid_argument = false;
    while (!invalid_argument &&
           (cmd = getopt(argc, argv, "v")) != EOF) {
        switch (cmd) {
        case 'v':
            verbose_logging = true;
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

    putenv(allow_no_stats_env);

    // Create a blackhole logger to prevent Address sanitizer error when
    // calling mock_init_alloc_hooks
    cb::logger::createBlackholeLogger();
    mock_init_alloc_hooks();
    init_mock_server();

    // Create the console logger for test case output
    cb::logger::createConsoleLogger();
    const auto spd_log_level = verbose_logging
                                       ? spdlog::level::level_enum::debug
                                       : spdlog::level::level_enum::critical;

    // Set the logging level in the api then setup the BucketLogger
    get_mock_server_api()->log->get_spdlogger()->set_level(spd_log_level);
    BucketLogger::setLoggerAPI(get_mock_server_api()->log);

    // Need to initialize ep_real_time and friends.
    UnitTestServerCore unitTestServerCore;
    initialize_time_functions(&unitTestServerCore);

    auto ret = RUN_ALL_TESTS();

    globalBucketLogger.reset();

    return ret;
}
