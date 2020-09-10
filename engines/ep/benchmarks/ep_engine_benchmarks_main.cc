/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <programs/engine_testapp/mock_server.h>

#include <benchmark/benchmark.h>
#include <engines/ep/src/bucket_logger.h>
#include <engines/ep/src/environment.h>
#include <logger/logger.h>
#include <platform/cbassert.h>

#include "ep_time.h"

static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

/**
 * main() function for ep_engine_benchmark. Sets up environment, then runs
 * all registered GoogleBenchmark benchmarks and GoogleTest tests.
 *
 * --gtest_filter and --benchmark_filter can be used to run a subset of
 * the tests / benchmarks.
 */
int main(int argc, char** argv) {
    setupWindowsDebugCRTAssertHandling();
    putenv(allow_no_stats_env);
    cb::logger::createBlackholeLogger();
    mock_init_alloc_hooks();
    init_mock_server();
    initialize_time_functions(get_mock_server_api()->core);

    ::benchmark::Initialize(&argc, argv);

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

    // Don't set the logger API until after we call initialize. If we attempt to
    // call this with --help then ::benchmark::Initialize calls exit(0) which
    // causes us to abort when we throw due to inability to lock a mutex in the
    // spdlog registry when we try to destruct the globalBucketLogger. If we
    // defer the API setup then we won't be able to get to the registry to cause
    // the abort.
    BucketLogger::setLoggerAPI(get_mock_server_api()->log);
    globalBucketLogger->set_level(spdlog::level::level_enum::critical);

    /*
     * Run the benchmarks. From benchmark.cc, 0 gets returned if
     * there is an error, else it returns the number of benchmark tests.
     * If we get a 0 return, then return 1 from main to signal an error
     * occurred.
     */
    auto result = ::benchmark::RunSpecifiedBenchmarks();

    globalBucketLogger.reset();

    return result == 0 ? 1 : 0;
}
