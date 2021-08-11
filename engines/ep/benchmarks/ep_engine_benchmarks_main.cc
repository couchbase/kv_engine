/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <programs/engine_testapp/mock_server.h>

#include <benchmark/benchmark.h>
#include <engines/ep/src/bucket_logger.h>
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
    init_mock_server();
    initialize_time_functions(get_mock_server_api()->core);

    ::benchmark::Initialize(&argc, argv);

    // Don't set the logger API until after we call initialize. If we attempt to
    // call this with --help then ::benchmark::Initialize calls exit(0) which
    // causes us to abort when we throw due to inability to lock a mutex in the
    // spdlog registry when we try to destruct the globalBucketLogger. If we
    // defer the API setup then we won't be able to get to the registry to cause
    // the abort.
    getGlobalBucketLogger()->set_level(spdlog::level::level_enum::critical);

    /*
     * Run the benchmarks. From benchmark.cc, 0 gets returned if
     * there is an error, else it returns the number of benchmark tests.
     * If we get a 0 return, then return 1 from main to signal an error
     * occurred.
     */
    auto result = ::benchmark::RunSpecifiedBenchmarks();

    return result == 0 ? 1 : 0;
}
