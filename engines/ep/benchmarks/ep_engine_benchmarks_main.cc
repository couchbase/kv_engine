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
    putenv(allow_no_stats_env);
    mock_init_alloc_hooks();
    init_mock_server(true);
    initialize_time_functions(get_mock_server_api()->core);
    ::benchmark::Initialize(&argc, argv);
    /*
     * Run the benchmarks. From benchmark.cc, 0 gets returned if
     * there is an error, else it returns the number of benchmark tests.
     * If we get a 0 return, then return 1 from main to signal an error
     * occurred.
     */
    return ::benchmark::RunSpecifiedBenchmarks() == 0 ? 1 : 0;
}
