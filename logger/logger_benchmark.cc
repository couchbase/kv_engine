/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <benchmark/benchmark.h>

#include <logger/logger.h>
#include <logger/logger_config.h>
#include <iostream>
#include <optional>

/**
 * A fixture for benchmarking the logger.
 */
class LoggerBench : public benchmark::Fixture {
protected:
    void SetUp(const benchmark::State& state) override;

    void TearDown(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            if (cb::logger::isInitialized()) {
                cb::logger::shutdown();
            } else {
                std::cerr << "Failed to shutdown logger as it was never "
                             "initialized in the first place";
                return;
            }
        }
    }
};

void LoggerBench::SetUp(const benchmark::State& state) {
    if (state.thread_index == 0) {
        cb::logger::Config config{};
        config.cyclesize = 2048;
        config.buffersize = 8192;
        config.unit_test = true;
        config.console = false;

        auto init = cb::logger::initialize(config);
        if (init) {
            std::cerr << "Failed to initialize logger: " << *init;
            return;
        }

        cb::logger::get()->set_level(spdlog::level::level_enum::warn);
    }
}

class LoggerBench_Blackhole : public LoggerBench {
protected:
    void SetUp(const benchmark::State& state) override {
        if (state.thread_index == 0) {
            cb::logger::createBlackholeLogger();
        }
    }
};

/**
 * Benchmark the cost of logging to a level which is dropped.
 * We're currently using the async logging (which is the one
 * we'll be using "in production".. except that we don't have
 * the file-backend writing to the disk
 */
BENCHMARK_DEFINE_F(LoggerBench, LogToLoggerWithDisabledLogLevel)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        LOG_TRACE("Foo");
    }
}

/**
 * Benchmark the cost of logging to a level which is enabled (moved
 * to the sink).
 * We're currently using the async logging (which is the one
 * we'll be using "in production".. except that we don't have
 * the file-backend writing to the disk
 */
BENCHMARK_DEFINE_F(LoggerBench, LogToLoggerWithEnabledLogLevel)
(benchmark::State& state) {
    if (state.thread_index == 0) {
        cb::logger::get()->set_level(spdlog::level::level_enum::trace);
    }
    while (state.KeepRunning()) {
        LOG_TRACE("Foo");
    }
}

/**
 * Benchmark the cost of grabbing the logger (which means checking
 * for it's existence and copy a shared pointer).
 */
BENCHMARK_DEFINE_F(LoggerBench_Blackhole, GetLogger)(benchmark::State& state) {
    while (state.KeepRunning()) {
        auto logger = cb::logger::get();
        benchmark::DoNotOptimize(logger);
    }
}

BENCHMARK_REGISTER_F(LoggerBench_Blackhole, GetLogger)->Threads(1)->Threads(16);
BENCHMARK_REGISTER_F(LoggerBench, LogToLoggerWithDisabledLogLevel)
        ->Threads(1)
        ->Threads(16);
BENCHMARK_REGISTER_F(LoggerBench, LogToLoggerWithEnabledLogLevel)
        ->Threads(1)
        ->Threads(16);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return EXIT_FAILURE;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    return EXIT_SUCCESS;
}
