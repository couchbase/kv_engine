/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "syntax_validator.h"

#include <benchmark/benchmark.h>
#include <folly/portability/GTest.h>

// This is a (modified) copy of the benchmark from platform
// @todo I believe benchmark supports templates the way gtest does so
//       we don't need to duplicate all of the code?

using cb::json::SyntaxValidator;

static void benchmarkJsonValidator(SyntaxValidator::Type type,
                                   benchmark::State& state,
                                   std::string_view data,
                                   bool valid) {
    auto validator = SyntaxValidator::New(type);
    EXPECT_EQ(valid, validator->validate(data));
    while (state.KeepRunning()) {
        validator->validate(data);
    }
}

static void BM_SyntaxValidator_JSONCheckerEmpty(benchmark::State& state) {
    benchmarkJsonValidator(
            SyntaxValidator::Type::JSON_checker, state, "", false);
}
BENCHMARK(BM_SyntaxValidator_JSONCheckerEmpty);

static void BM_SyntaxValidator_NlohmannEmpty(benchmark::State& state) {
    benchmarkJsonValidator(SyntaxValidator::Type::Nlohmann, state, "", false);
}
BENCHMARK(BM_SyntaxValidator_NlohmannEmpty);

// Benchmark checking a binary object for JSON.  Object is
// "immediately" non-JSON; i.e. first byte is not a valid JSON
// starting char.
static void BM_SyntaxValidator_JSONCheckerBinary(benchmark::State& state) {
    const std::vector<uint8_t> binaryDoc = {1, 2, 3, 4, 5};
    const std::string_view view{reinterpret_cast<const char*>(binaryDoc.data()),
                                binaryDoc.size()};

    benchmarkJsonValidator(
            SyntaxValidator::Type::JSON_checker, state, view, false);
}
BENCHMARK(BM_SyntaxValidator_JSONCheckerBinary);

static void BM_SyntaxValidator_NlohmannBinary(benchmark::State& state) {
    const std::vector<uint8_t> binaryDoc = {1, 2, 3, 4, 5};
    const std::string_view view{reinterpret_cast<const char*>(binaryDoc.data()),
                                binaryDoc.size()};
    benchmarkJsonValidator(SyntaxValidator::Type::Nlohmann, state, view, false);
}
BENCHMARK(BM_SyntaxValidator_NlohmannBinary);

// Benchmark checking a flat JSON array (of numbers) as JSON; e.g.
//   [ 0, 1, 2, 3, ..., N]
// Input argument 0 specifies the number of elements in the array.
std::string makeArray(benchmark::State& state) {
    std::string jsonArray = "[";
    for (int ii = 0; ii < state.range(0); ++ii) {
        jsonArray += std::to_string(ii) + ",";
    }
    // replace last comma with closing brace.
    jsonArray.back() = ']';
    return jsonArray;
}

static void BM_SyntaxValidator_JSONCheckerJsonArray(benchmark::State& state) {
    benchmarkJsonValidator(
            SyntaxValidator::Type::JSON_checker, state, makeArray(state), true);
}
BENCHMARK(BM_SyntaxValidator_JSONCheckerJsonArray)
        ->RangeMultiplier(10)
        ->Range(1, 10000);

static void BM_SyntaxValidator_NlohmannJsonArray(benchmark::State& state) {
    benchmarkJsonValidator(
            SyntaxValidator::Type::Nlohmann, state, makeArray(state), true);
}
BENCHMARK(BM_SyntaxValidator_NlohmannJsonArray)
        ->RangeMultiplier(10)
        ->Range(1, 10000);

// Benchmark checking a nested JSON dictonary as JSON; e.g.
//   {"0": { "1": { ... }}}
// Input argument 0 specifies the number of levels of nesting.
std::string makeNestedDict(benchmark::State& state) {
    std::string dict;
    for (int ii = 0; ii < state.range(0); ++ii) {
        dict += "{\"" + std::to_string(ii) + "\":";
    }
    dict += "0";
    for (int ii = 0; ii < state.range(0); ++ii) {
        dict += "}";
    }
    return dict;
}

static void BM_SyntaxValidator_JSONCheckerJsonNestedDict(
        benchmark::State& state) {
    benchmarkJsonValidator(SyntaxValidator::Type::JSON_checker,
                           state,
                           makeNestedDict(state),
                           true);
}
BENCHMARK(BM_SyntaxValidator_JSONCheckerJsonNestedDict)
        ->RangeMultiplier(10)
        ->Range(1, 10000);

static void BM_SyntaxValidator_NlohmannJsonNestedDict(benchmark::State& state) {
    benchmarkJsonValidator(SyntaxValidator::Type::Nlohmann,
                           state,
                           makeNestedDict(state),
                           true);
}
BENCHMARK(BM_SyntaxValidator_NlohmannJsonNestedDict)
        ->RangeMultiplier(10)
        ->Range(1, 10000);

// Using custom main() function (instead of BENCHMARK_MAIN macro) to
// init GoogleTest.
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(throw_on_failure) = true;
    ::testing::InitGoogleTest(&argc, argv);

    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
}
