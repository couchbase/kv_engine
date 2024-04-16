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

using cb::json::SyntaxValidator;

static void benchmarkJsonValidator(SyntaxValidator::Type type,
                                   benchmark::State& state,
                                   std::string_view data,
                                   bool valid) {
    auto validator = SyntaxValidator::New(type);
    EXPECT_EQ(valid, validator->validate(data));
    while (state.KeepRunning()) {
        EXPECT_EQ(valid, validator->validate(data));
    }
}

static void BM_SyntaxValidator_Empty(benchmark::State& state,
                                     SyntaxValidator::Type validator) {
    benchmarkJsonValidator(validator, state, "", false);
}
BENCHMARK_CAPTURE(BM_SyntaxValidator_Empty,
                  JSON_checker,
                  SyntaxValidator::Type::JSON_checker);
BENCHMARK_CAPTURE(BM_SyntaxValidator_Empty,
                  JSON_checker_vectorized,
                  SyntaxValidator::Type::JSON_checker_vectorized);
BENCHMARK_CAPTURE(BM_SyntaxValidator_Empty,
                  Nlohmann,
                  SyntaxValidator::Type::Nlohmann);

// Benchmark checking a binary object for JSON.  Object is
// "immediately" non-JSON; i.e. first byte is not a valid JSON
// starting char.
static void BM_SyntaxValidator_Binary(benchmark::State& state,
                                      SyntaxValidator::Type validator) {
    const std::vector<uint8_t> binaryDoc = {1, 2, 3, 4, 5};
    const std::string_view view{reinterpret_cast<const char*>(binaryDoc.data()),
                                binaryDoc.size()};

    benchmarkJsonValidator(validator, state, view, false);
}
BENCHMARK_CAPTURE(BM_SyntaxValidator_Binary,
                  JSON_checker,
                  SyntaxValidator::Type::JSON_checker);
BENCHMARK_CAPTURE(BM_SyntaxValidator_Binary,
                  JSON_checker_vectorized,
                  SyntaxValidator::Type::JSON_checker_vectorized);
BENCHMARK_CAPTURE(BM_SyntaxValidator_Binary,
                  Nlohmann,
                  SyntaxValidator::Type::Nlohmann);

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

static void BM_SyntaxValidator_JsonArray(benchmark::State& state,
                                         SyntaxValidator::Type validator) {
    benchmarkJsonValidator(validator, state, makeArray(state), true);
}

BENCHMARK_CAPTURE(BM_SyntaxValidator_JsonArray,
                  JSON_checker,
                  SyntaxValidator::Type::JSON_checker)
        ->RangeMultiplier(10)
        ->Range(1, 10000);
BENCHMARK_CAPTURE(BM_SyntaxValidator_JsonArray,
                  JSON_checker_vectorized,
                  SyntaxValidator::Type::JSON_checker_vectorized)
        ->RangeMultiplier(10)
        ->Range(1, 10000);
BENCHMARK_CAPTURE(BM_SyntaxValidator_JsonArray,
                  Nlohmann,
                  SyntaxValidator::Type::Nlohmann)
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

static void BM_SyntaxValidator_JsonNestedDict(benchmark::State& state,
                                              SyntaxValidator::Type validator) {
    benchmarkJsonValidator(validator, state, makeNestedDict(state), true);
}

BENCHMARK_CAPTURE(BM_SyntaxValidator_JsonNestedDict,
                  JSON_checker,
                  SyntaxValidator::Type::JSON_checker)
        ->RangeMultiplier(10)
        ->Range(1, 10000);
BENCHMARK_CAPTURE(BM_SyntaxValidator_JsonNestedDict,
                  JSON_checker_vectorized,
                  SyntaxValidator::Type::JSON_checker_vectorized)
        ->RangeMultiplier(10)
        ->Range(1, 10000);
BENCHMARK_CAPTURE(BM_SyntaxValidator_JsonNestedDict,
                  Nlohmann,
                  SyntaxValidator::Type::Nlohmann)
        ->RangeMultiplier(10)
        ->Range(1, 10000);

/**
 * Construct a JSON array with n copies of the given JSON-encoded value.
 */
std::string makeJsonArray(const std::string& valueJson, size_t n) {
    std::string jsonArray = "[";
    jsonArray.reserve((valueJson.size() + 1) * n);
    for (size_t i = 0; i < n; ++i) {
        jsonArray += valueJson;
        jsonArray += ',';
    }
    jsonArray.pop_back();
    jsonArray += ']';
    return jsonArray;
}

std::string makeStringsArray(benchmark::State& state) {
    std::string str = '\"' + std::string(state.range(0), 'x') + '\"';
    return makeJsonArray(str, 50);
}

static void BM_SyntaxValidator_Strings(benchmark::State& state,
                                       SyntaxValidator::Type validator) {
    benchmarkJsonValidator(validator, state, makeStringsArray(state), true);
}

BENCHMARK_CAPTURE(BM_SyntaxValidator_Strings,
                  JSON_checker,
                  SyntaxValidator::Type::JSON_checker)
        ->RangeMultiplier(10)
        ->Range(1, 10000);
BENCHMARK_CAPTURE(BM_SyntaxValidator_Strings,
                  JSON_checker_vectorized,
                  SyntaxValidator::Type::JSON_checker_vectorized)
        ->RangeMultiplier(10)
        ->Range(1, 10000);
BENCHMARK_CAPTURE(BM_SyntaxValidator_Strings,
                  Nlohmann,
                  SyntaxValidator::Type::Nlohmann)
        ->RangeMultiplier(10)
        ->Range(1, 10000);

static void BM_SyntaxValidator_SampleDocument(benchmark::State& state,
                                              SyntaxValidator::Type validator) {
    constexpr auto doc =
            "{\"id\":10000,\"type\":\"route\",\"airline\":\"AF\","
            "\"airlineid\":\"airline_137\",\"sourceairport\":\"TLV\","
            "\"destinationairport\":\"MRS\",\"stops\":0,\"equipment\":\"320\","
            "\"schedule\":[{\"day\":0,\"utc\":\"10:13:00\","
            "\"flight\":\"AF198\"},{\"day\":0,\"utc\":\"19:14:00\","
            "\"flight\":\"AF547\"},{\"day\":0,\"utc\":\"01:31:00\","
            "\"flight\":\"AF943\"},{\"day\":1,\"utc\":\"12:40:00\","
            "\"flight\":\"AF356\"},{\"day\":1,\"utc\":\"08:58:00\","
            "\"flight\":\"AF480\"},{\"day\":1,\"utc\":\"12:59:00\","
            "\"flight\":\"AF250\"},{\"day\":1,\"utc\":\"04:45:00\","
            "\"flight\":\"AF130\"},{\"day\":2,\"utc\":\"00:31:00\","
            "\"flight\":\"AF997\"},{\"day\":2,\"utc\":\"19:41:00\","
            "\"flight\":\"AF223\"},{\"day\":2,\"utc\":\"15:14:00\","
            "\"flight\":\"AF890\"},{\"day\":2,\"utc\":\"00:30:00\","
            "\"flight\":\"AF399\"},{\"day\":2,\"utc\":\"16:18:00\","
            "\"flight\":\"AF328\"},{\"day\":3,\"utc\":\"23:50:00\","
            "\"flight\":\"AF074\"},{\"day\":3,\"utc\":\"11:33:00\","
            "\"flight\":\"AF556\"},{\"day\":4,\"utc\":\"13:23:00\","
            "\"flight\":\"AF064\"},{\"day\":4,\"utc\":\"12:09:00\","
            "\"flight\":\"AF596\"},{\"day\":4,\"utc\":\"08:02:00\","
            "\"flight\":\"AF818\"},{\"day\":5,\"utc\":\"11:33:00\","
            "\"flight\":\"AF967\"},{\"day\":5,\"utc\":\"19:42:00\","
            "\"flight\":\"AF730\"},{\"day\":6,\"utc\":\"17:07:00\","
            "\"flight\":\"AF882\"},{\"day\":6,\"utc\":\"17:03:00\","
            "\"flight\":\"AF485\"},{\"day\":6,\"utc\":\"10:01:00\","
            "\"flight\":\"AF898\"},{\"day\":6,\"utc\":\"07:00:00\","
            "\"flight\":\"AF496\"}],\"distance\":2881.617376098415}";
    benchmarkJsonValidator(validator, state, doc, true);
}

BENCHMARK_CAPTURE(BM_SyntaxValidator_SampleDocument,
                  JSON_checker,
                  SyntaxValidator::Type::JSON_checker);
BENCHMARK_CAPTURE(BM_SyntaxValidator_SampleDocument,
                  JSON_checker_vectorized,
                  SyntaxValidator::Type::JSON_checker_vectorized);
BENCHMARK_CAPTURE(BM_SyntaxValidator_SampleDocument,
                  Nlohmann,
                  SyntaxValidator::Type::Nlohmann);

// Using custom main() function (instead of BENCHMARK_MAIN macro) to
// init GoogleTest.
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(throw_on_failure) = true;
    ::testing::InitGoogleTest(&argc, argv);

    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
}
