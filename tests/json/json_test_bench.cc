/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
 * This file contains various benchmarks for both the cJSON and JSON for Modern
 * C++ libraries Each library has the test implemented in its "standard" way so
 * we can compare the performance against each other. This is not intended to be
 * a complete performance test of either library.
 */

#include "config.h"

#include <benchmark/benchmark.h>
#include <cJSON.h>
#include <cJSON_utils.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

class JSONBenchmark : public ::benchmark::Fixture {
    void SetUp(benchmark::State& st) override {
        cb::io::sanitizePath(jsonFilePath);
        loaded_json = cb::io::loadFile(jsonFilePath);
        json = nlohmann::json::parse(loaded_json);
        cjson = cJSON_Parse(loaded_json.c_str());
    }

    void TearDown(benchmark::State& st) override {
        cJSON_Delete(cjson);
    }

protected:
    std::string jsonFilePath = SOURCE_ROOT "/tests/json/testdata.json";
    std::string loaded_json;
    nlohmann::json json;
    cJSON* cjson;
};

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_ParseFile)(benchmark::State& state) {
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(nlohmann::json::parse(loaded_json));
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_ParseFile)(benchmark::State& state) {
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(cJSON_Parse(loaded_json.c_str()));
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_AddStringToJson)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        for (auto i = 0; i < state.range(0); i++) {
            std::string key = "test_" + std::to_string(i);
            json[key] = "this is a test string";
        }
        state.PauseTiming();
        json = nlohmann::json::parse(loaded_json);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_AddStringToJson)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        for (auto i = 0; i < state.range(0); i++) {
            std::string key = "test_" + std::to_string(i);
            cJSON_AddStringToObject(
                    cjson, key.c_str(), "this is a test string");
        }
        state.PauseTiming();
        cjson = cJSON_Parse(loaded_json.c_str());
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_ElementAccess_PushBack)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        auto array = json["array"];
        for (auto i = 0; i < state.range(0); i++) {
            array.push_back("test string");
        }
        state.PauseTiming();
        json = nlohmann::json::parse(loaded_json);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_ElementAccess_PushBack)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        auto* array = cJSON_GetObjectItem(cjson, "array");
        for (auto i = 0; i < state.range(0); i++) {
            cJSON_AddItemToArray(array, cJSON_CreateString("test string"));
        }
        state.PauseTiming();
        cjson = cJSON_Parse(loaded_json.c_str());
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_DumpToString)(benchmark::State& state) {
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(json.dump());
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_DumpToString)(benchmark::State& state) {
    while (state.KeepRunning()) {
        benchmark::DoNotOptimize(::to_string(cjson, false));
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_AddObjectToJSON)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        for (auto i = 0; i < state.range(0); i++) {
            state.PauseTiming();
            nlohmann::json object;
            object["foo"] = "bar";
            object["bar"] = "foo";
            std::string key = "test_" + std::to_string(i);
            i++;
            state.ResumeTiming();

            json[key] = object;
        }
        state.PauseTiming();
        json = nlohmann::json::parse(loaded_json);
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_AddObjectToJSON)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        for (auto i = 0; i < state.range(0); i++) {
            state.PauseTiming();
            cJSON* object = cJSON_CreateObject();
            cJSON_AddStringToObject(object, "foo", "bar");
            cJSON_AddStringToObject(object, "bar", "foo");
            std::string key = "test_" + std::to_string(i);
            i++;
            state.ResumeTiming();

            cJSON_AddItemToObject(cjson, key.c_str(), object);
        }
        state.PauseTiming();
        cjson = cJSON_Parse(loaded_json.c_str());
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_EraseObjectFromJSON)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        state.PauseTiming();
        nlohmann::json object;
        object["foo"] = "bar";
        object["bar"] = "foo";
        std::string key = "test_key";
        json[key] = object;
        state.ResumeTiming();

        json.erase(key);
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_EraseObjectFromJSON)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        state.PauseTiming();
        cJSON* object = cJSON_CreateObject();
        cJSON_AddStringToObject(object, "foo", "bar");
        cJSON_AddStringToObject(object, "bar", "foo");
        std::string key = "test_key";
        cJSON_AddItemToObject(cjson, key.c_str(), object);
        state.ResumeTiming();

        cJSON_DetachItemFromObject(cjson, key.c_str());
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_FindElement_Exists)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        auto a = json.find("array");
        ASSERT_NE(a, json.end());
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_FindElement_Exists)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        auto* a = cJSON_GetObjectItem(cjson, "array");
        ASSERT_NE(a, nullptr);
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, JMC_FindElement_NotExists)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        auto a = json.find("not here");
        ASSERT_EQ(a, json.end());
    }
}

BENCHMARK_DEFINE_F(JSONBenchmark, cJSON_FindElement_NotExists)
(benchmark::State& state) {
    while (state.KeepRunning()) {
        auto* a = cJSON_GetObjectItem(cjson, "not here");
        ASSERT_EQ(a, nullptr);
    }
}

BENCHMARK_REGISTER_F(JSONBenchmark, JMC_ParseFile);
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_ParseFile);
BENCHMARK_REGISTER_F(JSONBenchmark, JMC_AddStringToJson)
        ->Args({1})
        ->Args({100});
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_AddStringToJson)
        ->Args({1})
        ->Args({100});
BENCHMARK_REGISTER_F(JSONBenchmark, JMC_ElementAccess_PushBack)
        ->Args({1})
        ->Args({100});
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_ElementAccess_PushBack)
        ->Args({1})
        ->Args({100});
BENCHMARK_REGISTER_F(JSONBenchmark, JMC_DumpToString);
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_DumpToString);
BENCHMARK_REGISTER_F(JSONBenchmark, JMC_AddObjectToJSON)
        ->Args({1})
        ->Args({100});
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_AddObjectToJSON)
        ->Args({1})
        ->Args({100});
BENCHMARK_REGISTER_F(JSONBenchmark, JMC_EraseObjectFromJSON);
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_EraseObjectFromJSON);
BENCHMARK_REGISTER_F(JSONBenchmark, JMC_FindElement_Exists);
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_FindElement_Exists);
BENCHMARK_REGISTER_F(JSONBenchmark, JMC_FindElement_NotExists);
BENCHMARK_REGISTER_F(JSONBenchmark, cJSON_FindElement_NotExists);

BENCHMARK_MAIN()