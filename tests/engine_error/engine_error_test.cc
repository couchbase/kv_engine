/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <gtest/gtest.h>
#include <memcached/engine_error.h>
#include <system_error>

TEST(EngineError, test_get_category) {
    auto& category = cb::engine_error_category();
    EXPECT_EQ("success", category.message(ENGINE_SUCCESS));
}

TEST(EngineError, test_invalid_value) {
    auto& category = cb::engine_error_category();
    try {
        std::string message = category.message(-1);
        FAIL() << "Should throw invalid_arguemnt for unknown values";
    } catch (const std::invalid_argument& ex) {
        EXPECT_STREQ(
                "engine_error_category::message: code does not represent a "
                "legal error code: -1",
                ex.what());
    }
}

TEST(EngineError, test_engine_error_with_scoped_enums) {
    cb::engine_error error(cb::engine_errc::no_memory, "foo");
    EXPECT_EQ(cb::engine_errc::no_memory, error.code());
    EXPECT_STREQ("engine error codes", error.code().category().name());
    EXPECT_STREQ("foo: no memory", error.what());
}

TEST(EngineError, test_system_error) {
    try {
        throw cb::engine_error(cb::engine_errc::no_memory, "foo");
    } catch (const std::system_error& error) {
        EXPECT_EQ(cb::engine_errc::no_memory, error.code());
        EXPECT_STREQ("engine error codes", error.code().category().name());
        EXPECT_STREQ("foo: no memory", error.what());
    }
}
