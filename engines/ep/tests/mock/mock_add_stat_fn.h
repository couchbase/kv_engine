/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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
#pragma once

#include <folly/portability/GMock.h>
#include <memcached/engine_common.h>

/**
 * Mock AddStatFn, implemented using GoogleMock.
 *
 * GoogleTest doesn't correctly print std::string_view if an expectation fails,
 * but does for std::string - therefore define a mock method taking
 * std::string and then operator() as a trampoline function taking
 * std::string_view and calling the GoogleMock method with std::string.
 *
 * To use, set expectations on the 'stat()' method then use asStdFunction()
 * to invoke:
 *
 *     MockAddStat mockAddStat;
 *     EXPECT_CALL(mockAddStat, stat("some_stat_key", "0", cookie));
 *     ...
 *     someMethodExpectingAddStatFn(..., mockAddStat.asStdFunction());
 */
struct MockAddStat {
    MOCK_CONST_METHOD3(callback, void(std::string, std::string, const void*));

    AddStatFn asStdFunction() {
        return [this](std::string_view key,
                      std::string_view value,
                      const void* cookie) {
            callback(std::string{key}, std::string{value}, cookie);
        };
    }
};
