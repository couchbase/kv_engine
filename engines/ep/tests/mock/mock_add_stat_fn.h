/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
