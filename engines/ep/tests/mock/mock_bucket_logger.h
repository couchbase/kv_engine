/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "bucket_logger.h"

#include <folly/portability/GMock.h>

/**
 * The MockBucket Logger is used to verify that the logger is called with
 * certain parameters / messages.
 *
 * The MockBucketLogger calls the log method as normal, and intercepts the
 * _sink_it call by overriding it to determine the correctness of the logging
 */
class MockBucketLogger : public BucketLogger {
public:
    MockBucketLogger(std::string name) : BucketLogger(name) {
        // Set the log level of the BucketLogger to trace to ensure messages
        // make it through to the sink it method. Does not alter the logging
        // level of the underlying spdlogger so we will not see console
        // output during the test.
        set_level(spdlog::level::level_enum::trace);
        using namespace testing;
        ON_CALL(*this, mlog(_, _))
                .WillByDefault(Invoke([](spdlog::level::level_enum sev,
                                         const std::string& msg) {}));
    }

    // Mock a method taking a logging level and formatted message to test log
    // outputs.
    MOCK_CONST_METHOD2(mlog,
                       void(spdlog::level::level_enum severity,
                            const std::string& message));

protected:
    // Override the sink_it_ method to redirect to the mocked method
    // Must call the mlog method to check the message details as they are
    // bundled in the log_msg object. Beware, msg.raw is not null terminated.
    // In these test cases however we just search for a substring within the log
    // message so this is okay.
    void sink_it_(spdlog::details::log_msg& msg) override {
        mlog(msg.level, msg.raw.data());
    }
};