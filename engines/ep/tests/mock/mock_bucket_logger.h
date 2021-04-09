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