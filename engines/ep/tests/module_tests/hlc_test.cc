/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "hlc.h"

#include "tests/module_tests/thread_gate.h"

#include <folly/portability/GTest.h>
#include <thread>

struct MockClock {
    static std::chrono::milliseconds currentTime;

    static std::chrono::time_point<std::chrono::system_clock> now() {
        return std::chrono::time_point<std::chrono::system_clock>{
                std::chrono::system_clock::duration{currentTime}};
    }
};

std::chrono::milliseconds MockClock::currentTime{1};

template <class Clock>
class MockHLC : public HLCT<Clock> {
public:
    MockHLC(uint64_t initHLC,
            int64_t epochSeqno,
            std::chrono::microseconds aheadThreshold,
            std::chrono::microseconds behindThreshold)
        : HLCT<Clock>(initHLC, epochSeqno, aheadThreshold, behindThreshold) {
    }

    void setNonLogicalClockGetNextCasHook(std::function<void()> hook) {
        HLCT<Clock>::nonLogicalClockGetNextCasHook = hook;
    }
    void setLogicalClockGetNextCasHook(std::function<void()> hook) {
        HLCT<Clock>::logicalClockGetNextCasHook = hook;
    }
};

class HLCTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Currently don't care what threshold we have for ours tests,
        std::chrono::microseconds threshold;
        testHLC = std::make_unique<MockHLC<MockClock>>(
                0, 0, threshold, threshold);
    }

    std::unique_ptr<MockHLC<MockClock>> testHLC;
};

TEST_F(HLCTest, RaceLogicalClockIncrementAtSameTime) {
    testHLC->forceMaxHLC(std::numeric_limits<uint32_t>::max());

    ThreadGate tg1(2);

    testHLC->setLogicalClockGetNextCasHook([&tg1]() { tg1.threadUp(); });

    uint64_t threadCas;
    auto thread = std::thread(
            [this, &threadCas]() { threadCas = testHLC->nextHLC(); });

    auto thisCas = testHLC->nextHLC();

    thread.join();

    EXPECT_NE(threadCas, thisCas);
}