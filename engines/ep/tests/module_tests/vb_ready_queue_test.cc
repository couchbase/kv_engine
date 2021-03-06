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

#include "vb_ready_queue.h"

#include <folly/portability/GTest.h>

class VBReadyQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        queue = std::make_unique<VBReadyQueue>(1024);
    }

    void TearDown() override {
    }

    std::unique_ptr<VBReadyQueue> queue;
};

TEST_F(VBReadyQueueTest, pushEmptyReturnsUnique) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    EXPECT_FALSE(queue->pushUnique(Vbid(0)));
}

TEST_F(VBReadyQueueTest, pushEmptyReturnsUniqueAfterPop) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    EXPECT_FALSE(queue->pushUnique(Vbid(0)));

    Vbid vbid;
    EXPECT_TRUE(queue->popFront(vbid));
    EXPECT_EQ(Vbid(0), vbid);

    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    EXPECT_FALSE(queue->pushUnique(Vbid(0)));
}

TEST_F(VBReadyQueueTest, insertionOrdering) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    EXPECT_FALSE(queue->pushUnique(Vbid(5)));
    EXPECT_FALSE(queue->pushUnique(Vbid(2)));

    Vbid vbid;
    EXPECT_TRUE(queue->popFront(vbid));
    EXPECT_EQ(Vbid(0), vbid);

    EXPECT_TRUE(queue->popFront(vbid));
    EXPECT_EQ(Vbid(5), vbid);

    EXPECT_TRUE(queue->popFront(vbid));
    EXPECT_EQ(Vbid(2), vbid);
}

#ifndef NDEBUG
// Check that at varying stages of pop, a push will return true (notify)
TEST_F(VBReadyQueueTest, PushAfterPopSizeLoad) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    queue->popFrontAfterSizeLoad = ([&]() {
        EXPECT_FALSE(queue->pushUnique(Vbid(0)));
        EXPECT_EQ(1, queue->size());
    });

    Vbid vbid;
    EXPECT_TRUE(queue->popFront(vbid));
}

TEST_F(VBReadyQueueTest, PushAfterPopQueueReady) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    queue->popFrontAfterQueueRead = ([&]() {
        EXPECT_FALSE(queue->pushUnique(Vbid(0)));
        EXPECT_EQ(1, queue->size());
    });

    Vbid vbid;
    EXPECT_TRUE(queue->popFront(vbid));
}

TEST_F(VBReadyQueueTest, PushAfterPopSizeSub) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    queue->popFrontAfterSizeSub = ([&]() {
        EXPECT_TRUE(queue->pushUnique(Vbid(0)));
        EXPECT_EQ(1, queue->size());
    });

    Vbid vbid;
    EXPECT_TRUE(queue->popFront(vbid));
}

TEST_F(VBReadyQueueTest, PushAfterPopQueuedValueSet) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));
    queue->popFrontAfterQueuedValueSet = ([&]() {
        EXPECT_FALSE(queue->pushUnique(Vbid(0)));
        EXPECT_EQ(2, queue->size());
    });

    Vbid vbid;
    EXPECT_TRUE(queue->popFront(vbid));

    EXPECT_EQ(1, queue->size());
}

// Check that at varying stages of push, a pop being run will still return the
// correct result
TEST_F(VBReadyQueueTest, PopAfterPushBeforeQueueWrite) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));

    queue->pushUniqueQueuedValuesUpdatedPreQueueWrite = [&]() {
        Vbid vbid;
        EXPECT_TRUE(queue->popFront(vbid));
    };

    EXPECT_FALSE(queue->pushUnique(Vbid(0)));
}

TEST_F(VBReadyQueueTest, PopAfterPushAfterQueueWrite) {
    EXPECT_TRUE(queue->pushUnique(Vbid(0)));

    queue->pushUniqueQueuedValuesUpdatedPostQueueWrite = [&]() {
        Vbid vbid;
        EXPECT_TRUE(queue->popFront(vbid));
    };

    EXPECT_TRUE(queue->pushUnique(Vbid(1)));
}

#endif
