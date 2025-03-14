/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "item.h"
#include "paging_visitor.h"
#include <string>

#include <folly/portability/GMock.h>

/**
 * Mock ItemPagingVisitor class.  Provide access to ItemEviction data structure.
 */
class MockItemPagingVisitor : public ItemPagingVisitor {
public:
    MockItemPagingVisitor(KVBucket& s,
                          EPStats& st,
                          std::unique_ptr<ItemEvictionStrategy> strategy,
                          std::shared_ptr<cb::Semaphore> pagerSemaphore,
                          bool pause,
                          const VBucketFilter& vbFilter)
        : ItemPagingVisitor(s,
                            st,
                            std::move(strategy),
                            std::move(pagerSemaphore),
                            pause,
                            vbFilter) {
        using namespace testing;
        ON_CALL(*this, visitBucket(_))
                .WillByDefault(Invoke([this](VBucket& vb) {
                    ItemPagingVisitor::visitBucket(vb);
                }));
    }

    void setItemEvictionStrategy(
            std::unique_ptr<ItemEvictionStrategy> newStrategy) {
        evictionStrategy = std::move(newStrategy);
    }

    ItemEvictionStrategy& getItemEvictionStrategyString() {
        return *evictionStrategy;
    }

    size_t getEjected() const {
        return ejected;
    }

    void setFreqCounterThreshold(uint16_t threshold) {
        // TODO: once tests are updated to inject the ItemEviction object
        //       into PagingVisitors when constructing them, this casting
        //       will no longer be necessary, and the freqCounter can be
        static_cast<LearningAgeAndMFUBasedEviction&>(*evictionStrategy)
                .setFreqCounterThreshold(threshold);
    }

    void setCurrentBucket(VBucket& _currentBucket) {
        currentBucket = &_currentBucket;
    }

    MOCK_METHOD1(visitBucket, void(VBucket&));
};

/**
 * Mock ExpiredPagingVisitor class.
 */
class MockExpiredPagingVisitor : public ExpiredPagingVisitor {
public:
    MockExpiredPagingVisitor(KVBucket& s,
                             EPStats& st,
                             std::shared_ptr<cb::Semaphore> pagerSemaphore,
                             bool pause,
                             const VBucketFilter& vbFilter)
        : ExpiredPagingVisitor(
                  s, st, std::move(pagerSemaphore), pause, vbFilter) {
        using namespace testing;
        ON_CALL(*this, visitBucket(_))
                .WillByDefault(Invoke([this](VBucket& vb) {
                    ExpiredPagingVisitor::visitBucket(vb);
                }));
    }

    void setCurrentBucket(VBucket& _currentBucket) {
        currentBucket = &_currentBucket;
    }

    MOCK_METHOD1(visitBucket, void(VBucket&));

    using ExpiredPagingVisitor::getExpiredItems;
};