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
 * Mock PagingVisitor class.  Provide access to ItemEviction data structure.
 */
class MockPagingVisitor : public PagingVisitor {
public:
    MockPagingVisitor(KVBucket& s,
                      EPStats& st,
                      EvictionRatios evictionRatios,
                      std::shared_ptr<cb::Semaphore> pagerSemaphore,
                      pager_type_t caller,
                      bool pause,
                      const VBucketFilter& vbFilter,
                      size_t agePercentage,
                      size_t freqCounterAgeThreshold)
        : PagingVisitor(s,
                        st,
                        evictionRatios,
                        std::move(pagerSemaphore),
                        caller,
                        pause,
                        vbFilter,
                        agePercentage,
                        freqCounterAgeThreshold) {
        using namespace testing;
        ON_CALL(*this, visitBucket(_))
                .WillByDefault(Invoke([this](VBucket& vb) {
                    PagingVisitor::visitBucket(vb);
                }));
    }

    ItemEviction& getItemEviction() {
        return itemEviction;
    }

    size_t getEjected() const {
        return ejected;
    }

    void setFreqCounterThreshold(uint16_t threshold) {
        freqCounterThreshold = threshold;
    }

    void setCurrentBucket(VBucket& _currentBucket) {
        currentBucket = &_currentBucket;
    }

    MOCK_METHOD1(visitBucket, void(VBucket&));
};
