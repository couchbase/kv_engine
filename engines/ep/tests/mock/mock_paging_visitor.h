/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "paging_visitor.h"

#include <string>

#include <gmock/gmock.h>

/**
 * Mock PagingVisitor class.  Provide access to ItemEviction data structure.
 */
class MockPagingVisitor : public PagingVisitor {
public:
    MockPagingVisitor(KVBucket& s,
                      EPStats& st,
                      double pcnt,
                      std::shared_ptr<std::atomic<bool>>& sfin,
                      pager_type_t caller,
                      bool pause,
                      double bias,
                      const VBucketFilter& vbFilter,
                      std::atomic<item_pager_phase>* phase,
                      bool _isEphemeral,
                      size_t agePercentage,
                      size_t freqCounterAgeThreshold,
                      EvictionPolicy evictionPolicy)
        : PagingVisitor(s,
                        st,
                        pcnt,
                        sfin,
                        caller,
                        pause,
                        bias,
                        vbFilter,
                        phase,
                        _isEphemeral,
                        agePercentage,
                        freqCounterAgeThreshold,
                        evictionPolicy) {
        using namespace testing;
        ON_CALL(*this, visitBucket(_))
                .WillByDefault(Invoke([this](VBucketPtr& vb) {
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

    void setCurrentBucket(VBucketPtr _currentBucket) {
        currentBucket = _currentBucket;
    }

    MOCK_METHOD1(visitBucket, void(VBucketPtr&));
};
