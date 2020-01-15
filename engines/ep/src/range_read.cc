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

#include "range_read.h"

void RangeGuard::updateRangeStart(seqno_t newStart) {
    if (valid()) {
        rlm->updateRangeLockStart(newStart);
        range.setBegin(newStart);
    }
}

void RangeGuard::reset() {
    if (valid()) {
        rlm->release(range);
    }
    invalidate();
}

RangeGuard RangeLockManager::tryLockRange(seqno_t start, seqno_t end) {
    auto existing = range.lock();

    if (existing->valid()) {
        // only a single range read is currently allowed, if there is
        // an existing range read set then we cannot acquire a new one
        return {};
    }

    SeqRange newRange{start, end};

    (*existing) = newRange;

    return {*this, newRange};
}
void RangeLockManager::release(const SeqRange& seqRange) {
    auto rr = range.lock();

    Expects(*rr == seqRange);

    rr->reset();
}

void RangeLockManager::updateRangeLockStart(seqno_t newStart) {
    auto r = range.lock();
    Expects(newStart > r->getBegin());
    r->setBegin(newStart);
}