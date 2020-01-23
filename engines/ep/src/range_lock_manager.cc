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

#include "range_lock_manager.h"

#include <boost/range/join.hpp>
#include <boost/range/numeric.hpp>

const SeqRange SeqRange::invalid = {0, 0};

SeqRange::SeqRange(seqno_t beginVal, seqno_t endVal)
    : begin(beginVal), end(endVal) {
    if ((end < begin) || (begin < 0)) {
        throw std::invalid_argument("Trying to create invalid SeqRange: [" +
                                    std::to_string(begin) + ", " +
                                    std::to_string(end) + "]");
    }
}

void SeqRange::setBegin(const seqno_t start) {
    if ((start <= 0) || (start > end)) {
        throw std::invalid_argument("Trying to set incorrect begin " +
                                    std::to_string(start) + " on SeqRange: [" +
                                    std::to_string(begin) + ", " +
                                    std::to_string(end) + "]");
    }
    begin = start;
}

SeqRange SeqRange::makeNonOverlapping(const SeqRange& other) const {
    if (!valid() || !other.valid()) {
        return {0, 0};
    }
    if (!overlaps(other)) {
        // case A (from comments in header)
        return *this;
    } else if (begin < other.begin) {
        // case B or E
        return {begin, other.begin - 1};
    } else if (end > other.end) {
        // case C
        return {other.end + 1, end};
    } else {
        // case D
        return {0, 0};
    }
}

std::ostream& operator<<(std::ostream& os, const SeqRange& sr) {
    return os << to_string(sr);
}

std::string to_string(const SeqRange& range) {
    using std::to_string;
    return std::string("{") + to_string(range.getBegin()) + ", " +
           to_string(range.getEnd()) + "}";
}

RangeGuard::RangeGuard(RangeGuard&& other) noexcept {
    rlm = other.rlm;
    itrToRange = other.itrToRange;
    exclusive = other.exclusive;

    // ensure other won't try to release the range on destruction
    other.invalidate();
}

RangeGuard& RangeGuard::operator=(RangeGuard&& other) noexcept {
    // release any existing range
    reset();

    rlm = other.rlm;
    itrToRange = other.itrToRange;
    exclusive = other.exclusive;

    // ensure other won't try to release the range on destruction
    other.invalidate();
    return *this;
}

RangeGuard::RangeGuard(RangeLockManager& rlm,
                       const ItrType& itr,
                       bool exclusive)
    : rlm(&rlm), itrToRange(itr), exclusive(exclusive) {
}

void RangeGuard::updateRangeStart(seqno_t newStart) {
    Expects(valid());
    rlm->updateRangeLockStart(itrToRange, newStart);
}

void RangeGuard::reset() {
    if (valid()) {
        rlm->release(itrToRange, exclusive);
    }
    invalidate();
}

/**
 * Create a range spanning both of the provided ranges.
 *
 * For convenience, param `a` may be invalid, in which case
 * rangeUnion(a, b) == b
 *
 * the second argument is expected to be valid.
 */
SeqRange rangeUnion(const SeqRange& a, const SeqRange& b) {
    Expects(b);
    if (!a) {
        return b;
    }
    return {std::min(a.getBegin(), b.getBegin()),
            std::max(a.getEnd(), b.getEnd())};
}
RangeGuard RangeLockManager::tryLockRange(seqno_t start,
                                          seqno_t end,
                                          RangeRequirement req) {
    auto r = ranges.lock();

    SeqRange requestedRange{start, end};

    auto blockingRanges = boost::range::join(r->shared, r->exclusive);

    switch (req) {
    case RangeRequirement::Exact:
        for (const auto& seqRange : blockingRanges) {
            if (requestedRange.overlaps(seqRange)) {
                // if any overlapping ranges locks which we cannot overlap are
                // present exit early, because this lock cannot proceed.
                return {};
            }
        }
        break;
    case RangeRequirement::Partial:
        // Partial locks can try to reduce the locked range if possible to
        // allow e.g., the stale item remove to run on a reduced range of
        // seqnos while backfills are in progress.
        for (const auto& seqRange : blockingRanges) {
            requestedRange = requestedRange.makeNonOverlapping(seqRange);
            if (!requestedRange) {
                // reducing the requested range down to avoid intersecting
                // with an existing range was not possible, locking failed.
                return {};
            }
        }
        break;
    }

    r->exclusive.push_back(requestedRange);

    // no need to clear the existing combined range, adding
    // a new range read can only expand it
    r->unionedRange = rangeUnion(r->unionedRange, requestedRange);

    return {*this, std::prev(r->exclusive.end()), true};
}

RangeGuard RangeLockManager::tryLockRangeShared(seqno_t start, seqno_t end) {
    auto r = ranges.lock();

    SeqRange requestedRange{start, end};

    for (const auto& exclusiveRange : r->exclusive) {
        if (requestedRange.overlaps(exclusiveRange)) {
            // if any overlapping exclusive ranges are present
            // exit early, because this lock cannot proceed.
            return {};
        }
    }

    r->shared.push_back(requestedRange);

    // no need to clear the existing combined range, adding
    // a new range read can only expand it
    r->unionedRange = rangeUnion(r->unionedRange, requestedRange);

    return {*this, std::prev(r->shared.end()), false};
}

void RangeLockManager::release(const RangeGuard::ItrType& itrToRange,
                               bool exclusive) {
    auto r = ranges.lock();

    auto& rangeList = (exclusive ? r->exclusive : r->shared);

    rangeList.erase(itrToRange);

    r->updateUnionedRange();
}

void RangeLockManager::updateRangeLockStart(
        const RangeGuard::ItrType& itrToRange, seqno_t newStart) {
    auto r = ranges.lock();

    auto previousStart = itrToRange->getBegin();

    if (newStart <= previousStart) {
        using std::to_string;
        throw std::logic_error(
                std::string("RangeLockManager::updateRangeLockStart: "
                            "tried to update start of range:") +
                to_string(*itrToRange) + "to :{" + to_string(newStart) + "}");
    }

    itrToRange->setBegin(newStart);

    if (previousStart == r->unionedRange.getBegin()) {
        // if the changed range read used to be the lower
        // bound of the overall protected range, by moving the
        // start, we *may* have reduced the overall range, if
        // no other range read starts at that seqno.
        // If this range read was *not* the lower bound,
        // we do not need to alter the combined range.
        r->updateUnionedRange();
    }
}

void RangeLockManager::LockedRanges::updateUnionedRange() {
    // clear the existing combined range, it is about to be regenerated
    // from the remaining range reads, and may cover a smaller range of seqnos.
    unionedRange.reset();

    if (shared.empty() && exclusive.empty()) {
        return;
    }

    const auto allRanges = boost::range::join(shared, exclusive);

    unionedRange = boost::accumulate(allRanges, SeqRange::invalid, rangeUnion);
}