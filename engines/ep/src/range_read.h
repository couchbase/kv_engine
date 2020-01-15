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
#pragma once

#include "seqlist.h"

#include <boost/optional.hpp>
#include <folly/Synchronized.h>
#include <list>

/**
 * Class that represents a range of sequence numbers.
 * SeqRange is closed, that is, both begin and end are inclusive.
 *
 * Note: begin <= 0 is considered an default/inactive range and can be set
 *       only by ctor or by reset.
 */
class SeqRange {
public:
    SeqRange(seqno_t beginVal, seqno_t endVal) : begin(beginVal), end(endVal) {
        if ((end < begin) || (begin < 0)) {
            throw std::invalid_argument("Trying to create invalid SeqRange: [" +
                                        std::to_string(begin) + ", " +
                                        std::to_string(end) + "]");
        }
    }

    bool operator==(const SeqRange& other) const {
        return begin == other.begin && end == other.end;
    }

    bool operator!=(const SeqRange& other) const {
        return !(*this == other);
    }

    /**
     * Returns true if the range overlaps with another.
     */
    bool overlaps(const SeqRange& other) const {
        return std::max(begin, other.begin) <= std::min(end, other.end);
    }

    /**
     *  Returns true if the seqno falls in the range
     */
    bool contains(const seqno_t seqno) const {
        return (seqno >= begin) && (seqno <= end);
    }

    void reset() {
        begin = 0;
        end = 0;
    }

    bool valid() const {
        return !(begin == 0 && end == 0);
    }

    std::pair<seqno_t, seqno_t> getRange() const {
        return {begin, end};
    }

    seqno_t getBegin() const {
        return begin;
    }

    void setBegin(const seqno_t start) {
        if ((start <= 0) || (start > end)) {
            throw std::invalid_argument(
                    "Trying to set incorrect begin " + std::to_string(start) +
                    " on SeqRange: [" + std::to_string(begin) + ", " +
                    std::to_string(end) + "]");
        }
        begin = start;
    }

    seqno_t getEnd() const {
        return end;
    }

private:
    seqno_t begin;
    seqno_t end;
};

class RangeLockManager;

/**
 * A class providing RAII style release of read-ranges; see ReadRangeManager.
 */
class RangeGuard {
public:
    RangeGuard() = default;
    ~RangeGuard() {
        reset();
    }

    RangeGuard(const RangeGuard&) = delete;
    RangeGuard& operator=(const RangeGuard&) = delete;

    RangeGuard(RangeGuard&& other) noexcept {
        rlm = other.rlm;
        range = other.range;

        // ensure other won't try to release the range on destruction
        other.invalidate();
    }

    RangeGuard& operator=(RangeGuard&& other) noexcept {
        // release any existing range
        reset();

        rlm = other.rlm;
        range = other.range;

        // ensure other won't try to release the range on destruction
        other.invalidate();
        return *this;
    }

    void updateRangeStart(seqno_t newStart);

    /**
     * Check if the RangeGuard is initialized and currently
     * holds a range read (which would be released on destruction).
     */
    bool valid() const {
        return rlm;
    }

    /**
     * Explicitly release the range read and invalidate the RangeGuard.
     * Normally called on destruction.
     */
    void reset();

    operator bool() const {
        return valid();
    }

protected:
    /**
     * Invalidate the RangeGuard *without* trying to release the range read, if
     * one is held.
     *
     * Invalidating an invalid RangeGuard is a no-op.
     */
    void invalidate() {
        rlm = nullptr;
    }
    // protected constructor, valid RangeReadHolders should not be directly
    // constructed; only RangeLockManager should do so.
    RangeGuard(RangeLockManager& rlm, const SeqRange& range)
        : rlm(&rlm), range(range) {
    }

private:
    RangeLockManager* rlm = nullptr;
    SeqRange range{0, 0};

    friend class RangeLockManager;
};

/**
 * Tracks ranges of seqnos in the seqlist which  have already "promised"
 * to keep for the lifetime of a RangeIterator (e.g., for backfill) so
 * it can read all seqnos in a given range; we cannot delete (de-duplicate)
 * such seqnos *if* they were to be modified.
 *
 * Currently only a single range is tracked but in the future multiple
 * ranges will be permitted.
 */
class RangeLockManager {
public:
    /**
     * Attempt to add a read range; a range of seqnos for which the items in the
     * seqlist need to (temporarily) not be modified so they can be safely read
     * (e.g., for backfill).
     *
     * Currently only a single read range is permitted at a given time.
     *
     * If the read range was successfully added, the returned RangeHolder will
     * be valid i.e., `rh.valid() == True` or `bool(rh) == True`. If the read
     * range could not be added, the seqnos _cannot_ be safely read and the
     * holder will not be valid.
     *
     * `valid()` should therefore always be checked.
     *
     * The added range read will be removed when the returned RangeHolder is
     * destroyed.
     *
     *
     * @param start First seqno which must be protected from modification
     * (inclusive)
     * @param end Last seqno which must be protected from modification
     * (inclusive)
     * @return an object which will release the range read upon destruction, if
     * valid.
     */
    RangeGuard tryLockRange(seqno_t start, seqno_t end);

    /**
     * Get the current range of seqnos which are locked.
     */
    SeqRange getLockedRange() const {
        return *range.lock();
    }

protected:
    /**
     * Release the currently held range lock. Only to be used internally
     * by a RangeGuard.
     *
     * @param range the range to be released; used to sanity check that
     *              the caller is trying to release the correct range.
     */
    void release(const SeqRange& range);

    /**
     * Advance the start of the currently locked range, "releasing" any seqnos
     * that are no longer within the range.
     */
    void updateRangeLockStart(seqno_t newStart);

    /**
     * Used to mark of the range where point-in-time snapshot is happening.
     * To get a valid point-in-time snapshot and for correct list iteration we
     * must not de-duplicate an item in the list in this range.
     */
    folly::Synchronized<SeqRange, SpinLock> range{{0, 0}};

    friend class RangeGuard;
};