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
    SeqRange(seqno_t beginVal, seqno_t endVal);

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

    operator bool() const {
        return valid();
    }

    std::pair<seqno_t, seqno_t> getRange() const {
        return {begin, end};
    }

    seqno_t getBegin() const {
        return begin;
    }

    void setBegin(const seqno_t start);

    seqno_t getEnd() const {
        return end;
    }

    const static SeqRange invalid;

private:
    seqno_t begin;
    seqno_t end;
};

std::string to_string(const SeqRange& range);

class RangeLockManager;

/**
 * A class providing RAII style release of read-ranges; see ReadRangeManager.
 */
class RangeGuard {
public:
    using ItrType = std::list<SeqRange>::iterator;

    RangeGuard() = default;
    ~RangeGuard() {
        reset();
    }

    RangeGuard(const RangeGuard&) = delete;
    RangeGuard& operator=(const RangeGuard&) = delete;

    RangeGuard(RangeGuard&& other) noexcept;
    RangeGuard& operator=(RangeGuard&& other) noexcept;

    /**
     * Move the start of the guarded range lock forwards.
     *
     * This movement is strictly monotonic, and calls with a seqno
     * less than or equal to the current range start will throw a
     * std::logic_error.
     * Should only be called on valid RangeGuards.
     *
     * @param newStart seqno to set the range start to.
     */
    void updateRangeStart(seqno_t newStart);

    /**
     * Check if the RangeGuard is initialized and currently
     * holds a range lock (which would be released on destruction).
     */
    bool valid() const {
        return rlm;
    }

    /**
     * Explicitly release the range lock and invalidate the RangeGuard.
     * Normally called on destruction.
     */
    void reset();

    operator bool() const {
        return valid();
    }

protected:
    /**
     * Invalidate the RangeGuard *without* trying to release the range lock, if
     * one is held.
     *
     * Invalidating an invalid RangeGuard is a no-op.
     */
    void invalidate() {
        rlm = nullptr;
    }

    // protected constructor, valid RangeGuards should not be directly
    // constructed; only RangeLockManager should do so.
    RangeGuard(RangeLockManager& rlm, const ItrType& itr, bool exclusive);

private:
    RangeLockManager* rlm = nullptr;
    // Iterator pointing to the SeqRange tracked within the RangeLockManager.
    // Not to be used directly. Passed to RangeLockManager methods to identify
    // which range lock this guard holds, e.g., to update to correct range
    // boulds in updateRangeStart, or to release the correct range on
    // destruction.
    ItrType itrToRange;
    bool exclusive = false;

    // RangeLockManager needs to access the protected constructor
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
     * seqlist need to (temporarily) not be relocated by front end ops so they
     * can be safely read - iff stale, they may also be removed.
     *
     * If the read range was successfully added, the returned RangeGuard will
     * be valid i.e., `rh.valid() == true` or `bool(rh) == true`. If the read
     * range could not be added, the seqnos _cannot_ be safely read and the
     * holder will not be valid. `valid()` should therefore always be checked.
     *
     * The added range lock will be removed when the returned RangeGuard is
     * destroyed.
     *
     * The requested lock requires that no overlapping range locks are present,
     * and prevents any new overlapping ones being created while exclusive
     * access is required. If an overlapping range lock is already present,
     * immediately returns an invalid RangeGuard without blocking.
     *
     * @param start First seqno which must be protected from modification
     * (inclusive)
     * @param end Last seqno which must be protected from modification
     * (inclusive)
     * @return an object which will release the range lock upon destruction, if
     * valid.
     */
    RangeGuard tryLockRange(seqno_t start, seqno_t end);

    /**
     * Variant of tryLockRange which permits concurrent read-only access to
     * the locked range; multiple shared locks may intersect. Requires that
     * there is no overlapping exclusive range lock present, and prevents any
     * new overlapping exclusive locks being created while held.
     */
    RangeGuard tryLockRangeShared(seqno_t start, seqno_t end);

    /**
     * * Get the current unioned locked range of seqnos.
     *
     * There may be multiple individual locks; the returned value is a range
     * spanning *all* current locks.
     */
    SeqRange getLockedRange() const {
        return ranges.lock()->unionedRange;
    }

protected:
    /**
     * Release the currently held range lock. Only to be used internally
     * by a RangeGuard.
     *
     * @param itrToRange iterator pointing to the range added when the
     *        lock was acquired.
     * @param exclusive if the locked range is exclusive
     */
    void release(const RangeGuard::ItrType& itrToRange, bool exclusive);

    /**
     * Advance the start of the currently locked range ("releasing" any seqnos
     * that are no longer within the range) and recompute the unioned range.
     * The range start may only be moved forwards.
     *
     *
     * Used through RangeGuard.
     */
    void updateRangeLockStart(const RangeGuard::ItrType& itrToRange,
                              seqno_t newStart);

    struct LockedRanges {
        /**
         * Compute a single range lock spanning all required individual ranges
         * and store it in `unionedRange`.
         */
        void updateUnionedRange();
        /**
         * Used to mark of the range where point-in-time snapshot is happening.
         * To get a valid point-in-time snapshot and for correct list iteration
         * we must not de-duplicate an item in the list in this range.
         *
         * This is a computed value, derived from the range locks in `all`.
         * It is generated when adding or removing values from `all`, so that
         * front end ops can test seqnos against a single range, rather than
         * against every element of `all`.
         */
        SeqRange unionedRange = SeqRange::invalid;
        /**
         * List of currently held read ranges which can "share" seqnos (are
         * allowed to overlap with each other).
         *
         * For simplicity (and front end performance) when multiple concurrent
         * read ranges are required, it is treated as if there is a single read
         * range spanning over all required seqnos - i.e., an effective read
         * range from the lowest range start to the highest range end. This
         * means if the range locks are disjoint, some items will be included
         * that don't strictly need to be - this may lead to more stale items
         * than necessary, but means that front end ops only need to check
         * existing seqlist items against a single range, rather than many.
         *
         * In general it is not expected that there will be many concurrent
         * range locks (largely limited by the max number of replicas), and they
         * are unlikely to be disjoint anyway.
         *
         * std::list used over std::vector/std::deque so ranges can be added and
         * randomly removed without invalidating the iterators held by
         * RangeGuards.
         */
        std::list<SeqRange> shared;

        /**
         * List of currently held ranges locks which require exclusive access to
         * the covered seqnos (may not overlap with each other or ranges in
         * `shared`).
         */
        std::list<SeqRange> exclusive;
    };

    folly::Synchronized<LockedRanges, SpinLock> ranges;

    friend class RangeGuard;
};