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

    /**
     * Generate a (potentially) reduced range which does
     * not intersect with a second SeqRange.
     *
     * For example, if `this` covers {3,8} and `other` covers {1,5}
     * the result of this.makeNonOverlapping(other) covers {6,8}.
     *
     *
     * SeqnoRanges are inclusive of both ends, and therefore the
     * result cannot include the start or end seqno of `other`
     * (otherwise they would intersect).
     *
     * NB: There is one caveated case, noted below.
     *
     *                 1 2 3 4 5 6 7 8 9 10
     * this          :     | - - - - |
     * other SeqRange: | - - - |
     * result        :           | - |
     * Cases:
     *
     * A: no overlap
     * this          : | - - - |
     * other         :           | - |
     * result        : | - - - |
     *
     * B: overlap at end
     * this          : | - - - |
     * other         :       | - - |
     * result        : | - |
     *
     * C: overlap at start
     * this          :       | - - - |
     * other         :   | - - |
     * result        :           | - |
     *
     * D: fully contained
     * this          : | - - |
     * other         : | - - - - |
     * result        :    Not valid (returns invalid SeqRange)
     *
     * E: fully contains
     * this          : | - - - - - - - - |
     * other         :       | - - |
     * result        : | - |         | - |
     *
     * HOWEVER, for simplicity, rather than dealing with two discontinuous
     * ranges as the result, this is reduced to:
     *
     * result        : | - |
     *
     * That is, only the earlier range (as if in case B)
     *
     */
    SeqRange makeNonOverlapping(const SeqRange& other) const;

    void reset() {
        begin = 0;
        end = 0;
    }

    bool valid() const {
        return !(begin == 0 && end == 0);
    }

    explicit operator bool() const {
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

std::ostream& operator<<(std::ostream& os, const SeqRange& sr);

class RangeLockManager;

/**
 * Flag whether lockSeqnoRange should fail if the requested seqno range cannot
 * be locked (Exact) or try to reduce the requested range by moving begin
 * forward and/or end backward to reach a range which *can* be locked
 * (BestEffort)
 *
 * E.g., Deleting stale items requires exclusive access, but could operate on a
 * smaller range of seqnos if a backfill is in progress.
 */
enum class RangeRequirement { Exact, Partial };

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

    explicit operator bool() const {
        return valid();
    }

    /**
     * Get the range for which this guard is holding a lock. This *may*
     * be different than the requested range if the lock request permitted
     * partial ranges.
     */
    const SeqRange& getRange() const {
        return *itrToRange;
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
     * If req == RangeRequirement::Exact, the lock will cover the entire
     * requested range, or fail if existing range locks intersect that range. If
     * req == RangeRequirement::Partial, the lock will cover the entire
     * requested range if possible, but will lock part of the range if possible.
     * Callers should check the value of guard.getRange() to find what range of
     * seqnos was locked. Will fail if the entire requested range is covered by
     * existing range locks.
     *
     * @param start First seqno which must be protected from modification
     * (inclusive)
     * @param end Last seqno which must be protected from modification
     * (inclusive)
     * @param req flag indicating if a lock covering a smaller range of seqnos
     *            would be acceptable to the caller if the full range cannot be
     *            locked
     * @return an object which will release the range lock upon destruction, if
     * valid.
     */
    RangeGuard tryLockRange(seqno_t start,
                            seqno_t end,
                            RangeRequirement req = RangeRequirement::Exact);

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
         * range locks (largely limited by the max number of replicas).
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

    folly::Synchronized<LockedRanges, std::mutex> ranges;

    friend class RangeGuard;
};
