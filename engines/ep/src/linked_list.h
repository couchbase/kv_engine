/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * This header file contains the class definition of one of the implementation
 * of the abstract class SequenceList
 */

#pragma once

#include "range_lock_manager.h"
#include "seqlist.h"
#include "stored-value.h"
#include <boost/intrusive/list.hpp>
#include <folly/Synchronized.h>
#include <platform/atomic.h>
#include <platform/monotonic.h>
#include <platform/non_negative_counter.h>
#include <relaxed_atomic.h>

/* This option will configure "list" to use the member hook */
using MemberHookOption =
        boost::intrusive::member_hook<OrderedStoredValue,
                                      boost::intrusive::list_member_hook<>,
                                      &OrderedStoredValue::seqno_hook>;

/* This list will use the member hook */
using OrderedLL = boost::intrusive::list<OrderedStoredValue, MemberHookOption>;


/**
 * This class implements SequenceList as a basic doubly linked list.
 * Uses boost intrusive list for doubly linked list implementation.
 *
 * Intrusive hook is to be added to OrderedStoredValue for it to be used in the
 * BasicLinkedList. Once in the BasicLinkedList, OrderedStoredValue is now
 * shared between HashTable and BasicLinkedList.
 *
 * BasicLinkedList sees only the hook for next and prev; HashTable
 * see only the hook for hashtable chaining.
 *
 * But there should be an agreement on the deletion (invalidation of next and
 * prev link; chaining link) of the elements between these 2 class objects.
 * Currently,
 * (i) HashTable owns a OrderedStoredValue (as a unique_ptr) that is not stale.
 * (ii) It relinquishes the ownership by marking it stale. This happens when
 *      deduplication is not possible and we want to keep an old value around.
 * (iii) BasicLinkedList deletes the stale OrderedStoredValues.
 * (iv) During a Hashtable clear (full or partial), which happens during
 *      VBucket delete or rollback, we first remove the element from
 *      BasicLinkedList (invalidate next, prev links) and then delete from the
 *      hashtable.
 *
 * Ordering/Hierarchy of Locks:
 * ===========================
 * BasicLinkedList has 3 locks namely:
 * (i) writeLock (ii) rangeLock (iii) rangeReadLock
 * Description of each lock can be found below in the class declaration, here
 * we describe in what order the locks should be grabbed
 *
 * rangeReadLock ==> writeLock ==> rangeLock is the valid lock hierarchy.
 *
 * Preferred/Expected Lock Duration:
 * ================================
 * 'writeLock' and 'rangeLock' are held for short durations, typically for
 * single list element writes and reads.
 * 'rangeReadLock' is held for longer duration on the list (for entire range).
 */
class BasicLinkedList : public SequenceList {
public:
    BasicLinkedList(Vbid vbucketId, EPStats& st);

    ~BasicLinkedList() override;

    void appendToList(std::lock_guard<std::mutex>& seqLock,
                      std::lock_guard<std::mutex>& writeLock,
                      OrderedStoredValue& v) override;

    SequenceList::UpdateStatus updateListElem(
            std::lock_guard<std::mutex>& seqLock,
            std::lock_guard<std::mutex>& writeLock,
            OrderedStoredValue& v) override;

    void updateHighSeqno(std::lock_guard<std::mutex>& listWriteLg,
                         const OrderedStoredValue& v) override;

    void maybeUpdateMaxVisibleSeqno(std::lock_guard<std::mutex>& seqLock,
                                    std::lock_guard<std::mutex>& writeLock,
                                    const OrderedStoredValue& newSV) override;

    void updateHighCompletedSeqno(std::lock_guard<std::mutex>& seqLock,
                                  std::lock_guard<std::mutex>& writeLock,
                                  int64_t hcs) override;

    void markItemStale(std::lock_guard<std::mutex>& listWriteLg,
                       StoredValue::UniquePtr ownedSv,
                       StoredValue* newSv) override;

    size_t purgeTombstones(
            seqno_t purgeUpToSeqno,
            Collections::IsDroppedEphemeralCb isDroppedKeyCb =
                    [](const DocKey, int64_t) { return false; },
            std::function<bool()> shouldPause =
                    []() { return false; }) override;

    void updateNumDeletedItems(bool oldDeleted, bool newDeleted) override;

    uint64_t getNumStaleItems() const override;

    size_t getStaleValueBytes() const override;

    size_t getStaleMetadataBytes() const override;

    uint64_t getNumDeletedItems() const override;

    uint64_t getNumItems() const override;

    uint64_t getHighSeqno() const override;

    seqno_t getHighestPurgedDeletedSeqno() const override;

    uint64_t getMaxVisibleSeqno() const override;

    uint64_t getHighCompletedSeqno() const override;
    uint64_t getHighCompletedSeqno(
            std::lock_guard<std::mutex>& writeLock) const override;

    std::pair<uint64_t, uint64_t> getRangeRead() const override;

    std::mutex& getListWriteLock() const override;

    std::optional<SequenceList::RangeIterator> makeRangeIterator(
            bool isBackfill) override;

    /**
     * Exclusively locks a range of seqnos in the sequence list. Prevents any
     * other range locks being acquired over an intersecting range.
     * Stops front end ops updating and relocating items in the given seqno
     * range, allowing them to be safely read, and safely modified/removed iff
     * stale. See `ReadRangeManager::tryLockRange`.
     *
     * If an existing range lock intersects the requested range the lock attempt
     * will fail (returning an invalid RangeGuard).
     * If successfully acquired, the lock will prevent new range locks being
     * acquired over any intersecting range until it is released.
     *
     * If req == RangeRequirement::Exact, the lock will cover the entire
     * requested range, or fail if existing range locks intersect that range. If
     * req == RangeRequirement::Partial, the lock will cover the entire
     * requested range if possible, but will lock part of the range if possible.
     * Callers should check the value of guard.getRange() to find what range of
     * seqnos was locked. Will fail if the entire requested range is covered by
     * existing range locks.
     *
     * @param start requested range start
     * @param end requested range end
     * @param req flag indicating if a lock covering a smaller range of seqnos
     *            would be acceptable to the caller if the full range cannot be
     *            locked
     * @return
     */
    RangeGuard tryLockSeqnoRange(
            seqno_t start,
            seqno_t end,
            RangeRequirement req = RangeRequirement::Exact);

    /**
     * Locks a range of seqnos in the sequence list. Prevents any *exclusive*
     * range locks being acquired over an intersecting range while also stopping
     * front end ops updating and relocating items in the given seqno range,
     * allowing them to be safely read. See
     * `ReadRangeManager::tryLockRangeShared`.
     *
     * The lock permits other shared range locks to intersect the covered range.
     * * If an existing *exclusive* range lock intersects the requested range
     * the lock attempt will fail (returning an invalid RangeGuard). If
     * successfully acquired, the lock will prevent *exclusive* range locks
     * being acquired over any intersecting range until it is released.
     */
    RangeGuard tryLockSeqnoRangeShared(seqno_t start, seqno_t end);

    void dump(std::ostream& ostream) const override;

protected:
    /* Underlying data structure that holds the items in an Ordered Sequence */
    OrderedLL seqList;

    /**
     * Lock that serializes writes (append, update, purgeTombstones) on
     * 'seqList' + the updation of the corresponding highSeqno or the
     * highestDedupedSeqno atomic
     */
    mutable std::mutex writeLock;

    /**
     * Used to mark of the range where point-in-time snapshot is happening.
     * To get a valid point-in-time snapshot and for correct list iteration we
     * must not de-duplicate an item in the range tracked by the manager.
     */
    RangeLockManager rangeLockManager;

    /* Overall memory consumed by (stale) OrderedStoredValues owned by the
       list */
    cb::RelaxedAtomic<size_t> staleSize;

    /* Metadata memory consumed by (stale) OrderedStoredValues owned by the
       list */
    cb::RelaxedAtomic<size_t> staleMetaDataSize;

private:
    /**
     * Purge the item at the iterator from the seqList
     * @param it item to purge
     * @param isStale is the item stale?
     * @param isReplaced has the item been replaced with a newer version?
     * @return the next item
     */
    OrderedLL::iterator purgeListElem(OrderedLL::iterator it,
                                      bool isStale,
                                      bool isReplaced);

    /**
     * We need to keep track of the highest seqno separately because there is a
     * small window wherein the last element of the list (though in correct
     * order) does not have a seqno.
     *
     * highseqno is monotonically increasing and is reset to a lower value
     * only in case of a rollback.
     *
     * Guarded by writeLock.
     */
    Monotonic<seqno_t> highSeqno;

    /**
     * The sequence number of the highest purged element.
     *
     * This should be non-decrementing, apart from a rollback where it will be
     * reset.
     */
    Monotonic<seqno_t> highestPurgedDeletedSeqno;

    /**
     * Seqno of the last visible item. Accounts only committed sync-writes (ie,
     * not Prepare and Abort) and normal mutations and deletions.
     * Sent into the SnapshotMarker for backfill-snapshots. This is the seqno
     * of the last item sent in backfill-snapshots to non-SyncRepl consumers.
     */
    Monotonic<uint64_t> maxVisibleSeqno{0};

    /**
     * Seqno of the highest completed prepare. Used at backfill to determine
     * if a prepare should be sent or not.
     */
    Monotonic<uint64_t> highCompletedSeqno{0};

    /**
     * Indicates the number of elements in the list that are stale (old,
     * duplicate values). Stale items are owned by the list and hence must
     * periodically clean them up.
     */
    cb::NonNegativeCounter<uint64_t> numStaleItems;

    /**
     * Indicates the number of logically deleted items in the list.
     * Since we are append-only, distributed cache supporting incremental
     * replication, we need to keep deleted items for while and periodically
     * purge them
     */
    cb::NonNegativeCounter<uint64_t> numDeletedItems;

    /* Used only to log debug messages */
    const Vbid vbid;

    /* Ep engine stats handle to track stats */
    EPStats& st;

    /* Point at which the tombstone purging was paused */
    OrderedLL::iterator pausedPurgePoint;

    friend std::ostream& operator<<(std::ostream& os,
                                    const BasicLinkedList& ll);

    class RangeIteratorLL : public SequenceList::RangeIteratorImpl {
    public:
        /**
         * Method to create instances of RangeIteratorLL. We only
         * allow one RangeIteratorLL object to exist at any one time,
         * hence creation can fail and that's why object creation is via a
         * public method and not constructor.
         *
         * @param ll ref to the linkedlist on which the iterator is created
         * @param isBackfill indicates if the iterator is for backfill (for
         *                   debug)
         *
         * @return Non-null pointer on success, or null if a RangeIteratorLL
         *         already exists.
         */
        static std::unique_ptr<RangeIteratorLL> create(BasicLinkedList& ll,
                                                       bool isBackfill);

        ~RangeIteratorLL() override;

        OrderedStoredValue& operator*() const override;

        /* Duplicate items are not returned by the iterator. That is, if there
           multiple copies of an item in the iterator range, then only the
           latest is returned */
        RangeIteratorLL& operator++() override;

        seqno_t curr() const override {
            return itrRange.getBegin();
        }

        seqno_t end() const override {
            return itrRange.getEnd();
        }

        seqno_t back() const override {
            return itrRange.getEnd() - 1;
        }

        uint64_t count() const override {
            return numRemaining;
        }

        uint64_t getMaxVisibleSeqno() const override {
            return maxVisibleSeqno;
        }

        uint64_t getHighCompletedSeqno() const override {
            return highCompletedSeqno;
        }

    private:
        /**
         * We have a private constructor because we want to create the iterator
         * optionally, that is, only when it is possible to get a read lock
         *
         * @param ll LinkedList object
         * @param listWriteLg Write lock of the LinkedList
         * @param isBackfill Is the RangeItr for backfilling?
         */
        RangeIteratorLL(BasicLinkedList& ll,
                        std::lock_guard<std::mutex>& listWriteLg,
                        bool isBackfill);

        /**
         * Indicates if the client should try creating the iterator at a later
         * point.
         *
         * @return true: iterator should be created later again
         *         false: iterator created successfully
         */
        bool tryLater() const {
            /* could not lock and the list has items */
            return (!rangeGuard && (list.getHighSeqno() > 0));
        }

        /**
         * Helps to increment the iterator. Moves the iterator to the next
         * element in the list
         */
        void incrOperatorHelper();

        /**
         * Indicates if there is a newer version of the curr item in the
         * iterator range
         *
         * @return true if there is a newer version of item; else false
         */
        bool itrRangeContainsAnUpdatedVersion();

        /* Ref to BasicLinkedList object which is iterated by this iterator.
           By setting the member variables of the list obj appropriately we
           ensure that iterator is not invalidated */
        BasicLinkedList& list;

        /* The current list element pointed by the iterator */
        OrderedLL::iterator currIt;

        /* guard holding the range lock over the linked list to stop items
         * in the needed range being updated */
        RangeGuard rangeGuard;

        /* Current range of the iterator */
        SeqRange itrRange;

        /* Number of items that can be iterated over by this (forward only)
           iterator at that instance */
        uint64_t numRemaining;

        const uint64_t maxVisibleSeqno;

        const uint64_t highCompletedSeqno;

        /* Indicates if the range iterator is for DCP backfill
           (for debug) */
        bool isBackfill;
    };

    friend class RangeIteratorLL;
};

/// Outputs a textual description of the BasicLinkedList
std::ostream& operator <<(std::ostream& os, const BasicLinkedList& ll);
