/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/**
 * This header file contains the abstract base class for the classes that hold
 * ordered sequence of items in memory. This is not generic, holds only ordered
 * seq of OrderedStoredValue's. (OrderedStoredValue is the in-memory
 * representation of an
 * item)
 */

#pragma once

#include <mutex>
#include <vector>

#include "collections/collections_types.h"
#include "collections/eraser_context.h"
#include "memcached/engine_error.h"
#include "stored-value.h"

#include <boost/optional/optional.hpp>

/* [EPHE TODO]: Check if uint64_t can be used instead */
using seqno_t = int64_t;

/**
 * SequenceList is the abstract base class for the classes that hold ordered
 * sequence of items in memory. To store/retrieve items sequentially in memory,
 * in our multi-threaded, monotonically increasing seq model, we need these
 * classes around basic data structures like Linkedlist or Skiplist.
 *
 * 'OrderedStoredValue' is the in-memory representation of an item that can be
 * stored sequentially and SequenceList derivatives store a sequence of
 * 'OrderedStoredValues'. Based on the implementation of SequenceList,
 * OrderedStoredValue might have to carry an intrusive component that
 * is necessary for the list.
 *
 * SequenceList expects the module that contains it, takes the responsibility
 * of serializing the OrderedStoredValue. SequenceList only guarantees to hold
 * the OrderedStoredValue in the order they are sent to it. For this it forces
 * the write calls to pass the seq lock held by them
 *
 * Note: These classes only handle the position of the 'OrderedStoredValue' in
 *       the ordered sequence desired. They do not update a OrderedStoredValue.
 *
 *       In the file 'item', 'OrderedStoredValue' are used interchangeably
 *
 * [EPHE TODO]: create a documentation (with diagrams) explaining write, update,
 *              rangeRead and Clean Up
 */
class SequenceList {
protected:
    /**
     * Abstract base class for implementation of range iterators in derived
     * classes of SequenceList.
     *
     * (a) The API is for unidirectional (forward only) iterator.
     * (b) Iterator cannot be invalidated while in use.
     * (c) Reading all the items from the iterator results in point-in-time
     *     snapshot.
     * (d) Only 1 iterator can be created for now.
     * (e) Currently iterator can be created only from start till end
     */
    class RangeIteratorImpl {
    public:
        virtual ~RangeIteratorImpl() {
        }

        /**
         * Returns the stored item at iterator's position
         */
        virtual OrderedStoredValue& operator*() const = 0;

        /**
         * Pre increment of the iterator position
         *
         * Note: We do not allow post increment for now as we support only
         *       one iterator at a time. (hence, we don't create a temp copy
         *       of the iterator obj)
         */
        virtual RangeIteratorImpl& operator++() = 0;

        /**
         * Curr iterator position, indicated by the seqno of the item at that
         * position
         */
        virtual seqno_t curr() const = 0;

        /**
         * End position of iterator, indicated by the seqno > highest_seqno
         * in the iterator range
         */
        virtual seqno_t end() const = 0;

        /**
         * Seqno of the last item in the iterator
         */
        virtual seqno_t back() const = 0;

        /**
         * Returns the number of items that can be iterated over by this
         * (forward only) iterator at that instance
         */
        virtual uint64_t count() const = 0;

        /**
         * Indicates the minimum seqno in the iterator that must be read to
         * get a consistent read snapshot
         */
        virtual seqno_t getEarlySnapShotEnd() const = 0;

        virtual uint64_t getMaxVisibleSeqno() const = 0;
    };

public:
    /**
     * Indicates whether the updateListElem is successful or the list is
     * allowing
     * only appends at the moment.
     */
    enum class UpdateStatus { Success, Append };

    /**
     * RangeIterator for a SequenceList objects.
     *
     * The iterator is unidirectional (forward only) and cannot be invalidated
     * while in use. That means reading the items in the iterator results in a
     * valid point-in-time snapshot. But this adds a caveat that while an
     * iterator instance is active, certain invariants are to be met to get the
     * snapshot, and this happens at the expense of increased memory usage.
     *
     * Implementation follows the pimpl idiom. This class serves as a wrapper
     * for the abstract base class that defines the APIs for range iteration on
     * the derived concrete classes of the SequenceList. It calls the range
     * iterators of the concrete classes polymorphically at runtime. However
     * clients get this statically typed "RangeIterator" object when they
     * request for an iterator for a SequenceList object.
     *
     * Usage:
     * 1. Create an iterator instance using SequenceList::makeRangeIterator()
     * 2. Iterate (read) through all the list elements.
     * 3. Delete the iterator.
     * Note: (a) Do not hold the iterator for long, as it will result in stale
     *           items in list and hence increased memory usage.
     *       (b) Make sure to delete the iterator after using it.
     *       (c) For now, we allow on one RangeIterator. Try to create more
     *           iterators will result in the create call(s) being blocked.
     */
    class RangeIterator {
    public:
        RangeIterator(std::unique_ptr<RangeIteratorImpl> rangeIterImpl);

        /* Needed for MSVC.
           MSVC does not do "return value optimization" if copy constructor is
           defined before move constructor
           (http://stackoverflow.com/questions/29459040)
         */
        RangeIterator(RangeIterator&& other)
            : rangeIterImpl(std::move(other.rangeIterImpl)) {
        }

        RangeIterator& operator=(RangeIterator&& other) {
            rangeIterImpl = std::move(other.rangeIterImpl);
            return *this;
        }

        RangeIterator(RangeIterator& other) = delete;

        /**
         * Returns the stored item at iterator's position
         */
        const OrderedStoredValue& operator*() const;

        const OrderedStoredValue* operator->() const;

        /**
         * Pre increment of the iterator position
         */
        RangeIterator& operator++();

        /**
         * Curr iterator position, indicated by the seqno of the item at that
         * position
         */
        seqno_t curr() const;

        /**
         * End position of iterator, indicated by the seqno > highest_seqno
         * in the iterator range
         */
        seqno_t end() const;

        /**
         * Seqno of the last item in the iterator
         */
        seqno_t back() const;

        /**
         * Returns the number of items that can be iterated over by this
         * (forward only) iterator at that instance
         */
        uint64_t count() const;

        /**
         * Indicates the minimum seqno in the iterator that must be read to
         * get a consistent read snapshot
         */
        seqno_t getEarlySnapShotEnd() const;

        uint64_t getMaxVisibleSeqno() const;

    private:
        /* Pointer to the abstract class of range iterator implementation */
        std::unique_ptr<RangeIteratorImpl> rangeIterImpl;
    };

    virtual ~SequenceList() {
    }

    /**
     * Add a new item at the end of the list.
     *
     * @param seqLock A sequence lock the calling module is expected to hold.
     * @param writeLock Write lock of the sequenceList from getListWriteLock()
     * @param v Ref to orderedStoredValue which will placed into the linked list
     *          Its intrusive list links will be updated.
     */
    virtual void appendToList(std::lock_guard<std::mutex>& seqLock,
                              std::lock_guard<std::mutex>& writeLock,
                              OrderedStoredValue& v) = 0;

    /**
     * If possible, update an existing element the list and move it to end.
     * If there is a range read in the position of the element being updated
     * we do not allow the update and indicate the caller to do an append.
     *
     * @param seqLock A sequence lock the calling module is expected to hold.
     * @param writeLock Write lock of the sequenceList from getListWriteLock()
     * @param v Ref to orderedStoredValue which will placed into the linked list
     *          Its intrusive list links will be updated.
     *
     * @return UpdateStatus::Success list element has been updated and moved to
     *                               end.
     *         UpdateStatus::Append list element is *not* updated. Caller must
     *                              handle the append.
     */
    virtual SequenceList::UpdateStatus updateListElem(
            std::lock_guard<std::mutex>& seqLock,
            std::lock_guard<std::mutex>& writeLock,
            OrderedStoredValue& v) = 0;

    /**
     * Provides point-in-time snapshots which can be used for incremental
     * replication.
     *
     * Copies the StoredValues as a vector of ref counterd items starting from
     * 'start + 1' seqno into 'items' as a snapshot.
     *
     * Since we use monotonically increasing point-in-time snapshots we cannot
     * guarantee that the snapshot ends at the requested end seqno. Due to
     * dedups we may have to send till a higher seqno in the snapshot.
     *
     * @param start requested start seqno
     * @param end requested end seqno
     *
     * @return ENGINE_SUCCESS, items in the snapshot and adjusted endSeqNo
     *         ENGINE_ENOMEM on no memory to copy items
     *         ENGINE_ERANGE on incorrect start and end
     */
    virtual std::tuple<ENGINE_ERROR_CODE, std::vector<UniqueItemPtr>, seqno_t>
    rangeRead(seqno_t start, seqno_t end) = 0;

    /**
     * Updates the highSeqno in the list. Since seqno is generated and managed
     * outside the list, the module managing it must update this after the seqno
     * is generated for the item already put in the list.
     *
     * @param listWriteLg Write lock of the sequenceList from getListWriteLock()
     * @param v Ref to orderedStoredValue
     */
    virtual void updateHighSeqno(std::lock_guard<std::mutex>& listWriteLg,
                                 const OrderedStoredValue& v) = 0;

    /**
     * Updates the highestDedupedSeqno in the list. Since seqno is generated and
     * managed outside the list, the module managing it must update this after
     * the seqno is generated for the item already put in the list.
     *
     * @param listWriteLg Write lock of the sequenceList from getListWriteLock()
     * @param v Ref to orderedStoredValue
     *
     */
    virtual void updateHighestDedupedSeqno(
            std::lock_guard<std::mutex>& listWriteLg,
            const OrderedStoredValue& v) = 0;

    /**
     * Updates the max-visible-seqno to the seqno of the new StoredValue, only
     * if the new item is committed.
     *
     * Note: In general we must hold the seqLock when we update seqnos in
     * the sequenceList, so this function requires it.
     * That is because different threads may interleave the execution of
     * CM::queueDirty() and updateSeqno() otherwise, which could lead to
     * breaking seqno monotonicity in seqList (and throwing).
     *
     * @todo: Change the other seqno-update functions to require the seqLock.
     *   We are currently using them correctly in our code (ie, we call them
     *   under seqLock), but the signatures should force that.
     *
     * @param seqLock The sequence lock the caller is expected to hold
     * @param writeLock Write lock of the sequenceList from getListWriteLock()
     * @param newSV
     */
    virtual void maybeUpdateMaxVisibleSeqno(
            std::lock_guard<std::mutex>& seqLock,
            std::lock_guard<std::mutex>& writeLock,
            const OrderedStoredValue& newSV) = 0;

    /**
     * Mark an OrderedStoredValue stale and assumes its ownership. Stores ptr to
     * the newer version in the OSV, or nullptr if there is no newer version
     * (i.e., expired Tombstone)
     * Note: It is upto the sequential data structure implementation how it
     *       wants to own the OrderedStoredValue (as owned type vs non-owned
     *       type)
     *
     * @param listWriteLg Write lock of the sequenceList from getListWriteLock()
     * @param ownedSv StoredValue whose ownership is passed to the sequential
     *                data structure.
     * @param replacement StoredValue which supersedes ownedSv, or nullptr.
     */
    virtual void markItemStale(std::lock_guard<std::mutex>& listWriteLg,
                               StoredValue::UniquePtr ownedSv,
                               StoredValue* replacement) = 0;

    /**
     * Remove from sequence list and delete all OSVs which are purgable.
     * OSVs which can be purged are items which are outside the ReadRange and
     * are Stale.
     *
     * @param purgeUpToSeqno Indicates the max seqno (inclusive) that could be
     *                       purged
     * @param isDroppedKey Callback function the purger will use to determine if
     *                     a key is belongs to a dropped collection.
     * @param shouldPause Callback function that indicates if tombstone purging
     *                    should pause. This is called for every element in the
     *                    sequence list when we iterate over the list during the
     *                    purge. The caller should decide if the purge should
     *                    continue or if it should be paused (in case it is
     *                    running for a long time). By default, we assume that
     *                    the tombstone purging need not be paused at all
     *
     * @return The number of items purged from the sequence list (and hence
     *         deleted).
     */
    virtual size_t purgeTombstones(
            seqno_t purgeUpToSeqno,

            Collections::IsDroppedEphemeralCb isDroppedKey,

            std::function<bool()> shouldPause = []() { return false; }) = 0;

    /**
     * Updates the number of deleted items in the sequence list whenever
     * an item is modified.
     *
     * @param oldDeleted Was the old item deleted?
     * @param newDeleted Was the new item replacing it deleted?
     */
    virtual void updateNumDeletedItems(bool oldDeleted, bool newDeleted) = 0;

    /**
     * Returns the number of stale items in the list.
     *
     * @return count of stale items
     */
    virtual uint64_t getNumStaleItems() const = 0;

    /**
     * Return the count of bytes of the values of stale items in the list.
     */
    virtual size_t getStaleValueBytes() const = 0;

    /**
     * Return the count of bytes of the metadata of stale items in the list.
     */
    virtual size_t getStaleMetadataBytes() const = 0;

    /**
     * Returns the number of deleted items in the list.
     *
     * @return count of deleted items
     */
    virtual uint64_t getNumDeletedItems() const = 0;

    /**
     * Returns the number of items in the list.
     *
     * @return count of items
     */
    virtual uint64_t getNumItems() const = 0;

    /**
     * Returns the highSeqno in the list.
     */
    virtual uint64_t getHighSeqno() const = 0;

    /**
     * Returns the highest de-duplicated sequence number in the list.
     */
    virtual uint64_t getHighestDedupedSeqno() const = 0;

    /**
     * Returns the highest purged Deleted sequence number in the list.
     */
    virtual seqno_t getHighestPurgedDeletedSeqno() const = 0;

    virtual uint64_t getMaxVisibleSeqno() const = 0;

    /**
     * Returns the current range lock begin and end sequence numbers,
     * inclusive of both ends.
     */
    virtual std::pair<uint64_t, uint64_t> getRangeRead() const = 0;

    /**
     * Returns the lock which must be held to make append/update to the seqList
     * + the updation of the corresponding highSeqno or the
     * highestDedupedSeqno atomic
     */
    virtual std::mutex& getListWriteLock() const = 0;

    /**
     * Creates a range iterator for the underlying SequenceList 'optionally'.
     * Under scenarios like where we want to limit the number of range iterators
     * the SequenceList, new range iterator will not be allowed
     *
     * @param isBackfill indicates if the iterator is for backfill (for debug)
     *
     * @return range iterator object when possible
     *         null when not possible
     */
    virtual boost::optional<SequenceList::RangeIterator> makeRangeIterator(
            bool isBackfill) = 0;

    /**
     * Debug - prints a representation of the list to stderr.
     */
    virtual void dump() const = 0;
};
