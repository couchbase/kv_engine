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

#include "config.h"
#include "item.h"
#include "memcached/engine_error.h"
#include "stored-value.h"

/* [EPHE TODO]: Check if uint64_t can be used instead */
using seqno_t = int64_t;

/**
 * SequenceList is the abstract base class for the classes that hold ordered
 * sequence of items in memory. To store/retreive items sequencially in memory,
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
 * of serializing the OrderedStoredValue. SequenceList only guarentees to hold
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
public:
    /**
     * Indicates whether the updateListElem is successful or the list is
     * allowing
     * only appends at the moment.
     */
    enum class UpdateStatus { Success, Append };

    virtual ~SequenceList() {
    }

    /**
     * Add a new item at the end of the list.
     *
     * @param seqLock A sequence lock the calling module is expected to hold.
     * @param v Ref to orderedStoredValue which will placed into the linked list
     *          Its intrusive list links will be updated.
     */
    virtual void appendToList(std::lock_guard<std::mutex>& seqLock,
                              OrderedStoredValue& v) = 0;

    /**
     * If possible, update an existing element the list and move it to end.
     * If there is a range read in the position of the element being updated
     * we do not allow the update and indicate the caller to do an append.
     *
     * @param seqLock A sequence lock the calling module is expected to hold.
     * @param v Ref to orderedStoredValue which will placed into the linked list
     *          Its intrusive list links will be updated.
     *
     * @return UpdateStatus::Success list element has been updated and moved to
     *                               end.
     *         UpdateStatus::Append list element is *not* updated. Caller must
     *                              handle the append.
     */
    virtual SequenceList::UpdateStatus updateListElem(
            std::lock_guard<std::mutex>& seqLock, OrderedStoredValue& v) = 0;

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
     * @return ENGINE_SUCCESS and items in the snapshot
     *         ENGINE_ENOMEM on no memory to copy items
     *         ENGINE_ERANGE on incorrect start and end
     */
    virtual std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> rangeRead(
            seqno_t start, seqno_t end) = 0;

    /**
     * Updates the highSeqno in the list. Since seqno is generated and managed
     * outside the list, the module managing it must update this after the seqno
     * is generated for the item already put in the list.
     * If the last OrderedStoredValue added to the list is soft deleted then
     * it also updates the deleted items count in the list
     *
     * @param v Ref to orderedStoredValue
     */
    virtual void updateHighSeqno(const OrderedStoredValue& v) = 0;

    /**
     * Mark an OrderedStoredValue stale and assumes its ownership.
     * Note: It is upto the sequential data structure implementation how it
     *       wants to own the OrderedStoredValue (as owned type vs non-owned
     *       type)
     *
     * @param ownedSv StoredValue whose ownership is passed to the sequential
     *                data strucuture.
     */
    virtual void markItemStale(StoredValue::UniquePtr ownedSv) = 0;

    /**
     * Returns the number of stale items in the list.
     *
     * @return count of stale items
     */
    virtual uint64_t getNumStaleItems() const = 0;

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
};
