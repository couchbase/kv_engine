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

#include "linked_list.h"

#include "stats.h"

#include <mutex>

BasicLinkedList::BasicLinkedList(uint16_t vbucketId, EPStats& st)
    : SequenceList(),
      readRange(0, 0),
      staleSize(0),
      staleMetaDataSize(0),
      highSeqno(0),
      highestDedupedSeqno(0),
      highestPurgedDeletedSeqno(0),
      numStaleItems(0),
      numDeletedItems(0),
      vbid(vbucketId),
      st(st) {
}

BasicLinkedList::~BasicLinkedList() {
    /* Delete stale items here, other items are deleted by the hash
       table */
    std::lock_guard<std::mutex> writeGuard(getListWriteLock());
    seqList.remove_and_dispose_if(
            [&writeGuard](const OrderedStoredValue& v) {
                return v.isStale(writeGuard);
            },
            [this](OrderedStoredValue* v) {
                this->st.currentSize.fetch_sub(v->metaDataSize());
                delete v;
            });

    /* Erase all the list elements (does not destroy elements, just removes
       them from the list) */
    seqList.clear();
}

void BasicLinkedList::appendToList(std::lock_guard<std::mutex>& seqLock,
                                   std::lock_guard<std::mutex>& writeLock,
                                   OrderedStoredValue& v) {
    seqList.push_back(v);
}

SequenceList::UpdateStatus BasicLinkedList::updateListElem(
        std::lock_guard<std::mutex>& seqLock,
        std::lock_guard<std::mutex>& writeLock,
        OrderedStoredValue& v) {
    /* Lock that needed for consistent read of SeqRange 'readRange' */
    std::lock_guard<SpinLock> lh(rangeLock);

    if (readRange.fallsInRange(v.getBySeqno())) {
        /* Range read is in middle of a point-in-time snapshot, hence we cannot
           move the element to the end of the list. Return a temp failure */
        return UpdateStatus::Append;
    }

    /* Since there is no other reads or writes happenning in this range, we can
       move the item to the end of the list */
    auto it = seqList.iterator_to(v);
    seqList.erase(it);
    seqList.push_back(v);

    return UpdateStatus::Success;
}

std::tuple<ENGINE_ERROR_CODE, std::vector<UniqueItemPtr>, seqno_t>
BasicLinkedList::rangeRead(seqno_t start, seqno_t end) {
    if ((start > end) || (start <= 0)) {
        LOG(EXTENSION_LOG_WARNING,
            "BasicLinkedList::rangeRead(): "
            "(vb:%d) ERANGE: start %" PRIi64 " > end %" PRIi64,
            vbid,
            start,
            end);
        return std::make_tuple(ENGINE_ERANGE, std::vector<UniqueItemPtr>(), 0);
    }

    /* Allows only 1 rangeRead for now */
    std::lock_guard<std::mutex> lckGd(rangeReadLock);

    {
        std::lock_guard<std::mutex> listWriteLg(getListWriteLock());
        std::lock_guard<SpinLock> lh(rangeLock);
        if (start > highSeqno) {
            LOG(EXTENSION_LOG_WARNING,
                "BasicLinkedList::rangeRead(): "
                "(vb:%d) ERANGE: start %" PRIi64 " > highSeqno %" PRIi64,
                vbid,
                start,
                static_cast<seqno_t>(highSeqno));
            /* If the request is for an invalid range, return before iterating
               through the list */
            return std::make_tuple(
                    ENGINE_ERANGE, std::vector<UniqueItemPtr>(), 0);
        }

        /* Mark the initial read range */
        end = std::min(end, static_cast<seqno_t>(highSeqno));
        end = std::max(end, static_cast<seqno_t>(highestDedupedSeqno));
        readRange = SeqRange(1, end);
    }

    /* Read items in the range */
    std::vector<UniqueItemPtr> items;

    for (const auto& osv : seqList) {
        int64_t currSeqno(osv.getBySeqno());

        if (currSeqno > end || currSeqno < 0) {
            /* We have read all the items in the requested range, or the osv
             * does not yet have a valid seqno; either way we are done */
            break;
        }

        {
            std::lock_guard<SpinLock> lh(rangeLock);
            readRange.setBegin(currSeqno); /* [EPHE TODO]: should we
                                                     update the min every time ?
                                                   */
        }

        if (currSeqno < start) {
            /* skip this item */
            continue;
        }

        /* Check if this OSV has been made stale and has been superseded by a
         * newer version. If it has, and the replacement is /also/ in the range
         * we are reading, we should skip this item to avoid duplicates */
        StoredValue* replacement;
        {
            std::lock_guard<std::mutex> writeGuard(getListWriteLock());
            replacement = osv.getReplacementIfStale(writeGuard);
        }

        if (replacement &&
            replacement->toOrderedStoredValue()->getBySeqno() <= end) {
            continue;
        }

        try {
            items.push_back(UniqueItemPtr(osv.toItem(false, vbid)));
        } catch (const std::bad_alloc&) {
            /* [EPHE TODO]: Do we handle backfill in a better way ?
                            Perhaps do backfilling partially (that is
                            backfill ==> stream; backfill ==> stream ..so on )?
             */
            LOG(EXTENSION_LOG_WARNING,
                "BasicLinkedList::rangeRead(): "
                "(vb %d) ENOMEM while trying to copy "
                "item with seqno %" PRIi64 "before streaming it",
                vbid,
                currSeqno);
            return std::make_tuple(
                    ENGINE_ENOMEM, std::vector<UniqueItemPtr>(), 0);
        }
    }

    /* Done with range read, reset the range */
    {
        std::lock_guard<SpinLock> lh(rangeLock);
        readRange.reset();
    }

    /* Return all the range read items */
    return std::make_tuple(ENGINE_SUCCESS, std::move(items), end);
}

void BasicLinkedList::updateHighSeqno(std::lock_guard<std::mutex>& listWriteLg,
                                      const OrderedStoredValue& v) {
    if (v.getBySeqno() < 1) {
        throw std::invalid_argument("BasicLinkedList::updateHighSeqno(): vb " +
                                    std::to_string(vbid) +
                                    "; Cannot set the highSeqno to a value " +
                                    std::to_string(v.getBySeqno()) +
                                    " which is < 1");
    }
    highSeqno = v.getBySeqno();
}
void BasicLinkedList::updateHighestDedupedSeqno(
        std::lock_guard<std::mutex>& listWriteLg, const OrderedStoredValue& v) {
    if (v.getBySeqno() < 1) {
        throw std::invalid_argument(
                "BasicLinkedList::updateHighestDedupedSeqno(): vb " +
                std::to_string(vbid) +
                "; Cannot set the highestDedupedSeqno to "
                "a value " +
                std::to_string(v.getBySeqno()) + " which is < 1");
    }
    highestDedupedSeqno = v.getBySeqno();
}

void BasicLinkedList::markItemStale(std::lock_guard<std::mutex>& listWriteLg,
                                    StoredValue::UniquePtr ownedSv,
                                    StoredValue* newSv) {
    /* Release the StoredValue as BasicLinkedList does not want it to be of
       owned type */
    StoredValue* v = ownedSv.release();

    /* Update the stats tracking the memory owned by the list */
    staleSize.fetch_add(v->size());
    staleMetaDataSize.fetch_add(v->metaDataSize());
    st.currentSize.fetch_add(v->metaDataSize());

    ++numStaleItems;
    v->toOrderedStoredValue()->markStale(listWriteLg, newSv);
}

size_t BasicLinkedList::purgeTombstones() {
    // Purge items marked as stale from the seqList.
    //
    // Strategy - we try to ensure that this function does not block
    // frontend-writes (adding new OrderedStoredValues (OSVs) to the seqList).
    // To achieve this (safely),
    // we (try to) acquire the rangeReadLock and setup a 'read' range for the
    // whole of the seqList. This prevents any other readers from iterating
    // the list (and accessing stale items) while we purge on it; but permits
    // front-end operations to continue as they:
    //   a) Only read/modify non-stale items (we only change stale items) and
    //   b) Do not change the list membership of anything within the read-range.
    // However, we do need to be careful about what members of OSVs we access
    // here - the only OSVs we can safely access are ones marked stale as they
    // are no longer in the HashTable (and hence subject to HashTable locks).
    // To check if an item is stale we need to acquire the writeLock
    // (OSV::stale is guarded by it) for each list item. While this isn't
    // ideal (that's the same lock needed by front-end operations), we can
    // release the lock between each element so front-end operations can
    // have the opportunity to acquire it.
    //
    // Attempt to acquire the readRangeLock, to block anyone else concurrently
    // reading from the list while we remove elements from it.
    std::unique_lock<std::mutex> rrGuard(rangeReadLock, std::try_to_lock);
    if (!rrGuard) {
        // If we cannot acquire the lock then another thread is
        // running a range read. Given these are typically long-running,
        // return without blocking.
        return 0;
    }

    // Determine the start and end iterators.
    OrderedLL::iterator startIt;
    OrderedLL::iterator endIt;
    {
        std::lock_guard<std::mutex> writeGuard(getListWriteLock());
        if (seqList.empty()) {
            // Nothing in sequence list - nothing to purge.
            return 0;
        }
        // Determine the {start} and {end} iterator (inclusive). Note
        // that (at most) one item is added to the seqList before its
        // sequence number is set (as the seqno comes from Ckpt
        // manager); if that is the case (such an item is guaranteed
        // to not be stale), then move end to the previous item
        // (i.e. we don't consider this "in-flight" item), as long as
        // there is at least two elements.
        startIt = seqList.begin();
        endIt = std::prev(seqList.end());
        // Need rangeLock for highSeqno & readRange
        std::lock_guard<SpinLock> rangeGuard(rangeLock);
        if ((startIt != endIt) && (!endIt->isStale(writeGuard))) {
            endIt = std::prev(endIt);
        }
        readRange = SeqRange(startIt->getBySeqno(), endIt->getBySeqno());
    }

    // Iterate across all but the last item in the seqList, looking
    // for stale items.
    // Note the for() loop terminates one element before endIt - we
    // actually want an inclusive iteration but as we are comparing
    // essentially random addresses (and we don't want to 'touch' the
    // element after endIt), we loop to one before endIt, then handle
    // endIt explicilty at the end.
    // Note(2): Iterator is manually incremented outside the for() loop as it
    // is invalidated when we erase items.
    size_t purgedCount = 0;
    bool stale;
    for (auto it = startIt; it != endIt;) {
        {
            std::lock_guard<std::mutex> writeGuard(getListWriteLock());
            stale = it->isStale(writeGuard);
        }
        // Only stale items are purged.
        if (!stale) {
            ++it;
            continue;
        }

        // Checks pass, remove from list and delete.
        it = purgeListElem(it);
        ++purgedCount;
    }
    // Handle the last element.
    {
        std::lock_guard<std::mutex> writeGuard(getListWriteLock());
        stale = endIt->isStale(writeGuard);
    }
    if (stale) {
        purgeListElem(endIt);
        ++purgedCount;
    }

    // Complete; reset the readRange.
    {
        std::lock_guard<SpinLock> lh(rangeLock);
        readRange.reset();
    }
    return purgedCount;
}

void BasicLinkedList::updateNumDeletedItems(bool oldDeleted, bool newDeleted) {
    if (oldDeleted && !newDeleted) {
        --numDeletedItems;
    } else if (!oldDeleted && newDeleted) {
        ++numDeletedItems;
    }
}

uint64_t BasicLinkedList::getNumStaleItems() const {
    return numStaleItems;
}

size_t BasicLinkedList::getStaleValueBytes() const {
    return staleSize;
}

size_t BasicLinkedList::getStaleMetadataBytes() const {
    return staleMetaDataSize;
}

uint64_t BasicLinkedList::getNumDeletedItems() const {
    std::lock_guard<std::mutex> lckGd(getListWriteLock());
    return numDeletedItems;
}

uint64_t BasicLinkedList::getNumItems() const {
    std::lock_guard<std::mutex> lckGd(getListWriteLock());
    return seqList.size();
}

uint64_t BasicLinkedList::getHighSeqno() const {
    std::lock_guard<std::mutex> lckGd(getListWriteLock());
    return highSeqno;
}

uint64_t BasicLinkedList::getHighestDedupedSeqno() const {
    std::lock_guard<std::mutex> lckGd(getListWriteLock());
    return highestDedupedSeqno;
}

seqno_t BasicLinkedList::getHighestPurgedDeletedSeqno() const {
    return highestPurgedDeletedSeqno;
}

uint64_t BasicLinkedList::getRangeReadBegin() const {
    std::lock_guard<SpinLock> lh(rangeLock);
    return readRange.getBegin();
}

uint64_t BasicLinkedList::getRangeReadEnd() const {
    std::lock_guard<SpinLock> lh(rangeLock);
    return readRange.getEnd();
}
std::mutex& BasicLinkedList::getListWriteLock() const {
    return writeLock;
}

boost::optional<SequenceList::RangeIterator>
BasicLinkedList::makeRangeIterator() {
    auto pRangeItr = RangeIteratorLL::create(*this);
    return pRangeItr ? RangeIterator(std::move(pRangeItr))
                     : boost::optional<SequenceList::RangeIterator>{};
}

void BasicLinkedList::dump() const {
    std::cerr << *this << std::endl;
}

std::ostream& operator <<(std::ostream& os, const BasicLinkedList& ll) {
    os << "BasicLinkedList[" << &ll << "] with numItems:" << ll.seqList.size()
       << " deletedItems:" << ll.numDeletedItems
       << " staleItems:" << ll.getNumStaleItems()
       << " highPurgeSeqno:" << ll.getHighestPurgedDeletedSeqno()
       << " elements:[" << std::endl;
    size_t count = 0;
    for (const auto& val : ll.seqList) {
        os << "    " << val << std::endl;
        ++count;
    }
    os << "] (count:" << count << ")";
    return os;
}

OrderedLL::iterator BasicLinkedList::purgeListElem(OrderedLL::iterator it) {
    StoredValue::UniquePtr purged(&*it);
    {
        std::lock_guard<std::mutex> lckGd(getListWriteLock());
        it = seqList.erase(it);
    }

    /* Update the stats tracking the memory owned by the list */
    staleSize.fetch_sub(purged->size());
    staleMetaDataSize.fetch_sub(purged->metaDataSize());
    st.currentSize.fetch_sub(purged->metaDataSize());

    // Similary for the item counts:
    --numStaleItems;
    if (purged->isDeleted()) {
        --numDeletedItems;
    }

    if (purged->isDeleted()) {
        highestPurgedDeletedSeqno = std::max(seqno_t(highestPurgedDeletedSeqno),
                                             purged->getBySeqno());
    }
    return it;
}

std::unique_ptr<BasicLinkedList::RangeIteratorLL>
BasicLinkedList::RangeIteratorLL::create(BasicLinkedList& ll) {
    /* Note: cannot use std::make_unique because the constructor of
       RangeIteratorLL is private */
    std::unique_ptr<BasicLinkedList::RangeIteratorLL> pRangeItr(
            new BasicLinkedList::RangeIteratorLL(ll));
    return pRangeItr->tryLater() ? nullptr : std::move(pRangeItr);
}

BasicLinkedList::RangeIteratorLL::RangeIteratorLL(BasicLinkedList& ll)
    : list(ll),
      /* Try to get range read lock, do not block */
      readLockHolder(list.rangeReadLock, std::try_to_lock),
      itrRange(0, 0),
      numRemaining(0),
      earlySnapShotEndSeqno(0) {
    if (!readLockHolder) {
        /* no blocking */
        return;
    }

    std::lock_guard<std::mutex> listWriteLg(list.getListWriteLock());
    std::lock_guard<SpinLock> lh(list.rangeLock);
    if (list.highSeqno < 1) {
        /* No need of holding a lock for the snapshot as there are no items;
           Also iterator range is at default (0, 0) */
        readLockHolder.unlock();
        return;
    }

    /* Iterator to the beginning of linked list */
    currIt = list.seqList.begin();

    /* Number of items that can be iterated over */
    numRemaining = list.seqList.size();

    /* The minimum seqno in the iterator that must be read to get a consistent
       read snapshot */
    earlySnapShotEndSeqno = list.highestDedupedSeqno;

    /* Mark the snapshot range on linked list. The range that can be read by the
       iterator is inclusive of the start and the end. */
    list.readRange =
            SeqRange(currIt->getBySeqno(), list.seqList.back().getBySeqno());

    /* Keep the range in the iterator obj. We store the range end seqno as one
       higher than the end seqno that can be read by this iterator.
       This is because, we must identify the end point of the iterator, and
       we the read is inclusive of the end points of list.readRange.

       Further, since use the class 'SeqRange' for 'itrRange' we cannot use
       curr() == end() + 1 to identify the end point because 'SeqRange' does
       not internally allow curr > end */
    itrRange = SeqRange(currIt->getBySeqno(),
                        list.seqList.back().getBySeqno() + 1);
}

BasicLinkedList::RangeIteratorLL::~RangeIteratorLL() {
    std::lock_guard<SpinLock> lh(list.rangeLock);
    if (readLockHolder.owns_lock()) {
        /* we must reset the list readRange only if the list iterator still owns
           the read lock on the list */
        list.readRange.reset();
    }
    /* As readLockHolder goes out of scope here, it will automatically release
       the snapshot read lock on the linked list */
}

OrderedStoredValue& BasicLinkedList::RangeIteratorLL::operator*() const {
    if (curr() >= end()) {
        /* We can't read beyond the range end */
        throw std::out_of_range(
                "BasicLinkedList::RangeIteratorLL::operator*()"
                ": Trying to read beyond range end seqno " +
                std::to_string(end()));
    }
    return *currIt;
}

BasicLinkedList::RangeIteratorLL& BasicLinkedList::RangeIteratorLL::
operator++() {
    if (curr() >= end()) {
        throw std::out_of_range(
                "BasicLinkedList::RangeIteratorLL::operator++()"
                ": Trying to move the iterator beyond range end"
                " seqno " +
                std::to_string(end()));
    }

    --numRemaining;

    /* Check if the iterator is pointing to the last element. Increment beyond
       the last element indicates the end of the iteration */
    if (curr() == itrRange.getEnd() - 1) {
        std::lock_guard<SpinLock> lh(list.rangeLock);
        /* We reset the range and release the readRange lock here so that any
           iterator client that does not delete the iterator obj will not end up
           holding the list readRange lock forever */
        list.readRange.reset();
        readLockHolder.unlock();

        /* Update the begin to end() so the client can see that the iteration
           has ended */
        itrRange.setBegin(end());
        return *this;
    }

    ++currIt;
    {
        /* As the iterator moves we reduce the snapshot range being read on the
           linked list. This helps reduce the stale items in the list during
           heavy update load from the front end */
        std::lock_guard<SpinLock> lh(list.rangeLock);
        list.readRange.setBegin(currIt->getBySeqno());
    }

    /* Also update the current range stored in the iterator obj */
    itrRange.setBegin(currIt->getBySeqno());

    return *this;
}
