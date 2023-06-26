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

#include "linked_list.h"
#include "bucket_logger.h"
#include "item.h"
#include "stats.h"

#include <memcached/vbucket.h>
#include <mutex>

BasicLinkedList::BasicLinkedList(Vbid vbucketId, EPStats& st)
    : SequenceList(),
      staleSize(0),
      staleMetaDataSize(0),
      highSeqno(0),
      highestPurgedDeletedSeqno(0),
      numStaleItems(0),
      numDeletedItems(0),
      vbid(vbucketId),
      st(st),
      pausedPurgePoint(seqList.end()) {
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
                this->st.coreLocal.get()->currentSize -= v->metaDataSize();
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
    auto range = rangeLockManager.getLockedRange();

    if (range.contains(v.getBySeqno())) {
        /* OSV is in middle of a point-in-time snapshot, hence we cannot
           move the element to the end of the list. Return a temp failure */
        return UpdateStatus::Append;
    }

    /* Since there is no other reads or writes happening in this range, we can
       move the item to the end of the list */
    auto it = seqList.iterator_to(v);
    /* If the list is being updated at 'pausedPurgePoint', then we must save
       the new 'pausedPurgePoint' */
    if (pausedPurgePoint == it) {
        pausedPurgePoint = seqList.erase(it);
    } else {
        seqList.erase(it);
    }
    seqList.push_back(v);

    return UpdateStatus::Success;
}

void BasicLinkedList::updateHighSeqno(std::lock_guard<std::mutex>& listWriteLg,
                                      const OrderedStoredValue& v) {
    if (v.getBySeqno() < 1) {
        throw std::invalid_argument(
                "BasicLinkedList::updateHighSeqno(): " + vbid.to_string() +
                "; Cannot set the highSeqno to a value " +
                std::to_string(v.getBySeqno()) + " which is < 1");
    }
    highSeqno = v.getBySeqno();
}

void BasicLinkedList::maybeUpdateMaxVisibleSeqno(
        std::lock_guard<std::mutex>& seqLock,
        std::lock_guard<std::mutex>& writeLock,
        const OrderedStoredValue& newSV) {
    switch (newSV.getCommitted()) {
    case CommittedState::CommittedViaMutation:
    case CommittedState::CommittedViaPrepare:
    case CommittedState::PrepareCommitted:
        maxVisibleSeqno = static_cast<uint64_t>(newSV.getBySeqno());
        return;
    case CommittedState::Pending:
    case CommittedState::PreparedMaybeVisible:
    case CommittedState::PrepareAborted:
        return;
    }
}

void BasicLinkedList::updateHighCompletedSeqno(
        std::lock_guard<std::mutex>& seqLock,
        std::lock_guard<std::mutex>& writeLock,
        int64_t hcs) {
    highCompletedSeqno = hcs;
}

void BasicLinkedList::markItemStale(std::lock_guard<std::mutex>& listWriteLg,
                                    StoredValue::UniquePtr ownedSv,
                                    StoredValue* newSv) {
    /* Release the StoredValue as BasicLinkedList does not want it to be of
       owned type */
    StoredValue* v = ownedSv.release().get();

    /* Update the stats tracking the memory owned by the list */
    staleSize += v->size();
    staleMetaDataSize += v->metaDataSize();
    st.coreLocal.get()->currentSize += v->metaDataSize();

    ++numStaleItems;
    v->toOrderedStoredValue()->markStale(listWriteLg, newSv);
}

size_t BasicLinkedList::purgeTombstones(
        seqno_t purgeUpToSeqno,
        Collections::IsDroppedEphemeralCb isDroppedKeyCb,
        std::function<bool()> shouldPause) {
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

    // Determine the start and end iterators.
    OrderedLL::iterator startIt;
    seqno_t startSeqno;
    RangeGuard range;
    {
        std::lock_guard<std::mutex> writeGuard(getListWriteLock());
        if (seqList.empty()) {
            // Nothing in sequence list - nothing to purge.
            return 0;
        }

        // Determine the start
        if (pausedPurgePoint != seqList.end()) {
            // resume
            startIt = pausedPurgePoint;
            pausedPurgePoint = seqList.end();
        } else {
            startIt = seqList.begin();
        }

        startSeqno = startIt->getBySeqno();

        if (startSeqno > purgeUpToSeqno) {
            /* Nothing to purge */
            return 0;
        }

        // Try to update the range lock. It will be released when the RangeGuard
        // goes out of scope.
        range = tryLockSeqnoRange(
                startSeqno, purgeUpToSeqno, RangeRequirement::Partial);
        if (!range) {
            // If we cannot set the read range, another thread is already
            // holding a read range. Given read range users are typically
            // long-running, return without blocking.
            return 0;
        }
    }

    if (range.getRange().getBegin() != startSeqno) {
        // a partial range lock was acquired, but the start had to
        // be moved forward. Can't efficiently advance the iterator
        // to the correct item and it would need to be under the write lock -
        // the items from startIt to the locked seqno are not protected
        // by the range lock, so we can't iterate through them safely outside
        // the write lock.
        EP_LOG_FMT(spdlog::level::level_enum::info,
                   "{} BasicLinkedList::purgeTombstones tried to lock seqno "
                   "range [{},{}] "
                   "but got [{},{}] instead. Start is different - cannot purge "
                   "right now",
                   vbid,
                   range.getRange().getBegin(),
                   range.getRange().getEnd(),
                   startSeqno,
                   purgeUpToSeqno);
        return 0;
    }

    // purge may have to stop at a lower seqno if the range lock
    // does not cover the full requested seqno range.
    seqno_t lastLockedSeqno = range.getRange().getEnd();

    // Sanity check, the rangeGuard would not be valid if this were not the case
    Expects(lastLockedSeqno >= startSeqno);

    // sanity check, the range lock doesn't extend to a seqno
    // later than what we requested.
    Expects(lastLockedSeqno <= purgeUpToSeqno);

    // Iterate across all but the last item in the seqList, looking
    // for stale items. May stop early if the range lock could only
    // cover a section of the seqList (see BasicLinkedList::tryLockSeqnoRange)
    size_t purgedCount = 0;
    bool stale;
    for (auto it = startIt; it != seqList.end();) {
        if (it->getBySeqno() > lastLockedSeqno) {
            if (lastLockedSeqno != purgeUpToSeqno) {
                //  have reached the end of the locked range, but the original
                //  requested end was higher i.e., the range lock was partial. Pause
                //  so next time purge is attempted it will resume from here (the
                //  range lock "blocking" part of the requested seqno range may have
                //  moved/gone)
                std::lock_guard<std::mutex> writeGuard(getListWriteLock());
                pausedPurgePoint = it;
            }
            // reached the end of the locked range, stop
            break;
        }

        if (it->getBySeqno() <= 0) {
            /* last item with no valid seqno yet */
            break;
        }

        // As we move past the items in the list, increment the begin of
        // the range lock to reduce the window of creating stale items during
        // updates
        if (it->getBySeqno() > startSeqno) {
            range.updateRangeStart(it->getBySeqno());
        }

        bool replaced = false;
        bool isReplacement = false;
        {
            std::lock_guard<std::mutex> writeGuard(getListWriteLock());
            stale = it->isStale(writeGuard);
            replaced = it->getReplacementIfStale(writeGuard);
            isReplacement = it->isStaleReplacement();
        }

        bool isDropped = false;
        if (!stale && isDroppedKeyCb) {
            isDropped = isDroppedKeyCb(it->getKey(), it->getBySeqno());
        }

        // Only stale or dropped items are purged, but they _cannot_ be
        // purged if they are pointed-to by an even older stale version of
        // the same value which has not yet been purged
        if (!isReplacement && (stale || isDropped)) {
            // Checks pass, remove from list and delete.
            it = purgeListElem(it, stale, replaced);
            ++purgedCount;
        } else {
            ++it;
        }

        if (shouldPause()) {
            std::lock_guard<std::mutex> writeGuard(getListWriteLock());
            // BasicLinkedList::updateListElem reads this value under the
            // write lock
            pausedPurgePoint = it;
            break;
        }
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

seqno_t BasicLinkedList::getHighestPurgedDeletedSeqno() const {
    return highestPurgedDeletedSeqno;
}

uint64_t BasicLinkedList::getMaxVisibleSeqno() const {
    std::lock_guard<std::mutex> lg(getListWriteLock());
    return maxVisibleSeqno;
}

uint64_t BasicLinkedList::getHighCompletedSeqno() const {
    std::lock_guard<std::mutex> lg(getListWriteLock());
    return highCompletedSeqno;
}

uint64_t BasicLinkedList::getHighCompletedSeqno(
        std::lock_guard<std::mutex>& writeLock) const {
    return highCompletedSeqno;
}

std::pair<uint64_t, uint64_t> BasicLinkedList::getRangeRead() const {
    return rangeLockManager.getLockedRange().getRange();
}

std::mutex& BasicLinkedList::getListWriteLock() const {
    return writeLock;
}

std::optional<SequenceList::RangeIterator> BasicLinkedList::makeRangeIterator(
        bool isBackfill) {
    auto pRangeItr = RangeIteratorLL::create(*this, isBackfill);
    return pRangeItr ? RangeIterator(std::move(pRangeItr))
                     : std::optional<SequenceList::RangeIterator>{};
}

RangeGuard BasicLinkedList::tryLockSeqnoRange(seqno_t start,
                                              seqno_t end,
                                              RangeRequirement req) {
    return rangeLockManager.tryLockRange(start, end, req);
}

RangeGuard BasicLinkedList::tryLockSeqnoRangeShared(seqno_t start,
                                                    seqno_t end) {
    return rangeLockManager.tryLockRangeShared(start, end);
}

void BasicLinkedList::dump(std::ostream& ostream) const {
    ostream << *this << std::endl;
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

OrderedLL::iterator BasicLinkedList::purgeListElem(OrderedLL::iterator it,
                                                   bool isStale,
                                                   bool isReplaced) {
    std::unique_ptr<OrderedStoredValue> purged;
    auto next = it;
    {
        std::lock_guard<std::mutex> lckGd(getListWriteLock());
        next = seqList.erase(it);
        purged.reset(&*it);
    }

    if (isStale) {
        /* Update the stats tracking the memory owned by the list */
        staleSize.fetch_sub(purged->size());
        staleMetaDataSize.fetch_sub(purged->metaDataSize());
        --numStaleItems;
    }

    st.coreLocal.get()->currentSize -= purged->metaDataSize();

    if (purged->isDeleted()) {
        --numDeletedItems;
    }

    /**
     * Only the purging of certain items contribute towards the moving of the
     * purge seqno. Those items are stale items that are deleted and have not
     * been replaced (i.e. those items that were marked stale by the
     * HTTombstonePurger).
     */
    if (isStale && !isReplaced && purged->isDeleted() &&
        purged->getBySeqno() > highestPurgedDeletedSeqno.load()) {
        highestPurgedDeletedSeqno = purged->getBySeqno();
    }
    return next;
}

std::unique_ptr<BasicLinkedList::RangeIteratorLL>
BasicLinkedList::RangeIteratorLL::create(BasicLinkedList& ll, bool isBackfill) {
    std::unique_ptr<BasicLinkedList::RangeIteratorLL> pRangeItr;
    {
        std::lock_guard<std::mutex> listWriteLg(ll.getListWriteLock());

        /* Note: cannot use std::make_unique because the constructor of
           RangeIteratorLL is private */
        pRangeItr = std::unique_ptr<BasicLinkedList::RangeIteratorLL>(
                new BasicLinkedList::RangeIteratorLL(
                        ll, listWriteLg, isBackfill));
    }

    return pRangeItr->tryLater() ? nullptr : std::move(pRangeItr);
}

BasicLinkedList::RangeIteratorLL::RangeIteratorLL(
        BasicLinkedList& ll,
        std::lock_guard<std::mutex>& listWriteLg,
        bool isBackfill)
    : list(ll),
      itrRange(0, 0),
      numRemaining(0),
      maxVisibleSeqno(list.maxVisibleSeqno),
      highCompletedSeqno(list.highCompletedSeqno),
      isBackfill(isBackfill) {
    if (list.highSeqno < 1) {
        /* No need of holding a lock for the snapshot as there are no items;
           Also iterator range is at default (0, 0) */
        return;
    }

    // Iterator to the first non-stale item from beginning of the SeqList
    for (currIt = list.seqList.begin(); currIt != list.seqList.end();
         ++currIt) {
        if (currIt->getReplacementIfStale(listWriteLg) == nullptr) {
            // currIt points to a non stale item or to a stale item with no
            // replacement.
            break;
        }
    }

    /* Number of items that can be iterated over */
    numRemaining = list.seqList.size();

    /* Mark the snapshot range on linked list. The range that can be read by the
       iterator is inclusive of the start and the end. */
    rangeGuard = ll.tryLockSeqnoRangeShared(currIt->getBySeqno(),
                                            list.seqList.back().getBySeqno());

    if (!rangeGuard) {
        // another rangeRead is in progress, return.
        return;
    }

    /* Keep the range in the iterator obj. We store the range end seqno as one
       higher than the end seqno that can be read by this iterator.
       This is because, we must identify the end point of the iterator, and
       the read is inclusive of the end points of the locked range.

       Further, since use the class 'SeqRange' for 'itrRange' we cannot use
       curr() == end() + 1 to identify the end point because 'SeqRange' does
       not internally allow curr > end */
    itrRange = SeqRange(currIt->getBySeqno(),
                        list.seqList.back().getBySeqno() + 1);

    auto severity = isBackfill ? spdlog::level::level_enum::info
                               : spdlog::level::level_enum::debug;

    EP_LOG_FMT(severity,
               "{} Created range iterator from {} to {}, maxVisibleSeqno:{}",
               list.vbid,
               itrRange.getBegin(),
               itrRange.getEnd(),
               maxVisibleSeqno);
}

BasicLinkedList::RangeIteratorLL::~RangeIteratorLL() {
    if (rangeGuard) {
        auto severity = isBackfill ? spdlog::level::level_enum::info
                                   : spdlog::level::level_enum::debug;
        EP_LOG_FMT(severity, "{} Releasing the range iterator", list.vbid);
    }
    /* As rangeGuard is destroyed, it will automatically release
       the range lock on the linked list */
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
    do {
        incrOperatorHelper();
        if (curr() == end()) {
            /* iterator has gone beyond the range, just return */
            return *this;
        }
    } while (itrRangeContainsAnUpdatedVersion());
    return *this;
}

void BasicLinkedList::RangeIteratorLL::incrOperatorHelper() {
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
        /* We reset the range lock here so that any iterator client that
         * does not delete the iterator obj will not end up holding the list
         * range lock forever */
        rangeGuard.reset();
        auto severity = isBackfill ? spdlog::level::level_enum::info
                                   : spdlog::level::level_enum::debug;
        EP_LOG_FMT(severity, "{} Releasing the range iterator", list.vbid);

        /* Update the begin to end() so the client can see that the iteration
           has ended */
        itrRange.setBegin(end());
        return;
    }

    ++currIt;

    /* As the iterator moves we reduce the snapshot range being read on the
       linked list. This helps reduce the stale items in the list during
       heavy update load from the front end */
    rangeGuard.updateRangeStart(currIt->getBySeqno());

    /* Also update the current range stored in the iterator obj */
    itrRange.setBegin(currIt->getBySeqno());
}

bool BasicLinkedList::RangeIteratorLL::itrRangeContainsAnUpdatedVersion() {
    /* Check if this OSV has been made stale and has been superseded by a
       newer version. If it has, and the replacement is /also/ in the range
       we are reading, we should skip this item to avoid duplicates */
    StoredValue* replacement;
    {
        /* Writer and tombstone purger hold the 'list.writeLock' when they
           change the pointer to the replacement OSV, and hence it would not be
           safe to read the uniquePtr without preventing concurrent changes to
           it */
        std::lock_guard<std::mutex> writeGuard(list.getListWriteLock());
        replacement = (*(*this)).getReplacementIfStale(writeGuard);
    }
    return (replacement != nullptr && replacement->getBySeqno() <= back());
}
