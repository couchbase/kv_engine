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
#include <mutex>

BasicLinkedList::BasicLinkedList(uint16_t vbucketId, EPStats& st)
    : SequenceList(),
      readRange(0, 0),
      staleSize(0),
      staleMetaDataSize(0),
      highSeqno(0),
      highestDedupedSeqno(0),
      numStaleItems(0),
      numDeletedItems(0),
      vbid(vbucketId),
      st(st) {
}

BasicLinkedList::~BasicLinkedList() {
    /* Delete stale items here, other items are deleted by the hash
       table */
    seqList.remove_and_dispose_if(
            [](const OrderedStoredValue& v) { return v.isStale(); },
            [this](OrderedStoredValue* v) {
                this->st.currentSize.fetch_sub(v->metaDataSize());
                delete v;
            });

    /* Erase all the list elements (does not destroy elements, just removes
       them from the list) */
    seqList.clear();
}

void BasicLinkedList::appendToList(std::lock_guard<std::mutex>& seqLock,
                                   OrderedStoredValue& v) {
    /* Allow only one write to the list at a time */
    std::lock_guard<std::mutex> lckGd(writeLock);

    seqList.push_back(v);
}

SequenceList::UpdateStatus BasicLinkedList::updateListElem(
        std::lock_guard<std::mutex>& seqLock, OrderedStoredValue& v) {
    /* Allow only one write to the list at a time */
    std::lock_guard<std::mutex> lckGd(writeLock);

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

std::pair<ENGINE_ERROR_CODE, std::vector<UniqueItemPtr>>
BasicLinkedList::rangeRead(seqno_t start, seqno_t end) {
    std::vector<UniqueItemPtr> empty;
    if ((start > end) || (start <= 0)) {
        LOG(EXTENSION_LOG_WARNING,
            "BasicLinkedList::rangeRead(): "
            "(vb:%d) ERANGE: start %" PRIi64 " > end %" PRIi64,
            vbid,
            start,
            end);
        return {ENGINE_ERANGE, std::move(empty)
                /* MSVC not happy with std::vector<UniqueItemPtr>() */};
    }

    /* Allows only 1 rangeRead for now */
    std::lock_guard<std::mutex> lckGd(rangeReadLock);

    {
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
            return {ENGINE_ERANGE, std::move(empty)};
        }

        /* Mark the initial read range */
        end = std::min(end, static_cast<seqno_t>(highSeqno));
        end = std::max(end, static_cast<seqno_t>(highestDedupedSeqno));
        readRange = SeqRange(1, end);
    }

    /* Read items in the range */
    std::vector<UniqueItemPtr> items;

    for (const auto& osv : seqList) {
        if (osv.getBySeqno() > end) {
            /* we are done */
            break;
        }

        {
            std::lock_guard<SpinLock> lh(rangeLock);
            readRange.setBegin(osv.getBySeqno()); /* [EPHE TODO]: should we
                                                     update the min every time ?
                                                   */
        }

        if (osv.getBySeqno() < start) {
            /* skip this item */
            continue;
        }

        try {
            items.push_back(UniqueItemPtr(osv.toItem(false, vbid)));
        } catch (const std::bad_alloc&) {
            LOG(EXTENSION_LOG_WARNING,
                "BasicLinkedList::rangeRead(): "
                "(vb %d) ENOMEM while trying to copy "
                "item with seqno %" PRIi64 "before streaming it",
                vbid,
                osv.getBySeqno());
            return {ENGINE_ENOMEM, std::move(empty)};
        }
    }

    /* Done with range read, reset the range */
    {
        std::lock_guard<SpinLock> lh(rangeLock);
        readRange.reset();
    }

    /* Return all the range read items */
    return {ENGINE_SUCCESS, std::move(items)};
}

void BasicLinkedList::updateHighSeqno(const OrderedStoredValue& v) {
    std::lock_guard<SpinLock> lh(rangeLock);
    highSeqno = v.getBySeqno();
    if (v.isDeleted()) {
        ++numDeletedItems;
    }
}

void BasicLinkedList::markItemStale(StoredValue::UniquePtr ownedSv) {
    /* Release the StoredValue as BasicLinkedList does not want it to be of
       owned type */
    StoredValue* v = ownedSv.release();

    /* Update the stats tracking the memory owned by the list */
    staleSize.fetch_add(v->size());
    staleMetaDataSize.fetch_add(v->metaDataSize());
    st.currentSize.fetch_add(v->metaDataSize());

    /* Safer to serialize with the deletion of stale values */
    std::lock_guard<std::mutex> lckGd(writeLock);

    ++numStaleItems;
    v->toOrderedStoredValue()->markStale();
}

uint64_t BasicLinkedList::getNumStaleItems() const {
    std::lock_guard<std::mutex> lckGd(writeLock);
    return numStaleItems;
}

uint64_t BasicLinkedList::getNumDeletedItems() const {
    std::lock_guard<std::mutex> lckGd(writeLock);
    return numDeletedItems;
}

uint64_t BasicLinkedList::getNumItems() const {
    std::lock_guard<std::mutex> lckGd(writeLock);
    return seqList.size();
}
