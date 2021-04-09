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

#include <mutex>
#include "linked_list.h"

SequenceList::RangeIterator::RangeIterator(
        std::unique_ptr<RangeIteratorImpl> rangeIterImpl)
    : rangeIterImpl(std::move(rangeIterImpl)) {
}

const OrderedStoredValue& SequenceList::RangeIterator::operator*() const {
    return *(*rangeIterImpl);
}

const OrderedStoredValue* SequenceList::RangeIterator::operator->() const {
    return &*(*rangeIterImpl);
}

SequenceList::RangeIterator& SequenceList::RangeIterator::operator++() {
    ++(*rangeIterImpl);
    return *this;
}

seqno_t SequenceList::RangeIterator::curr() const {
    return rangeIterImpl->curr();
}

seqno_t SequenceList::RangeIterator::end() const {
    return rangeIterImpl->end();
}

seqno_t SequenceList::RangeIterator::back() const {
    return rangeIterImpl->back();
}

uint64_t SequenceList::RangeIterator::count() const {
    return rangeIterImpl->count();
}

uint64_t SequenceList::RangeIterator::getMaxVisibleSeqno() const {
    return rangeIterImpl->getMaxVisibleSeqno();
}

uint64_t SequenceList::RangeIterator::getHighCompletedSeqno() const {
    return rangeIterImpl->getHighCompletedSeqno();
}
