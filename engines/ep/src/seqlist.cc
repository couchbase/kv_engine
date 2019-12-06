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

#include <mutex>
#include "linked_list.h"

SequenceList::RangeIterator::RangeIterator(
        std::unique_ptr<RangeIteratorImpl> rangeIterImpl)
    : rangeIterImpl(std::move(rangeIterImpl)) {
}

OrderedStoredValue& SequenceList::RangeIterator::operator*() const {
    return *(*rangeIterImpl);
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

seqno_t SequenceList::RangeIterator::getEarlySnapShotEnd() const {
    return rangeIterImpl->getEarlySnapShotEnd();
}

uint64_t SequenceList::RangeIterator::getMaxVisibleSeqno() const {
    return rangeIterImpl->getMaxVisibleSeqno();
}
