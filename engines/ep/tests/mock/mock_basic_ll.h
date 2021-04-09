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

/*
 * Mock of the BasicLinkedList class.  Wraps the real BasicLinkedList class
 * and provides access to functions like getAllItemsForVerification().
 */
#pragma once

#include "linked_list.h"

#include <mutex>
#include <vector>

class MockBasicLinkedList : public BasicLinkedList {
public:
    explicit MockBasicLinkedList(EPStats& st) : BasicLinkedList(Vbid(0), st) {
    }

    OrderedLL& getSeqList() {
        return seqList;
    }

    std::vector<seqno_t> getAllSeqnoForVerification() const {
        std::vector<seqno_t> allSeqnos;
        std::lock_guard<std::mutex> lckGd(writeLock);

        for (auto& val : seqList) {
            allSeqnos.push_back(val.getBySeqno());
        }
        return allSeqnos;
    }

    /* Register fake range lock for testing */
    RangeGuard registerFakeSharedRangeLock(seqno_t start, seqno_t end) {
        return tryLockSeqnoRangeShared(start, end);
    }

    /* Register fake read range for testing */
    RangeGuard registerFakeRangeLock(seqno_t start, seqno_t end) {
        return tryLockSeqnoRange(start, end);
    }
};
