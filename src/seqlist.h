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
#include "config.h"

/* Forward declarations */
class OrderedStoredValue;

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
    virtual ~SequenceList() {
    }

    /* Add a new item at the end of the list */
    virtual void appendToList(std::lock_guard<std::mutex>& seqLock,
                              OrderedStoredValue& v) = 0;
};
