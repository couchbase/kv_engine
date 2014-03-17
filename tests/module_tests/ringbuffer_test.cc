/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "config.h"

#include <iostream>
#include <vector>

#include "ringbuffer.h"

static void testEmpty() {
    RingBuffer<int> rb(10);
    cb_assert(rb.size() == 0);
    std::vector<int> v(rb.contents());
    cb_assert(v.empty());
}

static void testPartial() {
    RingBuffer<int> rb(10);
    rb.add(1);
    rb.add(2);
    rb.add(3);
    cb_assert(rb.size() == 3);
    std::vector<int> v(rb.contents());
    cb_assert(v.size() == 3);
    std::vector<int> expected;
    expected.push_back(1);
    expected.push_back(2);
    expected.push_back(3);

    cb_assert(v == expected);
}

static void testFull() {
    RingBuffer<int> rb(3);
    rb.add(1);
    rb.add(2);
    rb.add(3);
    cb_assert(rb.size() == 3);
    std::vector<int> v(rb.contents());
    cb_assert(v.size() == 3);
    std::vector<int> expected;
    expected.push_back(1);
    expected.push_back(2);
    expected.push_back(3);

    cb_assert(v == expected);
}

static void testWrapped() {
    RingBuffer<int> rb(2);
    rb.add(1);
    rb.add(2);
    rb.add(3);
    cb_assert(rb.size() == 2);
    std::vector<int> v(rb.contents());
    cb_assert(v.size() == 2);
    std::vector<int> expected;
    expected.push_back(2);
    expected.push_back(3);
    cb_assert(v == expected);
}

int main() {

    testEmpty();
    testPartial();
    testFull();
    testWrapped();

    return 0;
}
