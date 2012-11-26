#include "config.h"

#include <cassert>
#include <iostream>
#include <vector>

#include "ringbuffer.h"

static void testEmpty() {
    RingBuffer<int> rb(10);
    assert(rb.size() == 0);
    std::vector<int> v(rb.contents());
    assert(v.empty());
}

static void testPartial() {
    RingBuffer<int> rb(10);
    rb.add(1);
    rb.add(2);
    rb.add(3);
    assert(rb.size() == 3);
    std::vector<int> v(rb.contents());
    assert(v.size() == 3);
    std::vector<int> expected;
    expected.push_back(1);
    expected.push_back(2);
    expected.push_back(3);

    assert(v == expected);
}

static void testFull() {
    RingBuffer<int> rb(3);
    rb.add(1);
    rb.add(2);
    rb.add(3);
    assert(rb.size() == 3);
    std::vector<int> v(rb.contents());
    assert(v.size() == 3);
    std::vector<int> expected;
    expected.push_back(1);
    expected.push_back(2);
    expected.push_back(3);

    assert(v == expected);
}

static void testWrapped() {
    RingBuffer<int> rb(2);
    rb.add(1);
    rb.add(2);
    rb.add(3);
    assert(rb.size() == 2);
    std::vector<int> v(rb.contents());
    assert(v.size() == 2);
    std::vector<int> expected;
    expected.push_back(2);
    expected.push_back(3);
    assert(v == expected);
}

int main() {

    testEmpty();
    testPartial();
    testFull();
    testWrapped();

    return 0;
}
