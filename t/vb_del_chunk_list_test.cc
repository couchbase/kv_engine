/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "ep.hh"
#undef NDEBUG
#include <assert.h>


static void testAddChunkRange() {
    VBDeletionChunkRangeList chunk_range_list;

    // This fails as a range's start point shoud be less than equal to its end point
    chunk_range_list.add(std::make_pair(10, 1));
    assert(chunk_range_list.size() == 0);

    chunk_range_list.add(std::make_pair(1, 10));
    assert(chunk_range_list.size() == 1);

    // This will fail as the list of ranges should be in a sorted order without any
    // overlapping area.
    chunk_range_list.add(std::make_pair(5, 20));
    assert(chunk_range_list.size() == 1);

    chunk_range_list.add(std::make_pair(10, 30));
    assert(chunk_range_list.size() == 2);

    chunk_range_list.add(std::make_pair(30, 30));
    assert(chunk_range_list.size() == 3);

    chunk_range_list.add(std::make_pair(40, 60));
    assert(chunk_range_list.size() == 4);
}

static void testSplitChunkRange() {
    VBDeletionChunkRangeList chunk_range_list;
    // Create the range list of (1, 10), (11, 20), (21, 30)
    chunk_range_list.add(std::make_pair(1, 10));
    chunk_range_list.add(std::make_pair(11, 20));
    chunk_range_list.add(std::make_pair(21, 30));
    assert(chunk_range_list.size() == 3);

    chunk_range_iterator it = chunk_range_list.begin();
    // Split the first chunk range by range size 11, which doesn't cause any splits
    // because the first chunk range size 9 is less than the split range size 11.
    chunk_range_list.splitChunkRange(it, 11);
    assert(chunk_range_list.size() == 3);
    assert(it->first == 1 && it->second == 10);

    // Split the first chunk range by range size 9, which doesn't cause any splits
    // because the first chunk range size 9 is equal to the split range size 9
    chunk_range_list.splitChunkRange(it, 9);
    assert(chunk_range_list.size() == 3);
    assert(it->first == 1 && it->second == 10);

    // Split the first chunk range by 3, which results in spliting it into two ranges.
    // The range list becomes (1, 4), (5, 10), (11, 20), (21, 30)
    chunk_range_list.splitChunkRange(it, 3);
    assert(chunk_range_list.size() == 4);
    assert(it->first == 1 && it->second == 4);
    ++it;
    assert(it->first == 5 && it->second == 10);
}

static void testMergeChunkRanges() {
    VBDeletionChunkRangeList chunk_range_list;
    // Create the range list of (10, 20), (30, 40), (50, 70), (80, 100), (120, 150)
    chunk_range_list.add(std::make_pair(10, 20));
    chunk_range_list.add(std::make_pair(30, 40));
    chunk_range_list.add(std::make_pair(50, 70));
    chunk_range_list.add(std::make_pair(80, 100));
    chunk_range_list.add(std::make_pair(120, 150));
    assert(chunk_range_list.size() == 5);

    chunk_range_iterator it = chunk_range_list.begin();
    // Merge the current chunk range with its subsequent ranges by range size 7, which doesn't
    // result in any merges because the current range's size 10 is greater than 7.
    chunk_range_list.mergeChunkRanges(it, 7);
    assert(chunk_range_list.size() == 5);
    assert(it->first == 10 && it->second == 20);

    // Merge the current chunk range with its subsequent ranges by range size 10, which doesn't
    // result in any merges as the current range's size 10 is equal to the merge size 10.
    chunk_range_list.mergeChunkRanges(it, 10);
    assert(chunk_range_list.size() == 5);
    assert(it->first == 10 && it->second == 20);

    // Merge the current range with its subsequent ranges by range size 25
    // The range list is now (10, 35), (36, 40), (50, 70), (80, 100), (120, 150)
    chunk_range_list.mergeChunkRanges(it, 25);
    assert(chunk_range_list.size() == 5);
    assert(it->first == 10 && it->second == 35);

    // Move the iterator to point to the next chunk range (36, 40)
    ++it;
    assert(it->first == 36 && it->second == 40);

    // Merge the current range with its subsequent ranges by range size 40, which results in
    // the deletion of the chunk range (50, 70). The range list will be updated as
    // (10, 35), (36, 70), (80, 100), (120, 150)
    chunk_range_list.mergeChunkRanges(it, 40);
    assert(chunk_range_list.size() == 4);
    // The current range's end point is set to 70 as 76 does not belong to any ranges.
    assert(it->first == 36 && it->second == 70);

    // Move the iterator to point to the next chunk range (80, 100)
    ++it;
    assert(it->first == 80 && it->second == 100);

    // Merge the current range with its subsequent ranges by range size 100, which results in
    // reaching the end of the range list and causes the deletion of the range (120, 150).
    // The range list becomes (10, 35), (36, 76), (80, 150)
    chunk_range_list.mergeChunkRanges(it, 100);
    assert(chunk_range_list.size() == 3);
    assert(it->first == 80 && it->second == 150);
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    testAddChunkRange();
    testSplitChunkRange();
    testMergeChunkRanges();

    return 0;
}
