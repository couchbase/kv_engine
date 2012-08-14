/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "common.hh"
#undef NDEBUG
#include <assert.h>


int main(int argc, char **argv) {
    (void)argc; (void)argv;

    std::vector<uint64_t> elm_list;
    for (int i = 0; i < 10; ++i) {
        elm_list.push_back(i);
    }

    std::list<std::pair<uint64_t, uint64_t> > chunk_list;
    std::list<std::pair<uint64_t, uint64_t> >::iterator it;

    createChunkListFromArray(&elm_list, 0, chunk_list);
    assert(chunk_list.size() == 0);
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 1, chunk_list);
    assert(chunk_list.size() == 10);
    uint64_t i=0;
    for (it = chunk_list.begin(); it != chunk_list.end(); ++it) {
        assert(it->first == i && it->second == i);
        ++i;
    }
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 4, chunk_list);
    assert(chunk_list.size() == 3);
    it = chunk_list.begin();
    assert(it->first == 0 && it->second == 3);
    ++it;
    assert(it->first == 4 && it->second == 7);
    ++it;
    assert(it->first == 8 && it->second == 9);
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 7, chunk_list);
    assert(chunk_list.size() == 2);
    it = chunk_list.begin();
    assert(it->first == 0 && it->second == 6);
    ++it;
    assert(it->first == 7 && it->second == 9);
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 11, chunk_list);
    assert(chunk_list.size() == 1);
    it = chunk_list.begin();
    assert(it->first == 0 && it->second == 9);
    chunk_list.clear();

    return 0;
}
