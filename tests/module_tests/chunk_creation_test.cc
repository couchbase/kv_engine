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

#include "common.h"
#undef NDEBUG

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    std::vector<uint64_t> elm_list;
    for (int i = 0; i < 10; ++i) {
        elm_list.push_back(i);
    }

    std::list<std::pair<uint64_t, uint64_t> > chunk_list;
    std::list<std::pair<uint64_t, uint64_t> >::iterator it;

    createChunkListFromArray(&elm_list, 0, chunk_list);
    cb_assert(chunk_list.empty());
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 1, chunk_list);
    cb_assert(chunk_list.size() == 10);
    uint64_t i=0;
    for (it = chunk_list.begin(); it != chunk_list.end(); ++it) {
        cb_assert(it->first == i && it->second == i);
        ++i;
    }
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 4, chunk_list);
    cb_assert(chunk_list.size() == 3);
    it = chunk_list.begin();
    cb_assert(it->first == 0 && it->second == 3);
    ++it;
    cb_assert(it->first == 4 && it->second == 7);
    ++it;
    cb_assert(it->first == 8 && it->second == 9);
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 7, chunk_list);
    cb_assert(chunk_list.size() == 2);
    it = chunk_list.begin();
    cb_assert(it->first == 0 && it->second == 6);
    ++it;
    cb_assert(it->first == 7 && it->second == 9);
    chunk_list.clear();

    createChunkListFromArray(&elm_list, 11, chunk_list);
    cb_assert(chunk_list.size() == 1);
    it = chunk_list.begin();
    cb_assert(it->first == 0 && it->second == 9);
    chunk_list.clear();

    return 0;
}
