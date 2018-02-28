/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "items.h"
#include "assoc.h"

#include <benchmark/benchmark.h>
#include <platform/cb_malloc.h>
#include <platform/crc32c.h>
#include <random>

const uint32_t max_items = 100000;

hash_key* item_get_key(const hash_item* item) {
    const char *ret = reinterpret_cast<const char*>(item + 1);
    return (hash_key*)ret;
}

static void hash_key_create(hash_key* hkey, uint32_t ii) {
    char key[10];
    auto nkey = snprintf(key, sizeof(key), "%08" PRIu32, ii);
    auto hash_key_len = sizeof(bucket_id_t) + nkey;

    if (size_t(nkey) > sizeof(hkey->key_storage.client_key)) {
        hkey->header.full_key =
            static_cast<hash_key_data*>(cb_malloc(hash_key_len));
        if (hkey->header.full_key == NULL) {
            throw std::bad_alloc();
        }
    } else {
        hkey->header.full_key = (hash_key_data*)&hkey->key_storage;
    }
    hash_key_set_len(hkey, hash_key_len);
    hash_key_set_bucket_index(hkey, 0);
    hash_key_set_client_key(hkey, key, nkey);
}

/*
 * The item object stores a hash_key in a contiguous allocation
 * This method ensures correct copying into a contiguous hash_key
 */
static void hash_key_copy_to_item(hash_item* dst, const hash_key* src) {
    hash_key* key = item_get_key(dst);
    memcpy(key, src, sizeof(hash_key_header));
    key->header.full_key = (hash_key_data*)&key->key_storage;
    memcpy(hash_key_get_key(key), hash_key_get_key(src), hash_key_get_key_len(src));
}

hash_item *do_item_alloc(const hash_key *key) {
    size_t ntotal = sizeof(hash_item) + hash_key_get_alloc_size(key);
    auto* it = static_cast<hash_item*>(calloc(ntotal, 1));
    if (it == nullptr) {
        throw std::bad_alloc();
    }

    hash_key_copy_to_item(it, key);
    return it;
}

hash_item *item_alloc(uint32_t key) {
    hash_key hkey;
    hash_key_create(&hkey, key);
    auto* it = do_item_alloc(&hkey);
    return it;
}

void AccessSingleItem(benchmark::State& state) {
    hash_key hkey;
    hash_key_create(&hkey, 0);
    while (state.KeepRunning()) {
        if (assoc_find(crc32c(hash_key_get_key(&hkey),
                                 hash_key_get_key_len(&hkey), 0),
                       &hkey) == nullptr) {
            throw std::logic_error("AccessSingleItem: Expected to find key");
        }
    }
}

void AccessRandomItems(benchmark::State& state) {
    std::random_device rd;
    std::minstd_rand0 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;

    while (state.KeepRunning()) {
        uint32_t id = dis(gen) % max_items;
        hash_key hkey;
        hash_key_create(&hkey, id);
        if (assoc_find(crc32c(hash_key_get_key(&hkey),
                              hash_key_get_key_len(&hkey), 0),
                       &hkey) == nullptr) {
            throw std::logic_error("AccessRandomItems: Expected to find key");
        }
    }
}

BENCHMARK(AccessSingleItem)->ThreadRange(1, 16);
BENCHMARK(AccessRandomItems)->ThreadRange(1, 16);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return EXIT_FAILURE;
    }

    assoc_init(nullptr);

    // Populate the cache
    for (uint32_t ii = 0; ii < max_items; ++ii) {
        auto* it = item_alloc(ii);
        hash_key hkey;
        hash_key_create(&hkey, ii);
        assoc_insert(crc32c(hash_key_get_key(&hkey),
                            hash_key_get_key_len(&hkey),
                            0),
                     it);
    }

    // Wait until the assoc table is rebalanced
    while (assoc_expanding()) {
        usleep(250);
    }

    ::benchmark::RunSpecifiedBenchmarks();

    for (uint32_t ii = 0; ii < max_items; ++ii) {
        hash_key hkey;
        hash_key_create(&hkey, ii);
        auto hash = crc32c(hash_key_get_key(&hkey),
                          hash_key_get_key_len(&hkey), 0);

        auto* it = assoc_find(hash, &hkey);
        assoc_delete(hash, &hkey);
        free(static_cast<void*>(it));
    }

    assoc_destroy();

    return EXIT_SUCCESS;
}
