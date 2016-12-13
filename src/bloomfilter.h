/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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

#ifndef SRC_BLOOMFILTER_H_
#define SRC_BLOOMFILTER_H_ 1

#include "config.h"

#include <string>
#include <vector>

#include <memcached/dockey.h>

#include "murmurhash3.h"

enum bfilter_status_t {
    BFILTER_DISABLED,
    BFILTER_PENDING,
    BFILTER_COMPACTING,
    BFILTER_ENABLED
};

#if __x86_64__ || __ppc64__
#define MURMURHASH_3 MurmurHash3_x64_128
#else
#define MURMURHASH_3 MurmurHash3_x86_128
#endif

/**
 * A bloom filter instance for a vbucket.
 * We are to maintain the vbucket-number of these instances.
 *
 * Each vbucket will hold one such object.
 */
class BloomFilter {
public:
    BloomFilter(size_t key_count, double false_positive_prob,
                bfilter_status_t newStatus = BFILTER_DISABLED);
    ~BloomFilter();

    void setStatus(bfilter_status_t to);
    bfilter_status_t getStatus();
    std::string getStatusString();

    void addKey(const DocKey& key);
    bool maybeKeyExists(const DocKey& key);

    size_t getNumOfKeysInFilter();
    size_t getFilterSize();

protected:
    size_t estimateFilterSize(size_t key_count, double false_positive_prob);
    size_t estimateNoOfHashes(size_t key_count);

    uint64_t hashDocKey(const DocKey& key, uint32_t iteration) {
        uint64_t result = 0;
        uint32_t seed = iteration + (uint32_t(key.getDocNamespace()) * noOfHashes);
        MURMURHASH_3(key.data(), key.size(), seed, &result);
        return result;
    }

    size_t filterSize;
    size_t noOfHashes;

    size_t keyCounter;

    bfilter_status_t status;
    std::vector<bool> bitArray;
};

#endif // SRC_BLOOMFILTER_H_
