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

#include <memcached/engine.h>

#include "bloomfilter.h"

#include "murmurhash3.h"

#include <cmath>

#if __x86_64__ || __ppc64__
#define MURMURHASH_3 MurmurHash3_x64_128
#else
#define MURMURHASH_3 MurmurHash3_x86_128
#endif

BloomFilter::BloomFilter(size_t key_count, double false_positive_prob,
                         bfilter_status_t new_status) {

    status = new_status;
    filterSize = estimateFilterSize(key_count, false_positive_prob);
    noOfHashes = estimateNoOfHashes(key_count);
    keyCounter = 0;
    bitArray.assign(filterSize, false);
}

BloomFilter::~BloomFilter() {
    status = BFILTER_DISABLED;
    bitArray.clear();
}

size_t BloomFilter::estimateFilterSize(size_t key_count,
                                       double false_positive_prob) {
    return round(-(((double)(key_count) * log(false_positive_prob))
                                                    / (pow(log(2.0), 2))));
}

size_t BloomFilter::estimateNoOfHashes(size_t key_count) {
    return round(((double) filterSize / key_count) * (log(2.0)));
}

uint64_t BloomFilter::hashDocKey(const DocKey& key, uint32_t iteration) {
    uint64_t result = 0;
    auto hashable = key.getIdAndKey();
    uint32_t seed = iteration + (uint32_t(hashable.first) * noOfHashes);
    MURMURHASH_3(hashable.second.data(), hashable.second.size(), seed, &result);
    return result;
}

void BloomFilter::setStatus(bfilter_status_t to) {
    switch (status) {
        case BFILTER_DISABLED:
            if (to == BFILTER_ENABLED) {
                status = BFILTER_PENDING;
            }
            break;
        case BFILTER_PENDING:
            if (to == BFILTER_DISABLED) {
                status = to;
                bitArray.clear();
            } else if (to == BFILTER_COMPACTING) {
                status = to;
            }
            break;
        case BFILTER_COMPACTING:
            if (to == BFILTER_DISABLED) {
                status = to;
                bitArray.clear();
            } else if (to == BFILTER_ENABLED) {
                status = to;
            }
            break;
        case BFILTER_ENABLED:
            if (to == BFILTER_DISABLED) {
                status = to;
                bitArray.clear();
            } else if (to == BFILTER_COMPACTING) {
                status = to;
            }
            break;
    }
}

bfilter_status_t BloomFilter::getStatus() {
    return status;
}

std::string BloomFilter::getStatusString() {
    switch (status) {
        case BFILTER_DISABLED:
            return "DISABLED";
        case BFILTER_PENDING:
            return "PENDING (ENABLED)";
        case BFILTER_COMPACTING:
            return "COMPACTING";
        case BFILTER_ENABLED:
            return "ENABLED";
    }
    return "UNKNOWN";
}

void BloomFilter::addKey(const DocKey& key) {
    if (status == BFILTER_COMPACTING || status == BFILTER_ENABLED) {
        bool overlap = true;
        for (uint32_t i = 0; i < noOfHashes; i++) {
            uint64_t result = hashDocKey(key, i);
            if (overlap && bitArray[result % filterSize] == 0) {
                overlap = false;
            }
            bitArray[result % filterSize] = 1;
        }
        if (!overlap) {
            keyCounter++;
        }
    }
}

bool BloomFilter::maybeKeyExists(const DocKey& key) {
    if (status == BFILTER_COMPACTING || status == BFILTER_ENABLED) {
        for (uint32_t i = 0; i < noOfHashes; i++) {
            uint64_t result = hashDocKey(key, i);
            if (bitArray[result % filterSize] == 0) {
                // The key does NOT exist.
                return false;
            }
        }
    }
    // The key may exist.
    return true;
}

size_t BloomFilter::getNumOfKeysInFilter() {
    if (status == BFILTER_COMPACTING || status == BFILTER_ENABLED) {
        return keyCounter;
    } else {
        return 0;
    }
}

size_t BloomFilter::getFilterSize() {
    if (status == BFILTER_COMPACTING || status == BFILTER_ENABLED) {
        return filterSize;
    } else {
        return 0;
    }
}
