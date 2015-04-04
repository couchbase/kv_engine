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
#include "common.h"

typedef enum {
    BFILTER_DISABLED,
    BFILTER_PENDING,
    BFILTER_COMPACTING,
    BFILTER_ENABLED
} bfilter_status_t;

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

    void addKey(const char *key, size_t keylen);
    bool maybeKeyExists(const char *key, uint32_t keylen);

    size_t getNumOfKeysInFilter();
    size_t getFilterSize();

private:
    size_t estimateFilterSize(size_t key_count, double false_positive_prob);
    size_t estimateNoOfHashes(size_t key_count);

    size_t filterSize;
    size_t noOfHashes;

    size_t keyCounter;

    bfilter_status_t status;
    std::vector<bool> bitArray;
};

#endif // SRC_BLOOMFILTER_H_
