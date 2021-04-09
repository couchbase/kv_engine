/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <string>
#include <vector>

struct DocKey;

enum bfilter_status_t {
    BFILTER_DISABLED,
    BFILTER_PENDING,
    BFILTER_COMPACTING,
    BFILTER_ENABLED
};

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
    size_t getNoOfHashes() const;

protected:
    size_t estimateFilterSize(size_t key_count, double false_positive_prob);
    size_t estimateNoOfHashes(size_t key_count);

    uint64_t hashDocKey(const DocKey& key, uint32_t iteration);

    size_t filterSize;
    size_t noOfHashes;

    size_t keyCounter;

    bfilter_status_t status;
    std::vector<bool> bitArray;
};
