/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "hash_table.h"
#include <hdrhistogram/hdrhistogram.h>

/**
 * Hash table visitor that finds the min and max bucket depths.
 */
class HashTableDepthStatVisitor : public HashTableDepthVisitor {
public:
    HashTableDepthStatVisitor() = default;

    void visit(size_t bucket, size_t depth, size_t mem) override {
        (void)bucket;
        min = std::min(min, depth);
        max = std::max(max, depth);
        depthHisto.add(depth);
        size += depth;
        memUsed += mem;
    }

    Hdr1sfInt32Histogram depthHisto;
    size_t size = 0;
    size_t memUsed = 0;
    size_t min = std::numeric_limits<size_t>::max();
    size_t max = 0;
};
