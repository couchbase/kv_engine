/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "item_compressor_visitor.h"
#include "vbucket.h"
#include <platform/compress.h>

// ItemCompressorVisitor implementation //////////////////////////////

ItemCompressorVisitor::ItemCompressorVisitor()
    : compressed_count(0),
      visited_count(0),
      currentVb(nullptr),
      currentMinCompressionRatio(0.0) {
}

ItemCompressorVisitor::~ItemCompressorVisitor() {
}

void ItemCompressorVisitor::setDeadline(
        std::chrono::steady_clock::time_point deadline) {
    progressTracker.setDeadline(deadline);
}

bool ItemCompressorVisitor::visit(const HashTable::HashBucketLock& lh,
                                  StoredValue& v) {

    // Check if the item can be compressed
    if (compressMode == BucketCompressionMode::Active && v.isCompressible()) {
        cb::compression::Buffer deflated;
        if (cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                     {v.getValue()->getData(), v.valuelen()},
                                     deflated)) {
            auto comp_ratio = static_cast<float>(v.valuelen()) /
                              static_cast<float>(deflated.size());

            // Compress the document only if the compression ratio is greater
            // than or equal to the current minium compression ratio
            if (comp_ratio >= currentMinCompressionRatio) {
                currentVb->ht.storeCompressedBuffer(lh, deflated, v);

                // If the value was compressed, increment the count of number
                // of compressed documents
                compressed_count++;
            } else {
                v.setUncompressible();
            }
        }
    }

    visited_count++;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    return progressTracker.shouldContinueVisiting(visited_count);
}

void ItemCompressorVisitor::clearStats() {
    compressed_count = 0;
    visited_count = 0;
}

size_t ItemCompressorVisitor::getCompressedCount() const {
    return compressed_count;
}

size_t ItemCompressorVisitor::getVisitedCount() const {
    return visited_count;
}

void ItemCompressorVisitor::setCompressionMode(
        const BucketCompressionMode compressionMode) {
    compressMode = compressionMode;
}

void ItemCompressorVisitor::setCurrentVBucket(VBucket& vb) {
    currentVb = &vb;
}

void ItemCompressorVisitor::setMinCompressionRatio(float minCompressionRatio) {
    currentMinCompressionRatio = minCompressionRatio;
}
