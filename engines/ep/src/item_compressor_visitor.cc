/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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
                currentVb->ht.storeCompressedBuffer(deflated, v);

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
