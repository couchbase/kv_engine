/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "work_sharding.h"

#include <folly/hash/Hash.h>

size_t selectWorkerForVBucket(size_t numWorkers,
                              size_t numVBs,
                              Vbid vbid,
                              uint64_t distributionKey) {
    // When vBuckets > workers, just use modulo.
    // numVBs can be 0, if we have no vBuckets matching the state we try to
    // distribute. For BGFetchers where we distribute based on active vBuckets,
    // in some states we might only have no active vBuckets, but replica reads.
    if (numWorkers <= numVBs || !numVBs) {
        return vbid.get() % numWorkers;
    }

    // More workers than vBuckets. In that case, at least a subset
    // of vBuckets can use more than 1 worker.
    const auto minWorkersPerVBucket = numWorkers / numVBs;

    // Normalize the vBucket ID to an index between 0 and the number of
    // vBuckets.
    const size_t normalizedIndex = vbid.get() % numVBs;
    // Calculate how many workers we have "assigned" to this vBucket.
    // If the ratio between workers and vBuckets is 2:1, then each vBucket
    // will have 2 workers.
    // If the ratio is 3:2, then half of the vBuckets will have 2 workers,
    // and the other half will have only 1. Assuming 15 workers and 10
    // consecutive vBuckets (0-9), the first 5 vBuckets will be assigned two
    // workers. In that case, vb:3 will have workers 3 and 18.
    const bool hasExtraWorker = normalizedIndex < (numWorkers % numVBs);
    const size_t workersForVBucket =
            minWorkersPerVBucket + static_cast<size_t>(hasExtraWorker);

    // Pick the worker to use.
    const size_t ownWorkerIndex =
            folly::hash::twang_mix64(distributionKey) % workersForVBucket;
    // Calculate the worker index.
    return normalizedIndex + ownWorkerIndex * numVBs;
}
