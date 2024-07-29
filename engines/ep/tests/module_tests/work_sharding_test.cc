/*
 *     Copyright 2o24-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "work_sharding.h"
#include <folly/hash/Hash.h>
#include <folly/portability/GTest.h>

/**
 * @return a distribution key for selectWorkerForVBucket which will result in
 * the specified N-th worker being selected from the set of possible workers
 * matching the vBucket.
 */
uint64_t keyForWorker(uint64_t n) {
    return folly::hash::twang_unmix64(n);
}

TEST(WorkSharding, LessWorkersThanVBuckets) {
    int workers = 10;

    // More vBuckets than workers, means we just use vbid % nWorkers.
    EXPECT_EQ(0,
              selectWorkerForVBucket(workers, 100, Vbid(0), keyForWorker(0)));
    EXPECT_EQ(1,
              selectWorkerForVBucket(workers, 100, Vbid(1), keyForWorker(0)));
    EXPECT_EQ(9,
              selectWorkerForVBucket(workers, 100, Vbid(9), keyForWorker(0)));
    EXPECT_EQ(9,
              selectWorkerForVBucket(workers, 100, Vbid(19), keyForWorker(0)));
    EXPECT_EQ(9,
              selectWorkerForVBucket(workers, 100, Vbid(119), keyForWorker(0)));
    EXPECT_EQ(9,
              selectWorkerForVBucket(workers, 100, Vbid(219), keyForWorker(0)));
}

TEST(WorkSharding, WorkersIsMultipleOfVBuckets) {
    int workers = 100;

    // All vBuckets get 10 workers.
    EXPECT_EQ(0, selectWorkerForVBucket(workers, 10, Vbid(0), keyForWorker(0)));
    EXPECT_EQ(1, selectWorkerForVBucket(workers, 10, Vbid(1), keyForWorker(0)));
    EXPECT_EQ(9, selectWorkerForVBucket(workers, 10, Vbid(9), keyForWorker(0)));
    // vBucket 19 gets workers 9, 19, 29,...
    EXPECT_EQ(9,
              selectWorkerForVBucket(workers, 10, Vbid(19), keyForWorker(0)));
    EXPECT_EQ(19,
              selectWorkerForVBucket(workers, 10, Vbid(19), keyForWorker(1)));
    EXPECT_EQ(29,
              selectWorkerForVBucket(workers, 10, Vbid(19), keyForWorker(2)));
    EXPECT_EQ(9,
              selectWorkerForVBucket(workers, 10, Vbid(19), keyForWorker(100)));
}

TEST(WorkSharding, WorkersIsMoreThanVBuckets) {
    int workers = 100;

    // All vBuckets get at least 1 worker.
    EXPECT_EQ(0, selectWorkerForVBucket(workers, 75, Vbid(0), keyForWorker(0)));
    // And vBuckets 0-25 get an additional worker, total of 2.
    EXPECT_EQ(75,
              selectWorkerForVBucket(workers, 75, Vbid(0), keyForWorker(1)));
    EXPECT_EQ(0, selectWorkerForVBucket(workers, 75, Vbid(0), keyForWorker(2)));
    EXPECT_EQ(0,
              selectWorkerForVBucket(workers, 75, Vbid(0), keyForWorker(100)));
}
