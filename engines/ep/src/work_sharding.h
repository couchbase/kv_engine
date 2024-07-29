/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "memcached/vbucket.h"

/**
 * Selects a worker based on the vbid and a distribution key.
 *
 * Every vBucket has a statically assigned set of workers. The distribution key
 * is used to select a worker from that set.
 *
 * When the number of workers is less than the number of vBuckets, uses vbid
 * modulo workers.
 * When the number of workers is N x vBuckets, assigns exactly N workers per
 * vBucket.
 * When the number of workers is not an exact multiple of the number of
 * vBuckets, first assigns vBuckets N workers, then any K remaining workers are
 * assigned to the first K vBuckets (so some vBuckets get more workers).
 *
 * @param numWorkers the number of allocated workers (this could be threads,
 * BGFetchers, etc.)
 * @param numVBs the total number of vBuckets which will be utilising this pool
 * of workers
 * @param vbid the vbid of the vBucket we want to assign a worker to
 * @param distributionKey when the number of workers per vBucket is > 1, used to
 * select a worker from that set
 */
size_t selectWorkerForVBucket(size_t numWorkers,
                              size_t numVBs,
                              Vbid vbid,
                              uint64_t distributionKey);
