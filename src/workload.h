/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#ifndef SRC_WORKLOAD_H_
#define SRC_WORKLOAD_H_ 1

#include "config.h"
#include <string>
#include "common.h"

typedef enum {
    HIGH_BUCKET_PRIORITY=5,
    LOW_BUCKET_PRIORITY=2,
    NO_BUCKET_PRIORITY=0
} bucket_priority_t;

/**
 * Workload optimization policy
 */
class WorkLoadPolicy {
public:
    WorkLoadPolicy(int m, int s)
        : maxNumWorkers(m), maxNumShards(s) { }

    /**
     * Return the number of readers based on the
     * number of shards.
     */
    size_t getNumReaders();

    /**
     * Calculate number of auxillary IO workers
     * If high priority bucket then set to 2
     * If low priority bucket then set to 1
     */
    size_t getNumAuxIO(void);

    /**
     * Return the number of writers based on the
     * number of shards.
     */
    size_t getNumWriters();

    /**
     * get number of shards based on this workload policy
     */
    size_t getNumShards(void);


    bucket_priority_t getBucketPriority(void) {
        if (maxNumWorkers < HIGH_BUCKET_PRIORITY) {
            return LOW_BUCKET_PRIORITY;
        }
        return HIGH_BUCKET_PRIORITY;
    }

    size_t getNumWorkers(void) {
        return maxNumWorkers;
    }

private:

    int maxNumWorkers;
    int maxNumShards;
};

#endif  // SRC_WORKLOAD_H_
