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
    READ_HEAVY,
    WRITE_HEAVY,
    MIX
} workload_pattern_t;

typedef enum {
    HIGH_BUCKET_PRIORITY=7,
    MEDIUM_BUCKET_PRIORITY=4,
    LOW_BUCKET_PRIORITY=2,
    NO_BUCKET_PRIORITY=0
} bucket_priority_t;

const double workload_high_priority=0.6;
const double workload_low_priority=0.4;

/**
 * Workload optimization policy
 */
class WorkLoadPolicy {
public:
    WorkLoadPolicy(int m, const std::string p)
        : pattern(calculatePattern(p)), maxNumWorkers(m) { }

    /**
     * Caculate workload pattern based on configuraton
     * parameter
     */
    static workload_pattern_t calculatePattern(const std::string &p);

    /**
     * Caculate max number of readers based on current
     * number of shards and access workload pattern.
     */
    size_t calculateNumReaders();

    /**
     * Caculate max number of writers based on current
     * number of shards and access workload pattern.
     */
    size_t calculateNumWriters();

    /**
     * reset workload pattern
     */
    void resetWorkLoadPattern(const std::string p) {
        if (p.compare("mix")) {
            pattern = MIX;
        } else if (p.compare("write")) {
            pattern = WRITE_HEAVY;
        } else {
            pattern = READ_HEAVY;
        }
    }

    /**
     * get number of shards based on this workload policy
     */
    size_t getNumShards(void);

    /**
     * get current workload pattern name
     */
    const char *getWorkloadPattern(void) {
        switch (pattern) {
        case MIX:
            return "Optimized for random data access";
        case READ_HEAVY:
            return "Optimized for read data access";
        case WRITE_HEAVY:
            return "Optimized for write data access";
        default:
            return "Undefined workload pattern";
        }
    }

private:

    workload_pattern_t pattern;
    int maxNumWorkers;
};

#endif  // SRC_WORKLOAD_H_
