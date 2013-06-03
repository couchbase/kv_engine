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

typedef enum {
    READ_HEAVY,
    WRITE_HEAVY,
    MIX
} workload_pattern_t;

const double workload_high_priority=1.0;
const double workload_low_priority=0.5;

/**
 * Workload optimization policy
 */
class WorkLoadPolicy {
public:
    WorkLoadPolicy(int s, const std::string p)
        : numShards(s), pattern(calculatePattern(p)) { }

    /**
     * Caculate workload pattern based on configuraton
     * parameter
     */
    static workload_pattern_t calculatePattern(const std::string &p);

    /**
     * Caculate max number of readers based on current
     * number of shards and access workload pattern.
     */
    int calculateNumReaders();

    /**
     * Caculate max number of writers based on current
     * number of shards and access workload pattern.
     */
    int calculateNumWriters();

    /**
     * validate given numbers of readers and
     * writers based on number of shards and workload policy
     */
    static bool validateNumWorkers(int r, int w,
                                   int shards, std::string policy);

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
     * reset total number of shards
     */
    void resetNumShards(int s) {
        numShards = s;
    }

    /**
     * get current number of shards
     */
    int getNumShards(void) { return numShards; }

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
        }
    }

private:
    int getNumThreads(int shards, double priority);
    int numShards;
    workload_pattern_t pattern;
};
#endif /* SRC_WORKLOAD_H_ */
