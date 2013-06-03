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

#include "workload.h"

workload_pattern_t WorkLoadPolicy::calculatePattern(const std::string &p) {
    if (p.compare("mix") == 0) {
        return MIX;
    } else if (p.compare("write") == 0) {
        return WRITE_HEAVY;
    } else {
        return READ_HEAVY;
    }
}

int WorkLoadPolicy::calculateNumReaders() {
    int readers;
    switch (pattern) {
        case MIX:
            readers = numShards;
            break;
        case WRITE_HEAVY:
            readers = getNumThreads(numShards, workload_low_priority);
            break;
        default: // READ_HEAVY
            readers = getNumThreads(numShards, workload_high_priority);
    }
    return readers;
}

int WorkLoadPolicy::calculateNumWriters() {
    int writers;
    switch (pattern) {
        case MIX:
            writers = numShards;
            break;
        case READ_HEAVY:
            writers = getNumThreads(numShards, workload_low_priority);
            break;
        default: // WRITE_HEAVY
            writers = getNumThreads(numShards, workload_high_priority);
    }
    return writers;
}

bool WorkLoadPolicy::validateNumWorkers(int r, int w,
                                        int shards, std::string policy) {
    workload_pattern_t p = calculatePattern(policy);
    if (p == MIX) {
        return ((r + w) == (shards * 2));
    } else {
        return ((r + w) <=
                (shards * (workload_high_priority + workload_low_priority)) + 0.5);
    }
}

int WorkLoadPolicy::getNumThreads(int shards, double priority) {
    return ((shards * priority) + 0.5);
}
