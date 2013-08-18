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

#include "config.h"

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

size_t WorkLoadPolicy::calculateNumReaders() {
    size_t readers;
    switch (pattern) {
        case MIX:
            readers = getNumShards();
            break;
        case WRITE_HEAVY:
            readers = maxNumWorkers - getNumShards();
            break;
        default: // READ_HEAVY
            readers = getNumShards();
    }
    return readers;
}

size_t WorkLoadPolicy::calculateNumWriters() {
    size_t writers;
    switch (pattern) {
        case MIX:
            writers = maxNumWorkers - getNumShards();
            break;
        case READ_HEAVY:
            writers = maxNumWorkers - getNumShards();
            break;
        default: // WRITE_HEAVY
            writers = getNumShards();
    }
    return writers;
}


size_t WorkLoadPolicy::getNumShards(void) {
    if (pattern == MIX) {
        return ((maxNumWorkers * 0.5) + 0.5);
    } else {
        // number of shards cannot be greater than total number of
        // high priority worker threads
        return ((maxNumWorkers * workload_high_priority) + 0.5);
    }
}
