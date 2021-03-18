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
#pragma once

#include <atomic>

#include <platform/sysinfo.h>
#include <string>

enum bucket_priority_t {
    HIGH_BUCKET_PRIORITY=6,
    LOW_BUCKET_PRIORITY=2,
    NO_BUCKET_PRIORITY=0
};

enum workload_pattern_t {
    READ_HEAVY,
    WRITE_HEAVY,
    MIXED
};

/**
 * Workload optimization policy
 */
class WorkLoadPolicy {
public:
    WorkLoadPolicy(int m, int numShards)
        : maxNumWorkers(m),
          maxNumShards(numShards),
          workloadPattern(READ_HEAVY) {
        if (maxNumShards == 0) {
            // If user didn't specify a maximum shard count, then auto-select
            // based on the number of available CPUs.
            maxNumShards = cb::get_available_cpu_count();

            // Sanity - must always have at least 1 shard., but not more than
            // 128 given we don't have machines commonly available to test at
            // greater sizes.
            maxNumShards = std::max(1, maxNumShards);
            maxNumShards = std::min(128, maxNumShards);
        }
    }

    size_t getNumShards() {
        return maxNumShards;
    }

    bucket_priority_t getBucketPriority() const {
        if (maxNumWorkers < HIGH_BUCKET_PRIORITY) {
            return LOW_BUCKET_PRIORITY;
        }
        return HIGH_BUCKET_PRIORITY;
    }

    size_t getNumWorkers() {
        return maxNumWorkers;
    }

    workload_pattern_t getWorkLoadPattern() {
        return workloadPattern.load();
    }

    std::string stringOfWorkLoadPattern() {
        switch (workloadPattern.load()) {
        case READ_HEAVY:
            return "read_heavy";
        case WRITE_HEAVY:
            return "write_heavy";
        default:
            return "mixed";
        }
    }

    void setWorkLoadPattern(workload_pattern_t pattern) {
        workloadPattern.store(pattern);
    }

private:

    int maxNumWorkers;
    int maxNumShards;
    std::atomic<workload_pattern_t> workloadPattern;
};
