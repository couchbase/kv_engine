/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <atomic>

#include <platform/sysinfo.h>
#include <string>

enum bucket_priority_t {
    HIGH_BUCKET_PRIORITY = 6,
    LOW_BUCKET_PRIORITY = 2,
    NO_BUCKET_PRIORITY = 0
};

enum workload_pattern_t { READ_HEAVY, WRITE_HEAVY, MIXED };

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
