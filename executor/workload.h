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

#include <gsl/gsl-lite.hpp>
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
        : maxNumWorkers(m), numShards(numShards), workloadPattern(READ_HEAVY) {
        Expects(numShards > 0);
    }

    size_t getNumShards() {
        return numShards;
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
    int numShards;
    std::atomic<workload_pattern_t> workloadPattern;
};
