/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_taskable.h"

#include <utility>

MockTaskable::MockTaskable(std::string name, bucket_priority_t priority)
    : name(std::move(name)), policy(priority, 1) {
}

const std::string& MockTaskable::getName() const {
    return name;
}

task_gid_t MockTaskable::getGID() const {
    return 0;
}

bucket_priority_t MockTaskable::getWorkloadPriority() const {
    return policy.getBucketPriority();
}

void MockTaskable::setWorkloadPriority(bucket_priority_t prio) {
}

WorkLoadPolicy& MockTaskable::getWorkLoadPolicy() {
    return policy;
}

void MockTaskable::logRunTime(const GlobalTask& task,
                              std::string_view threadName,
                              std::chrono::steady_clock::duration runTime) {
}

bool MockTaskable::isShutdown() {
    return false;
}
