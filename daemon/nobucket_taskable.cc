/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "nobucket_taskable.h"
#include "memcached.h"

const std::string& NoBucketTaskable::getName() const {
    return name;
}

task_gid_t NoBucketTaskable::getGID() const {
    return uintptr_t(this);
}

bucket_priority_t NoBucketTaskable::getWorkloadPriority() const {
    return HIGH_BUCKET_PRIORITY;
}

void NoBucketTaskable::setWorkloadPriority(bucket_priority_t prio) {
    // ignore
}

WorkLoadPolicy& NoBucketTaskable::getWorkLoadPolicy() {
    return policy;
}

bool NoBucketTaskable::isShutdown() {
    return is_memcached_shutting_down();
}

NoBucketTaskable& NoBucketTaskable::instance() {
    static NoBucketTaskable instance;
    return instance;
}
