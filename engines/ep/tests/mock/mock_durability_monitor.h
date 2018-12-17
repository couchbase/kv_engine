/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "durability_monitor.h"
#include "stored-value.h"

/*
 * Wrapper to DurabilityMonitor, exposes protected methods for testing.
 */
class MockDurabilityMonitor : public DurabilityMonitor {
public:
    MockDurabilityMonitor(VBucket& vb) : DurabilityMonitor(vb) {
    }

    size_t public_getNumTracked() const {
        std::lock_guard<std::mutex> lg(state.m);
        return DurabilityMonitor::getNumTracked(lg);
    }

    int64_t public_getReplicaMemorySeqno(const std::string& replicaUUID) const {
        std::lock_guard<std::mutex> lg(state.m);
        return DurabilityMonitor::getReplicaMemorySeqno(lg, replicaUUID);
    }
};

/*
 * @todo: Remove as soon as VBucket is ready for Milestone A1
 */
class MockStoredValue : public StoredValue {
public:
    MockStoredValue(const Item& item, EPStats& stats)
        : StoredValue(item, nullptr /*next*/, stats, false /*isOrdered*/) {
    }
};
