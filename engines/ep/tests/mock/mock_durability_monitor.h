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

#include "durability/active_durability_monitor.h"
#include "stored-value.h"

/*
 * Wrapper to DurabilityMonitor, exposes protected methods for testing.
 */
class MockActiveDurabilityMonitor : public ActiveDurabilityMonitor {
public:
    MockActiveDurabilityMonitor(VBucket& vb) : ActiveDurabilityMonitor(vb) {
    }

    size_t public_getNumTracked() const {
        std::lock_guard<std::mutex> lg(state.m);
        return ActiveDurabilityMonitor::getNumTracked(lg);
    }

    uint8_t public_getFirstChainSize() const {
        std::lock_guard<std::mutex> lg(state.m);
        return ActiveDurabilityMonitor::getFirstChainSize(lg);
    }

    uint8_t public_getFirstChainMajority() const {
        std::lock_guard<std::mutex> lg(state.m);
        return ActiveDurabilityMonitor::getFirstChainMajority(lg);
    }

    NodeSeqnos public_getNodeWriteSeqnos(const std::string& replica) const {
        std::lock_guard<std::mutex> lg(state.m);
        return ActiveDurabilityMonitor::getNodeWriteSeqnos(lg, replica);
    }

    NodeSeqnos public_getNodeAckSeqnos(const std::string& replica) const {
        std::lock_guard<std::mutex> lg(state.m);
        return ActiveDurabilityMonitor::getNodeAckSeqnos(lg, replica);
    }

    std::unordered_set<int64_t> public_getTrackedSeqnos() const {
        return ActiveDurabilityMonitor::getTrackedSeqnos();
    }

    size_t public_wipeTracked() {
        return ActiveDurabilityMonitor::wipeTracked();
    }
};
