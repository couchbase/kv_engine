/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "configuration.h"
#include "tapthrottle.h"

TapThrottle::TapThrottle(Configuration &config, EPStats &s) :
    queueCap(config.getTapThrottleQueueCap()),
    capPercent(config.getTapThrottleCapPcnt()),
    stats(s)
{}

bool TapThrottle::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.diskQueueSize.load();
    if (stats.tapThrottleWriteQueueCap == -1) {
        return true;
    }
    return queueSize < static_cast<size_t>(stats.tapThrottleWriteQueueCap);
}

bool TapThrottle::hasSomeMemory() const {
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    double maxSize = static_cast<double>(stats.getMaxDataSize());

    return memoryUsed < (maxSize * stats.tapThrottleThreshold);
}

bool TapThrottle::shouldProcess() const {
    return persistenceQueueSmallEnough() && hasSomeMemory();
}

void TapThrottle::adjustWriteQueueCap(size_t totalItems) {
    if (queueCap == -1) {
        stats.tapThrottleWriteQueueCap.store(-1);
        return;
    }
    size_t qcap = static_cast<size_t>(queueCap);
    size_t throttleCap = 0;
    if (capPercent > 0) {
        throttleCap = (static_cast<double>(capPercent) / 100.0) * totalItems;
    }
    stats.tapThrottleWriteQueueCap.store(throttleCap > qcap ? throttleCap : qcap);
}
