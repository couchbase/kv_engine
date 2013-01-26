/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "tapthrottle.hh"
#include "configuration.hh"

TapThrottle::TapThrottle(Configuration &config, EPStats &s) :
    queueCap(config.getTapThrottleQueueCap()),
    capPercent(config.getTapThrottleCapPcnt()),
    stats(s)
{}

bool TapThrottle::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.diskQueueSize.get();
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
        stats.tapThrottleWriteQueueCap.set(-1);
        return;
    }
    size_t qcap = static_cast<size_t>(queueCap);
    size_t throttleCap = 0;
    if (capPercent > 0) {
        throttleCap = (static_cast<double>(capPercent) / 100.0) * totalItems;
    }
    stats.tapThrottleWriteQueueCap.set(throttleCap > qcap ? throttleCap : qcap);
}
