/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "tapthrottle.hh"


bool TapThrottle::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.queue_size.get() + stats.flusher_todo.get();
    if (stats.tapThrottleWriteQueueCap == static_cast<size_t>(-1)) {
        return true;
    }
    return queueSize < stats.tapThrottleWriteQueueCap;
}

bool TapThrottle::hasSomeMemory() const {
    double currentSize = static_cast<double>(stats.currentSize.get() + stats.memOverhead.get());
    double maxSize = static_cast<double>(stats.maxDataSize.get());

    return currentSize < (maxSize * stats.tapThrottleThreshold);
}

bool TapThrottle::shouldProcess() const {
    return persistenceQueueSmallEnough() && hasSomeMemory();
}
