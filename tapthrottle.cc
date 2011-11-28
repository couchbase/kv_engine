/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "tapthrottle.hh"

const double TAP_THROTTLE_MEM_THRESHOLD(0.9);

bool TapThrottle::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.queue_size.get() + stats.flusher_todo.get();
    return queueSize < stats.tapThrottleWriteQueueCap;
}

bool TapThrottle::hasSomeMemory() const {
    double currentSize = static_cast<double>(stats.currentSize.get() + stats.memOverhead.get());
    double maxSize = static_cast<double>(stats.maxDataSize.get());

    return currentSize < (maxSize * TAP_THROTTLE_MEM_THRESHOLD);
}

bool TapThrottle::shouldProcess() const {
    return persistenceQueueSmallEnough() && hasSomeMemory();
}
