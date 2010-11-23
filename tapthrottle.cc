/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "tapthrottle.hh"

const size_t MINIMUM_SPACE(1024 * 1024);
const size_t MAXIMUM_QUEUE(100000);

bool TapThrottle::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.queue_size.get() + stats.flusher_todo.get();
    return queueSize < MAXIMUM_QUEUE;
}

bool TapThrottle::hasSomeMemory() const {
    size_t currentSize = stats.currentSize.get() + stats.memOverhead.get();
    size_t maxSize = stats.maxDataSize.get();

    return currentSize < maxSize && (maxSize - currentSize) > MINIMUM_SPACE;
}

bool TapThrottle::shouldProcess() const {
    return persistenceQueueSmallEnough() && hasSomeMemory();
}
