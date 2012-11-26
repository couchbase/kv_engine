/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_TAPTHROTTLE_H_
#define SRC_TAPTHROTTLE_H_ 1

#include "config.h"

#include "common.h"
#include "dispatcher.h"
#include "stats.h"

class Configuration;

/**
 * Monitors various internal state to report whether we should
 * throttle incoming tap.
 */
class TapThrottle {
public:

    TapThrottle(Configuration &config, EPStats &s);

    /**
     * If true, we should process incoming tap requests.
     */
    bool shouldProcess() const;

    void setCapPercent(size_t perc) { capPercent = perc; }
    void setQueueCap(ssize_t cap) { queueCap = cap; }

    void adjustWriteQueueCap(size_t totalItems);

private:
    bool persistenceQueueSmallEnough() const;
    bool hasSomeMemory() const;

    ssize_t queueCap;
    size_t capPercent;
    EPStats &stats;
};

#endif  // SRC_TAPTHROTTLE_H_
