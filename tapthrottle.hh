/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TAPTHROTTLE_HH
#define TAPTHROTTLE_HH 1

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

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

#endif // TAPTHROTTLE_HH
