/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TAPTHROTTLE_HH
#define TAPTHROTTLE_HH 1

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

/**
 * Monitors various internal state to report whether we should
 * throttle incoming tap.
 */
class TapThrottle {
public:

    TapThrottle(EPStats &s) : stats(s) {}

    /**
     * If true, we should process incoming tap requests.
     */
    bool shouldProcess() const;
private:

    bool persistenceQueueSmallEnough() const;

    bool hasSomeMemory() const;

    EPStats &stats;
};

#endif // TAPTHROTTLE_HH
