/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "objectregistry.h"

#include <memcached/cookie_iface.h>
#include <memcached/tracer.h>

/**
 * Traces a scope
 * Usage:
 *   {
 *     TRACE_SCOPE(cookie, "test1");
 *     ....
 *    }
 */
template <typename EventId>
class ScopedTracer {
public:
    ScopedTracer(cb::tracing::Traceable<EventId>& traceable, EventId eventId)
        : traceable(traceable),
          start(cb::tracing::Clock::now()),
          eventId(eventId) {
    }

    ~ScopedTracer() {
        NonBucketAllocationGuard guard;
        traceable.getTracer().record(eventId, start, cb::tracing::Clock::now());
    }

protected:
    cb::tracing::Traceable<EventId>& traceable;
    cb::tracing::Clock::time_point start;
    EventId eventId;
};

/**
 * Adapter class to assist in recording the duration of an event into
 * Tracer objects, by storing the cookie and code in the object. To be used
 * with ScopeTimer classes.
 *
 * The start and stop methods record the duration of the span and it is
 * injected into the provided traceable object as part of object destruction.
 */
template <typename EventId>
class TracerStopwatch {
public:
    TracerStopwatch(cb::tracing::Traceable<EventId>& traceable,
                    const EventId code)
        : TracerStopwatch(&traceable, code) {
    }
    TracerStopwatch(cb::tracing::Traceable<EventId>* traceable,
                    const EventId eventId)
        : traceable(traceable), eventId(eventId) {
    }

    ~TracerStopwatch() {
        if (traceable) {
            NonBucketAllocationGuard guard;
            auto& tracer = traceable->getTracer();
            tracer.record(eventId, startTime, stopTime);
        }
    }

    void start(cb::tracing::Clock::time_point tp) {
        startTime = tp;
    }

    void stop(cb::tracing::Clock::time_point tp) {
        stopTime = tp;
    }

protected:
    cb::tracing::Traceable<EventId>* const traceable;
    const EventId eventId;

    cb::tracing::Clock::time_point startTime;
    cb::tracing::Clock::time_point stopTime;
};
