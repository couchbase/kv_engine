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

#include <memcached/tracer.h>

static inline cb::tracing::Traceable* cookie2traceable(const void* cc) {
    return reinterpret_cast<cb::tracing::Traceable*>(const_cast<void*>(cc));
}

/**
 * Traces a scope
 * Usage:
 *   {
 *     TRACE_SCOPE(cookie, "test1");
 *     ....
 *    }
 */
class ScopedTracer {
public:
    ScopedTracer(cb::tracing::Traceable& traceable, cb::tracing::Code code)
        : traceable(traceable) {
        if (traceable.isTracingEnabled()) {
            NonBucketAllocationGuard guard;
            spanId = traceable.getTracer().begin(code);
        }
    }

    /// Constructor from Cookie (void*)
    ScopedTracer(const void* cookie, cb::tracing::Code code)
        : ScopedTracer(*cookie2traceable(cookie), code) {
    }

    ~ScopedTracer() {
        if (traceable.isTracingEnabled()) {
            traceable.getTracer().end(spanId);
        }
    }

protected:
    cb::tracing::Traceable& traceable;

    /// ID of our Span.
    cb::tracing::SpanId spanId = {};
};

/**
 * Adapter class to assist in recording the duration of an event into
 * Tracer objects, by storing the cookie and code in the object. To be used
 * with ScopeTimer classes.
 *
 * The start and stop methods record the duration of the span and it is
 * injected into the provided traceable object as part of object destruction.
 */
class TracerStopwatch {
public:
    TracerStopwatch(cb::tracing::Traceable* traceable,
                    const cb::tracing::Code code)
        : traceable(traceable), code(code) {
    }

    /// Constructor from Cookie (void*)
    TracerStopwatch(const void* cookie, const cb::tracing::Code code)
        : TracerStopwatch(cookie2traceable(cookie), code) {
    }

    ~TracerStopwatch() {
        if (traceable && traceable->isTracingEnabled()) {
            NonBucketAllocationGuard guard;
            auto& tracer = traceable->getTracer();
            const auto spanId = tracer.begin(code, startTime);
            tracer.end(spanId, stopTime);
        }
    }

    bool isEnabled() const {
        if (traceable) {
            return traceable->isTracingEnabled();
        }
        return false;
    }

    void start(cb::tracing::Clock::time_point tp) {
        startTime = tp;
    }

    void stop(cb::tracing::Clock::time_point tp) {
        stopTime = tp;
    }

protected:
    cb::tracing::Traceable* const traceable;
    const cb::tracing::Code code;

    cb::tracing::Clock::time_point startTime;
    cb::tracing::Clock::time_point stopTime;
};
