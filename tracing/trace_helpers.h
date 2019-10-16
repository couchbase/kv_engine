/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#pragma once

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
    ScopedTracer(cb::tracing::Traceable& traceable, cb::tracing::TraceCode code)
        : traceable(traceable) {
        if (traceable.isTracingEnabled()) {
            spanId = traceable.getTracer().begin(code);
        }
    }

    /// Constructor from Cookie (void*)
    ScopedTracer(const void* cookie, cb::tracing::TraceCode code)
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
 * It's stop() and start() methods (which only take a
 * std::chrono::steady_clock::time_point) use the stored cookie and TraceCode to
 * record the times in the appropriate tracer object.
 */
class TracerStopwatch {
public:
    TracerStopwatch(cb::tracing::Traceable* traceable,
                    const cb::tracing::TraceCode code)
        : traceable(traceable), code(code) {
    }

    /// Constructor from Cookie (void*)
    TracerStopwatch(const void* cookie, const cb::tracing::TraceCode code)
        : TracerStopwatch(cookie2traceable(cookie), code) {
    }

    void start(std::chrono::steady_clock::time_point startTime) {
        if (traceable && traceable->isTracingEnabled()) {
            spanId = traceable->getTracer().begin(code, startTime);
        }
    }

    void stop(std::chrono::steady_clock::time_point stopTime) {
        if (traceable && traceable->isTracingEnabled()) {
            traceable->getTracer().end(spanId, stopTime);
        }
    }

protected:
    cb::tracing::Traceable* const traceable;

    /// ID of our Span.
    cb::tracing::SpanId spanId = {};

    cb::tracing::TraceCode code;
};
