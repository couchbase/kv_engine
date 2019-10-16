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

#include "tracer.h"

#include "daemon/cookie.h"

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
    ScopedTracer(Cookie& cookie, const cb::tracing::TraceCode code)
        : cookie(cookie) {
        if (cookie.isTracingEnabled()) {
            spanId = cookie.getTracer().begin(code);
        }
    }

    /// Constructor from Cookie (void*)
    ScopedTracer(const void* cookie, const cb::tracing::TraceCode code)
        : ScopedTracer(*reinterpret_cast<Cookie*>(const_cast<void*>(cookie)),
                       code) {
    }

    ~ScopedTracer() {
        if (cookie.isTracingEnabled()) {
            cookie.getTracer().end(spanId);
        }
    }

protected:
    Cookie& cookie;

    /// ID of our Span.
    cb::tracing::Tracer::SpanId spanId = {};
};

/**
 * Simple helper class which adds a trace begin or end event instantly
 * when it is created.
 * For use when the start & stop times are not inside the same scope; and hence
 * need the ability to specify them individually.
 */
class InstantTracer {
public:
    InstantTracer(Cookie& cookie,
                  const cb::tracing::TraceCode code,
                  bool begin,
                  std::chrono::steady_clock::time_point time =
                          std::chrono::steady_clock::now()) {
        if (cookie.isTracingEnabled()) {
            auto& tracer = cookie.getTracer();
            if (begin) {
                tracer.begin(code, time);
            } else {
                tracer.end(code, time);
            }
        }
    }

    /// Constructor from Cookie (void*)
    InstantTracer(const void* cookie,
                  const cb::tracing::TraceCode code,
                  bool begin,
                  std::chrono::steady_clock::time_point time =
                          std::chrono::steady_clock::now())
        : InstantTracer(*reinterpret_cast<Cookie*>(const_cast<void*>(cookie)),
                        code,
                        begin,
                        time) {
    }
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
    TracerStopwatch(Cookie& cookie, const cb::tracing::TraceCode code)
        : cookie(cookie), code(code) {
    }

    /// Constructor from Cookie (void*)
    TracerStopwatch(const void* cookie, const cb::tracing::TraceCode code)
        : TracerStopwatch(*reinterpret_cast<Cookie*>(const_cast<void*>(cookie)),
                          code) {
    }

    void start(std::chrono::steady_clock::time_point startTime) {
        if (cookie.isTracingEnabled()) {
            spanId = cookie.getTracer().begin(code, startTime);
        }
    }

    void stop(std::chrono::steady_clock::time_point stopTime) {
        if (cookie.isTracingEnabled()) {
            cookie.getTracer().end(spanId, stopTime);
        }
    }

protected:
    Cookie& cookie;

    /// ID of our Span.
    cb::tracing::Tracer::SpanId spanId = {};

    cb::tracing::TraceCode code;
};

#define TRACE_SCOPE(ck, code) ScopedTracer __st__##__LINE__(ck, code)

/**
 * Begin a trace. Accepts a single optional argument - the time_point to use
 * as the starting time instead of the default std::chrono::steady_clock::now().
 */
#define TRACE_BEGIN(ck, code, ...) \
    InstantTracer(ck, code, /*begin*/ true, __VA_ARGS__)

/**
 * End a trace. Accepts a single optional argument - the time_point to use
 * as the ending time instead of the default std::chrono::steady_clock::now().
 */
#define TRACE_END(ck, code, ...) \
    InstantTracer(ck, code, /*begin*/ false, __VA_ARGS__)
