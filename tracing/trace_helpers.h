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

// #define DISABLE_SESSION_TRACING 1

#ifndef DISABLE_SESSION_TRACING

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

#define TRACE_SCOPE(ck, code) ScopedTracer __st__##__LINE__(ck, code)

#else
/**
 * if DISABLE_SESSION_TRACING is set
 * unset all TRACE macros
 */
#define TRACE_SCOPE(ck, code)

#endif
