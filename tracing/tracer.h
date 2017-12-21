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
#include <cstddef>
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <memcached/visibility.h>
#include <platform/platform.h>
#include <platform/processclock.h>
#include "tracing/tracetypes.h"

namespace cb {
namespace tracing {
// fwd declaration
class MEMCACHED_PUBLIC_CLASS Tracer;
} // namespace tracing
} // namespace cb

// to have this in the global namespace
// get the tracepoints either as raw
// or formatted list of durations
MEMCACHED_PUBLIC_API std::string to_string(const cb::tracing::Tracer& tracer,
                                           bool raw = false);
MEMCACHED_PUBLIC_API std::ostream& operator<<(
        std::ostream& os, const cb::tracing::Tracer& tracer);

namespace cb {
namespace tracing {

class MEMCACHED_PUBLIC_CLASS Span {
public:
    Span(TraceCode code,
         std::chrono::microseconds start,
         std::chrono::microseconds duration = std::chrono::microseconds(0))
        : code(code), start(start), duration(duration) {
    }
    TraceCode code;
    std::chrono::microseconds start;
    std::chrono::microseconds duration;
};

/**
 * Tracer maintains an ordered vector of tracepoints
 * with name:time(micros)
 */
class MEMCACHED_PUBLIC_CLASS Tracer {
public:
    using SpanId = std::size_t;

    static SpanId invalidSpanId();

    SpanId begin(const TraceCode tracecode);
    bool end(SpanId spanId);
    bool end(const TraceCode tracecode);

    // get the tracepoints as ordered durations
    const std::vector<Span>& getDurations() const;

    std::chrono::microseconds getTotalMicros() const;

    /**
     * Max time period represented here is 02:00.125042
     */
    uint16_t getEncodedMicros(uint64_t actual = 0) const;
    std::chrono::microseconds decodeMicros(uint16_t encoded) const;

    // clear the collected trace data;
    void clear();

    friend std::string(::to_string)(const cb::tracing::Tracer& tracer,
                                    bool raw);

protected:
    std::vector<Span> vecSpans;
    std::mutex spanMutex;
};

struct MEMCACHED_PUBLIC_CLASS Traceable {
    Traceable() : enableTracing(false) {
    }

    bool isTracingEnabled() const {
        return enableTracing;
    }

    void setTracingEnabled(bool enable) {
        enableTracing = enable;
    }

    cb::tracing::Tracer& getTracer() {
        return tracer;
    }

    bool enableTracing;
    cb::tracing::Tracer tracer;
};

} // namespace tracing
} // namespace cb
