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

#include <memcached/tracetypes.h>
#include <chrono>
#include <vector>

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
                                           bool raw = true);
MEMCACHED_PUBLIC_API std::ostream& operator<<(
        std::ostream& os, const cb::tracing::Tracer& tracer);

namespace cb {
namespace tracing {

class MEMCACHED_PUBLIC_CLASS Span {
public:
    /// Type used for storing durations - 32bit microsecond.
    /// gives maximum duration of 35.79minutes.
    using Duration = std::chrono::duration<int32_t, std::micro>;

    Span(TraceCode code,
         std::chrono::steady_clock::time_point start,
         Duration duration = Duration::max())
        : start(start), duration(duration), code(code) {
    }
    std::chrono::steady_clock::time_point start;
    Duration duration;
    TraceCode code;
};

/**
 * Tracer maintains an ordered vector of tracepoints
 * with name:time(micros)
 */
class MEMCACHED_PUBLIC_CLASS Tracer {
public:
    /// Begin a Span starting from the specified time point (defaults to now)
    SpanId begin(TraceCode tracecode,
                 std::chrono::steady_clock::time_point startTime =
                         std::chrono::steady_clock::now());

    bool end(SpanId spanId,
             std::chrono::steady_clock::time_point endTime =
                     std::chrono::steady_clock::now());

    /// End a Span, stopping at the specified time point (defaults to now).
    bool end(TraceCode tracecode,
             std::chrono::steady_clock::time_point endTime =
                     std::chrono::steady_clock::now());

    // get the tracepoints as ordered durations
    const std::vector<Span>& getDurations() const;

    Span::Duration getTotalMicros() const;

    uint16_t getEncodedMicros() const;
    /**
     * Max time period represented here is 02:00.125042
     */
    static uint16_t encodeMicros(uint64_t actual);

    static std::chrono::microseconds decodeMicros(uint16_t encoded);

    // clear the collected trace data;
    void clear();

    friend std::string(::to_string)(const cb::tracing::Tracer& tracer,
                                    bool raw);

protected:
    std::vector<Span> vecSpans;
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
