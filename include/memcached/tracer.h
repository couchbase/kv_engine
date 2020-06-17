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

#include <memcached/visibility.h>
#include <chrono>
#include <string>
#include <vector>

namespace cb {
namespace tracing {

enum class Code : uint8_t {
    /// Time spent in the entire request
    Request,
    /// Time spent waiting for a background fetch operation to be scheduled.
    BackgroundWait,
    /// Time spent performing the actual background load from disk.
    BackgroundLoad,
    /// Time spent in EngineIface::get
    Get,
    /// Time spent in EngineIface::get_if
    GetIf,
    /// Time spent in EngineIface::getStats
    GetStats,
    /// Time spent in EngineIface::setWithMeta
    /// (only success.. @todo This seems weird and should be fixed)
    SetWithMeta,
    /// Time spent in EngineIface::store and EngineIface::store_if
    Store,
    /// Time spent by a SyncWrite in Prepared state before being completed.
    SyncWritePrepare,
    /// Time when a SyncWrite local ACK is received by the Active.
    SyncWriteAckLocal,
    /// Time when a SyncWrite replica ACK is received by the Active.
    SyncWriteAckRemote,
};

using SpanId = std::size_t;

class MEMCACHED_PUBLIC_CLASS Span {
public:
    /// Type used for storing durations - 32bit microsecond.
    /// gives maximum duration of 35.79minutes.
    using Duration = std::chrono::duration<int32_t, std::micro>;

    Span(Code code,
         std::chrono::steady_clock::time_point start,
         Duration duration = Duration::max())
        : start(start), duration(duration), code(code) {
    }
    std::chrono::steady_clock::time_point start;
    Duration duration;
    Code code;
};

/**
 * Tracer maintains an ordered vector of tracepoints
 * with name:time(micros)
 */
class MEMCACHED_PUBLIC_CLASS Tracer {
public:
    /// Begin a Span starting from the specified time point (defaults to now)
    SpanId begin(Code tracecode,
                 std::chrono::steady_clock::time_point startTime =
                         std::chrono::steady_clock::now());

    /// End a Span, stopping at the specified time point (defaults to now).
    bool end(SpanId spanId,
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

protected:
    std::vector<Span> vecSpans;
};

class MEMCACHED_PUBLIC_CLASS Traceable {
public:
    virtual ~Traceable() = default;
    bool isTracingEnabled() const {
        return tracingEnabled;
    }
    void setTracingEnabled(bool enable) {
        tracingEnabled = enable;
    }
    Tracer& getTracer() {
        return tracer;
    }
    const Tracer& getTracer() const {
        return tracer;
    }

protected:
    bool tracingEnabled = false;
    Tracer tracer;
};

} // namespace tracing
} // namespace cb

MEMCACHED_PUBLIC_API std::string to_string(const cb::tracing::Tracer& tracer);
MEMCACHED_PUBLIC_API std::ostream& operator<<(
        std::ostream& os, const cb::tracing::Tracer& tracer);
MEMCACHED_PUBLIC_API std::string to_string(cb::tracing::Code tracecode);
