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

#include <folly/Synchronized.h>
#include <memcached/visibility.h>
#include <chrono>
#include <string>
#include <vector>

namespace cb::tracing {

enum class Code : uint8_t {
    /// Time spent in the entire request
    Request,
    /// The time spent during execution on front end thread
    Execute,
    /// The time spent associating a bucket
    AssociateBucket,
    /// The time spent disassociating a bucket
    DisassociateBucket,
    /// Time spent creating the RBAC context
    CreateRbacContext,
    /// Time spent updating privilege context when toggling buckets
    UpdatePrivilegeContext,
    /// Time spent generating audit event
    Audit,
    /// Time spent reconfiguring audit daemon
    AuditReconfigure,
    /// Time spent generating audit stats
    AuditStats,
    /// Time spent decompressing Snappy data.
    SnappyDecompress,
    /// Time spent validating if incoming value is JSON.
    JsonValidate,
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
    /// Time spent in Select Bucket
    SelectBucket,
};

using SpanId = std::size_t;
using Clock = std::chrono::steady_clock;

class MEMCACHED_PUBLIC_CLASS Span {
public:
    /// Type used for storing durations - 32bit microsecond.
    /// gives maximum duration of 35.79minutes.
    using Duration = std::chrono::duration<int32_t, std::micro>;

    Span(Code code,
         Clock::time_point start,
         Duration duration = Duration::max())
        : start(start), duration(duration), code(code) {
    }
    Clock::time_point start;
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
    SpanId begin(Code tracecode, Clock::time_point startTime = Clock::now());

    /// End a Span, stopping at the specified time point (defaults to now).
    bool end(SpanId spanId, Clock::time_point endTime = Clock::now());

    // Record a complete Span (when both start and end are already known).
    void record(Code code, Clock::time_point start, Clock::time_point end);

    // Extract the trace vector (and clears the internal trace vector)
    std::vector<Span> extractDurations();

    Span::Duration getTotalMicros() const;

    uint16_t getEncodedMicros() const;
    /**
     * Max time period represented here is 02:00.125042
     */
    static uint16_t encodeMicros(uint64_t actual);

    static std::chrono::microseconds decodeMicros(uint16_t encoded);

    // clear the collected trace data;
    void clear();

    /// Get a string representation of all of the spans
    std::string to_string() const;

protected:
    folly::Synchronized<std::vector<Span>, std::mutex> vecSpans;
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

/**
 * Helper class to assist in recording a Span into Tracer objects via
 * ScopeTimerN<> classes.
 *
 * The start and stop methods record the duration of the span and it is
 * injected into the provided traceable object as part of object destruction (as
 * long as start() was at least called).
 *
 * Normally the span is only included if the client requested trace information,
 * but it is possible to force the inclusion of the trace span.
 *
 * To allow simplification of code which might or might not be executed in
 * a client-specific context it is possible to specify nullptr as the traceable
 * and this is a "noop".
 */
class MEMCACHED_PUBLIC_CLASS SpanStopwatch {
public:
    SpanStopwatch(Traceable& traceable, Code code, bool alwaysInclude = false)
        : traceable(&traceable), code(code), alwaysInclude(alwaysInclude) {
    }
    SpanStopwatch(Traceable* traceable, Code code, bool alwaysInclude = false)
        : traceable(traceable), code(code), alwaysInclude(alwaysInclude) {
    }

    ~SpanStopwatch() {
        if (traceable && (alwaysInclude || traceable->isTracingEnabled())) {
            traceable->getTracer().record(code, startTime, stopTime);
        }
    }

    void start(Clock::time_point tp) {
        startTime = tp;
    }

    void stop(Clock::time_point tp) {
        stopTime = tp;
    }

private:
    Traceable* const traceable;
    Clock::time_point startTime;
    Clock::time_point stopTime;
    const Code code;
    const bool alwaysInclude;
};

} // namespace cb::tracing

MEMCACHED_PUBLIC_API std::ostream& operator<<(
        std::ostream& os, const cb::tracing::Tracer& tracer);
MEMCACHED_PUBLIC_API std::string to_string(cb::tracing::Code tracecode);
